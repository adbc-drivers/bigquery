/*
* Copyright (c) 2026 ADBC Drivers Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

#if NET8_0_OR_GREATER

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax.Grpc;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.Storage.V1;
using Xunit;

namespace AdbcDrivers.BigQuery.Tests
{
    public class ProxyManagerTests
    {
        [Fact]
        public async Task HttpClientRoutesRequestThroughProxy()
        {
            using var proxy = new CaptureProxyServer();
            using HttpClient client = ProxyManager.CreateHttpClient(new ProxyConfiguration(proxy.Address));

            using HttpResponseMessage response = await client.GetAsync("http://example.invalid/query");

            Assert.Equal(HttpStatusCode.BadGateway, response.StatusCode);
            Assert.Equal("GET http://example.invalid/query HTTP/1.1", await proxy.RequestLine);
        }

        [Fact]
        public async Task HttpClientAppliesProxyCredentials()
        {
            using var proxy = new CaptureProxyServer(requireProxyAuth: true);
            var configuration = new ProxyConfiguration(proxy.Address, "proxy-user", "proxy-pass");
            using HttpClient client = ProxyManager.CreateHttpClient(configuration);

            using HttpResponseMessage response = await client.GetAsync("http://example.invalid/query");

            string expected = "Basic " + Convert.ToBase64String(Encoding.ASCII.GetBytes("proxy-user:proxy-pass"));
            Assert.Equal(expected, await proxy.ProxyAuthorization);
        }

        [Fact]
        public async Task GrpcAdapterCreatesConnectTunnelThroughProxy()
        {
            using var proxy = new CaptureProxyServer();
            var builder = new BigQueryReadClientBuilder
            {
                Credential = GoogleCredential.FromAccessToken("test-token"),
                Endpoint = "example.invalid:443",
                GrpcAdapter = ProxyManager.CreateGrpcAdapter(new ProxyConfiguration(proxy.Address))
            };
            BigQueryReadClient client = builder.Build();
            using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));

            Task<ReadSession> call = client.CreateReadSessionAsync(
                new CreateReadSessionRequest
                {
                    Parent = "projects/test-project",
                    ReadSession = new ReadSession { Table = "projects/test-project/datasets/test/tables/test" }
                },
                CallSettings.FromCancellationToken(cancellation.Token));

            Assert.Equal("CONNECT example.invalid:443 HTTP/1.1", await proxy.RequestLine.WaitAsync(cancellation.Token));
            await Assert.ThrowsAnyAsync<Exception>(async () => await call);
        }

        [Fact]
        public void CreateProxyConfigurationBuildsAddressFromHostAndPort()
        {
            var properties = new Dictionary<string, string>
            {
                [BigQueryParameters.ProxyHost] = "proxy.internal",
                [BigQueryParameters.ProxyPort] = "8080",
                [BigQueryParameters.ProxyUsername] = "proxy-user",
                [BigQueryParameters.ProxyPassword] = "proxy-pass"
            };

            ProxyConfiguration? configuration = ProxyManager.CreateProxyConfiguration(properties);

            Assert.NotNull(configuration);
            Assert.Equal("proxy.internal:8080", configuration!.Address);
            Assert.Equal("proxy-user", configuration.Username);
            Assert.Equal("proxy-pass", configuration.Password);
        }

        [Fact]
        public void CreateProxyConfigurationReturnsNullWhenHostMissing()
        {
            var properties = new Dictionary<string, string>
            {
                [BigQueryParameters.ProxyPort] = "8080"
            };

            Assert.Null(ProxyManager.CreateProxyConfiguration(properties));
        }

        [Theory]
        [InlineData("0")]
        [InlineData("70000")]
        [InlineData("not-a-number")]
        public void CreateProxyConfigurationRejectsInvalidPort(string port)
        {
            var properties = new Dictionary<string, string>
            {
                [BigQueryParameters.ProxyHost] = "proxy.internal",
                [BigQueryParameters.ProxyPort] = port
            };

            Assert.Throws<ArgumentException>(() => ProxyManager.CreateProxyConfiguration(properties));
        }

        [Theory]
        [InlineData("ftp://localhost:21")]
        [InlineData("://invalid")]
        public void InvalidProxyAddressIsRejected(string proxyAddress)
        {
            Assert.Throws<ArgumentException>(() => ProxyManager.CreateHttpClient(new ProxyConfiguration(proxyAddress)));
        }

        private sealed class CaptureProxyServer : IDisposable
        {
            private readonly TcpListener _listener = new TcpListener(IPAddress.Loopback, 0);
            private readonly CancellationTokenSource _cancellation = new CancellationTokenSource();
            private readonly TaskCompletionSource<string> _requestLine = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly TaskCompletionSource<string?> _proxyAuthorization = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly bool _requireProxyAuth;

            internal CaptureProxyServer(bool requireProxyAuth = false)
            {
                _requireProxyAuth = requireProxyAuth;
                _listener.Start();
                int port = ((IPEndPoint)_listener.LocalEndpoint).Port;
                Address = $"127.0.0.1:{port}";
                _ = AcceptLoopAsync();
            }

            internal string Address { get; }

            internal Task<string> RequestLine => _requestLine.Task;

            internal Task<string?> ProxyAuthorization => _proxyAuthorization.Task;

            private async Task AcceptLoopAsync()
            {
                try
                {
                    while (!_cancellation.IsCancellationRequested)
                    {
                        using TcpClient client = await _listener.AcceptTcpClientAsync(_cancellation.Token);
                        using NetworkStream stream = client.GetStream();
                        using var reader = new StreamReader(stream, Encoding.ASCII, false, 1024, true);

                        string? requestLine = await reader.ReadLineAsync(_cancellation.Token);

                        string? proxyAuthorization = null;
                        string? line;
                        while (!string.IsNullOrEmpty(line = await reader.ReadLineAsync(_cancellation.Token)))
                        {
                            int separator = line.IndexOf(':');
                            if (separator > 0 &&
                                line.Substring(0, separator).Trim().Equals("Proxy-Authorization", StringComparison.OrdinalIgnoreCase))
                            {
                                proxyAuthorization = line.Substring(separator + 1).Trim();
                            }
                        }

                        if (_requireProxyAuth && proxyAuthorization == null)
                        {
                            byte[] challenge = Encoding.ASCII.GetBytes(
                                "HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm=\"test\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                            await stream.WriteAsync(challenge, _cancellation.Token);
                            continue;
                        }

                        _requestLine.TrySetResult(requestLine ?? string.Empty);
                        _proxyAuthorization.TrySetResult(proxyAuthorization);

                        byte[] response = Encoding.ASCII.GetBytes("HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                        await stream.WriteAsync(response, _cancellation.Token);
                    }
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception ex)
                {
                    _requestLine.TrySetException(ex);
                    _proxyAuthorization.TrySetException(ex);
                }
            }

            public void Dispose()
            {
                _cancellation.Cancel();
                _listener.Stop();
                _cancellation.Dispose();
            }
        }
    }
}

#endif
