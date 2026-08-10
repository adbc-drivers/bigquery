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

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using Google.Api.Gax.Grpc;
using Google.Apis.Http;

namespace AdbcDrivers.BigQuery
{
    internal static class ProxyManager
    {
        internal static ProxyConfiguration? CreateProxyConfiguration(IReadOnlyDictionary<string, string> properties)
        {
            if (!properties.TryGetValue(BigQueryParameters.ProxyHost, out string? proxyHost) ||
                string.IsNullOrWhiteSpace(proxyHost))
            {
                return null;
            }

            properties.TryGetValue(BigQueryParameters.ProxyUser, out string? proxyUser);
            properties.TryGetValue(BigQueryParameters.ProxyPassword, out string? proxyPassword);

            string scheme = ResolveProxyScheme(properties);
            string host = StripScheme(proxyHost.Trim());
            string address = $"{scheme}://{host}";

            if (properties.TryGetValue(BigQueryParameters.ProxyPort, out string? sProxyPort) &&
                !string.IsNullOrWhiteSpace(sProxyPort))
            {
                if (!int.TryParse(sProxyPort, out int port) || port < 1 || port > 65535)
                {
                    throw new ArgumentException($"The value '{sProxyPort}' for parameter '{BigQueryParameters.ProxyPort}' is not a valid port number.");
                }

                address = $"{scheme}://{host}:{port}";
            }

            return new ProxyConfiguration(address, proxyUser, proxyPassword);
        }

        private static string ResolveProxyScheme(IReadOnlyDictionary<string, string> properties)
        {
            // Defaults to http: a forward proxy typically listens on plain HTTP and tunnels HTTPS via CONNECT.
            if (!properties.TryGetValue(BigQueryParameters.ProxyProtocol, out string? protocol) ||
                string.IsNullOrWhiteSpace(protocol))
            {
                return "http";
            }

            if (protocol.Equals("http", StringComparison.OrdinalIgnoreCase) ||
                protocol.Equals("https", StringComparison.OrdinalIgnoreCase))
            {
                return protocol.ToLowerInvariant();
            }

            throw new ArgumentException($"The value '{protocol}' for parameter '{BigQueryParameters.ProxyProtocol}' must be 'http' or 'https'.");
        }

        private static string StripScheme(string host)
        {
            int schemeIndex = host.IndexOf("://", StringComparison.Ordinal);
            return schemeIndex >= 0 ? host.Substring(schemeIndex + 3) : host;
        }

        internal static HttpClient CreateHttpClient(ProxyConfiguration? proxy)
        {
            return proxy == null
                ? new HttpClient()
                : new HttpClient(CreateHttpHandler(proxy));
        }

        internal static HttpClientFactory? CreateHttpClientFactory(ProxyConfiguration? proxy)
        {
            return proxy == null
                ? null
                : HttpClientFactory.ForProxy(CreateWebProxy(proxy));
        }

        internal static GrpcAdapter? CreateGrpcAdapter(ProxyConfiguration? proxy)
        {
            if (proxy == null)
            {
                return null;
            }

            return GrpcNetClientAdapter.Default.WithAdditionalOptions(options =>
            {
                options.HttpHandler = CreateGrpcHttpHandler(proxy);
                options.DisposeHttpClient = true;
            });
        }

        private static HttpMessageHandler CreateHttpHandler(ProxyConfiguration proxy)
        {
            return new HttpClientHandler
            {
                Proxy = CreateWebProxy(proxy),
                UseProxy = true
            };
        }

        internal static HttpMessageHandler CreateGrpcHttpHandler(ProxyConfiguration proxy)
        {
#if NET6_0_OR_GREATER
            return new HttpClientHandler
            {
                Proxy = CreateWebProxy(proxy),
                UseProxy = true
            };
#else
            // WinHttpHandler throws at send time if a custom Proxy is set without this policy.
            return new WinHttpHandler
            {
                EnableMultipleHttp2Connections = true,
                WindowsProxyUsePolicy = WindowsProxyUsePolicy.UseCustomProxy,
                Proxy = CreateWebProxy(proxy)
            };
#endif
        }

        private static WebProxy CreateWebProxy(ProxyConfiguration proxy)
        {
            string proxyAddress = proxy.Address;
            bool hasHttpScheme = proxyAddress.StartsWith("http://", StringComparison.OrdinalIgnoreCase) ||
                proxyAddress.StartsWith("https://", StringComparison.OrdinalIgnoreCase);
            if (!hasHttpScheme && proxyAddress.Contains("://"))
            {
                throw new ArgumentException($"The proxy address '{proxyAddress}' must use HTTP or HTTPS.", nameof(proxy));
            }

            string normalizedAddress = hasHttpScheme ? proxyAddress : $"http://{proxyAddress}";
            if (!Uri.TryCreate(normalizedAddress, UriKind.Absolute, out Uri? proxyUri) ||
                (proxyUri.Scheme != Uri.UriSchemeHttp && proxyUri.Scheme != Uri.UriSchemeHttps))
            {
                throw new ArgumentException($"The proxy address '{proxyAddress}' must be an HTTP or HTTPS URI or a host and port.", nameof(proxy));
            }

            WebProxy webProxy = new WebProxy(proxyUri);
            if (proxy.HasCredentials)
            {
                webProxy.Credentials = new NetworkCredential(proxy.Username, proxy.Password);
            }

            return webProxy;
        }
    }
}
