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

using System.Net;
using System.Net.Http;
using Xunit;

namespace AdbcDrivers.BigQuery.Tests
{
    /// <summary>
    /// Framework-specific checks for the gRPC proxy handler. Runs on both net8.0 and net472.
    /// </summary>
    public class ProxyHandlerTests
    {
        [Fact]
        public void GrpcHttpHandlerIsConfiguredForCustomProxy()
        {
            HttpMessageHandler handler = ProxyManager.CreateGrpcHttpHandler(new ProxyConfiguration("127.0.0.1:8080"));

#if NET6_0_OR_GREATER
            HttpClientHandler httpClientHandler = Assert.IsType<HttpClientHandler>(handler);
            Assert.True(httpClientHandler.UseProxy);
            Assert.NotNull(httpClientHandler.Proxy);
#else
            // WinHttpHandler throws at send time unless the custom-proxy policy is set.
            WinHttpHandler winHttpHandler = Assert.IsType<WinHttpHandler>(handler);
            Assert.Equal(WindowsProxyUsePolicy.UseCustomProxy, winHttpHandler.WindowsProxyUsePolicy);
            Assert.NotNull(winHttpHandler.Proxy);
#endif
        }

        [Fact]
        public void GrpcHttpHandlerAppliesProxyCredentials()
        {
            HttpMessageHandler handler = ProxyManager.CreateGrpcHttpHandler(
                new ProxyConfiguration("127.0.0.1:8080", "proxy-user", "proxy-pass"));

#if NET6_0_OR_GREATER
            IWebProxy? proxy = Assert.IsType<HttpClientHandler>(handler).Proxy;
#else
            IWebProxy? proxy = Assert.IsType<WinHttpHandler>(handler).Proxy;
#endif
            NetworkCredential? credential = proxy?.Credentials as NetworkCredential;
            Assert.NotNull(credential);
            Assert.Equal("proxy-user", credential!.UserName);
            Assert.Equal("proxy-pass", credential.Password);
        }
    }
}
