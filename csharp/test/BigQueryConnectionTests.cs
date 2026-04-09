/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.Reflection;
using Xunit;

namespace AdbcDrivers.BigQuery.Tests
{
    /// <summary>
    /// Unit tests for BigQueryConnection configuration parsing.
    /// Note: Tests that call Open() require valid Google Cloud credentials and are located in integration tests.
    /// These tests focus on configuration parsing in the constructor.
    /// </summary>
    public class BigQueryConnectionTests
    {
        #region Retry Configuration Tests

        [Fact]
        public void Constructor_SetsMaxRetryAttempts_WhenValidValueProvided()
        {
            // Arrange
            var properties = CreateBaseProperties();
            properties["adbc.bigquery.maximum_retries"] = "10";

            // Act
            var connection = new BigQueryConnection(properties);

            // Assert
            var maxRetryAttempts = GetPrivateProperty<int>(connection, "MaxRetryAttempts");
            Assert.Equal(10, maxRetryAttempts);
        }

        [Fact]
        public void Constructor_UsesDefaultMaxRetryAttempts_WhenNotProvided()
        {
            // Arrange
            var properties = CreateBaseProperties();
            // Note: MaximumRetryAttempts is intentionally not set

            // Act
            var connection = new BigQueryConnection(properties);

            // Assert
            var maxRetryAttempts = GetPrivateProperty<int>(connection, "MaxRetryAttempts");
            Assert.Equal(5, maxRetryAttempts); // Default is 5
        }

        [Fact]
        public void Constructor_SetsRetryDelayMs_WhenValidValueProvided()
        {
            // Arrange
            var properties = CreateBaseProperties();
            properties["adbc.bigquery.retry_delay_ms"] = "500";

            // Act
            var connection = new BigQueryConnection(properties);

            // Assert
            var retryDelayMs = GetPrivateProperty<int>(connection, "RetryDelayMs");
            Assert.Equal(500, retryDelayMs);
        }

        [Fact]
        public void Constructor_UsesDefaultRetryDelayMs_WhenNotProvided()
        {
            // Arrange
            var properties = CreateBaseProperties();
            // Note: RetryDelayMs is intentionally not set

            // Act
            var connection = new BigQueryConnection(properties);

            // Assert
            var retryDelayMs = GetPrivateProperty<int>(connection, "RetryDelayMs");
            Assert.Equal(200, retryDelayMs); // Default is 200
        }

        [Theory]
        [InlineData("-1")]  // Negative value
        [InlineData("abc")] // Invalid string
        public void Constructor_UsesDefaultMaxRetryAttempts_WhenInvalidValueProvided(string invalidValue)
        {
            // Arrange
            var properties = CreateBaseProperties();
            properties["adbc.bigquery.maximum_retries"] = invalidValue;

            // Act
            var connection = new BigQueryConnection(properties);

            // Assert
            var maxRetryAttempts = GetPrivateProperty<int>(connection, "MaxRetryAttempts");
            Assert.Equal(5, maxRetryAttempts); // Default is 5
        }

        [Fact]
        public void Constructor_SetsZeroMaxRetryAttempts_WhenZeroProvided()
        {
            // Arrange
            // Zero is a valid value (no retries)
            var properties = CreateBaseProperties();
            properties["adbc.bigquery.maximum_retries"] = "0";

            // Act
            var connection = new BigQueryConnection(properties);

            // Assert
            var maxRetryAttempts = GetPrivateProperty<int>(connection, "MaxRetryAttempts");
            Assert.Equal(0, maxRetryAttempts);
        }

        [Theory]
        [InlineData("-1")]  // Negative value
        [InlineData("abc")] // Invalid string
        public void Constructor_UsesDefaultRetryDelayMs_WhenInvalidValueProvided(string invalidValue)
        {
            // Arrange
            var properties = CreateBaseProperties();
            properties["adbc.bigquery.retry_delay_ms"] = invalidValue;

            // Act
            var connection = new BigQueryConnection(properties);

            // Assert
            var retryDelayMs = GetPrivateProperty<int>(connection, "RetryDelayMs");
            Assert.Equal(200, retryDelayMs); // Default is 200
        }

        [Fact]
        public void Constructor_SetsZeroRetryDelayMs_WhenZeroProvided()
        {
            // Arrange
            // Zero is a valid value (no delay between retries)
            var properties = CreateBaseProperties();
            properties["adbc.bigquery.retry_delay_ms"] = "0";

            // Act
            var connection = new BigQueryConnection(properties);

            // Assert
            var retryDelayMs = GetPrivateProperty<int>(connection, "RetryDelayMs");
            Assert.Equal(0, retryDelayMs);
        }

        #endregion

        #region Timeout Configuration Tests

        [Fact]
        public void Constructor_StoresGetQueryResultsOptionsTimeout_WhenProvided()
        {
            // Arrange
            var properties = CreateBaseProperties();
            properties["adbc.bigquery.get_query_results_options.timeout"] = "60";

            // Act
            var connection = new BigQueryConnection(properties);

            // Assert - verify the property is stored in the properties dictionary
            var propsValue = GetPropertiesDictionary(connection);
            Assert.True(propsValue.TryGetValue("adbc.bigquery.get_query_results_options.timeout", out string? value));
            Assert.Equal("60", value);
        }

        [Fact]
        public void Constructor_StoresClientTimeout_WhenProvided()
        {
            // Arrange
            var properties = CreateBaseProperties();
            properties["adbc.bigquery.client.timeout"] = "120";

            // Act
            var connection = new BigQueryConnection(properties);

            // Assert - verify the property is stored in the properties dictionary
            var propsValue = GetPropertiesDictionary(connection);
            Assert.True(propsValue.TryGetValue("adbc.bigquery.client.timeout", out string? value));
            Assert.Equal("120", value);
        }

        [Fact]
        public void Constructor_StoresBothTimeoutProperties_WhenProvided()
        {
            // Arrange
            var properties = CreateBaseProperties();
            properties["adbc.bigquery.get_query_results_options.timeout"] = "60";
            properties["adbc.bigquery.client.timeout"] = "120";

            // Act
            var connection = new BigQueryConnection(properties);

            // Assert - verify both properties are stored
            var propsValue = GetPropertiesDictionary(connection);

            Assert.True(propsValue.TryGetValue("adbc.bigquery.get_query_results_options.timeout", out string? queryTimeout));
            Assert.Equal("60", queryTimeout);

            Assert.True(propsValue.TryGetValue("adbc.bigquery.client.timeout", out string? clientTimeout));
            Assert.Equal("120", clientTimeout);
        }

        [Theory]
        [InlineData("0")]
        [InlineData("-1")]
        [InlineData("abc")]
        [InlineData("")]
        public void Constructor_StoresInvalidGetQueryResultsOptionsTimeout_AsIs(string invalidValue)
        {
            // Arrange
            // Invalid values are stored as-is; validation happens in CalculateClientTimeout()
            var properties = CreateBaseProperties();
            properties["adbc.bigquery.get_query_results_options.timeout"] = invalidValue;

            // Act
            var connection = new BigQueryConnection(properties);

            // Assert - property is stored even if invalid
            var propsValue = GetPropertiesDictionary(connection);
            Assert.True(propsValue.TryGetValue("adbc.bigquery.get_query_results_options.timeout", out string? value));
            Assert.Equal(invalidValue, value);
        }

        [Theory]
        [InlineData("0")]
        [InlineData("-1")]
        [InlineData("abc")]
        [InlineData("")]
        public void Constructor_StoresInvalidClientTimeout_AsIs(string invalidValue)
        {
            // Arrange
            // Invalid values are stored as-is; validation happens in CalculateClientTimeout()
            var properties = CreateBaseProperties();
            properties["adbc.bigquery.client.timeout"] = invalidValue;

            // Act
            var connection = new BigQueryConnection(properties);

            // Assert - property is stored even if invalid
            var propsValue = GetPropertiesDictionary(connection);
            Assert.True(propsValue.TryGetValue("adbc.bigquery.client.timeout", out string? value));
            Assert.Equal(invalidValue, value);
        }

        #endregion

        #region CalculateClientTimeout Tests
        // These tests verify the timeout adjustment logic using the internal CalculateClientTimeout() method

        [Fact]
        public void CalculateClientTimeout_AdjustsClientTimeout_WhenLessThanQueryResultsTimeoutPlus30()
        {
            // Arrange
            // GetQueryResultsOptionsTimeout = 60 seconds
            // ClientTimeout = 70 seconds (less than 60 + 30 = 90)
            // Expected: ClientTimeout should be adjusted to 90 seconds
            var properties = CreateBaseProperties();
            properties["adbc.bigquery.get_query_results_options.timeout"] = "60";
            properties["adbc.bigquery.client.timeout"] = "70";

            var connection = new BigQueryConnection(properties);

            // Act
            var (effectiveTimeout, wasAdjusted, wasSetFromQueryResults) = connection.CalculateClientTimeout();

            // Assert
            Assert.NotNull(effectiveTimeout);
            Assert.Equal(TimeSpan.FromSeconds(90), effectiveTimeout.Value);
            Assert.True(wasAdjusted);
            Assert.False(wasSetFromQueryResults);
        }

        [Fact]
        public void CalculateClientTimeout_DoesNotAdjust_WhenClientTimeoutGreaterThanQueryResultsTimeoutPlus30()
        {
            // Arrange
            // GetQueryResultsOptionsTimeout = 60 seconds
            // ClientTimeout = 120 seconds (greater than 60 + 30 = 90)
            // Expected: ClientTimeout should remain 120 seconds
            var properties = CreateBaseProperties();
            properties["adbc.bigquery.get_query_results_options.timeout"] = "60";
            properties["adbc.bigquery.client.timeout"] = "120";

            var connection = new BigQueryConnection(properties);

            // Act
            var (effectiveTimeout, wasAdjusted, wasSetFromQueryResults) = connection.CalculateClientTimeout();

            // Assert
            Assert.NotNull(effectiveTimeout);
            Assert.Equal(TimeSpan.FromSeconds(120), effectiveTimeout.Value);
            Assert.False(wasAdjusted);
            Assert.False(wasSetFromQueryResults);
        }

        [Fact]
        public void CalculateClientTimeout_SetsAutomatically_WhenOnlyQueryResultsTimeoutProvided()
        {
            // Arrange
            // GetQueryResultsOptionsTimeout = 60 seconds
            // ClientTimeout = not set
            // Expected: ClientTimeout should be automatically set to 60 + 30 = 90 seconds
            var properties = CreateBaseProperties();
            properties["adbc.bigquery.get_query_results_options.timeout"] = "60";
            // Note: ClientTimeout is intentionally not set

            var connection = new BigQueryConnection(properties);

            // Act
            var (effectiveTimeout, wasAdjusted, wasSetFromQueryResults) = connection.CalculateClientTimeout();

            // Assert
            Assert.NotNull(effectiveTimeout);
            Assert.Equal(TimeSpan.FromSeconds(90), effectiveTimeout.Value);
            Assert.False(wasAdjusted);
            Assert.True(wasSetFromQueryResults);
        }

        [Fact]
        public void CalculateClientTimeout_UsesClientTimeoutAsIs_WhenQueryResultsTimeoutNotSet()
        {
            // Arrange
            // GetQueryResultsOptionsTimeout = not set
            // ClientTimeout = 45 seconds
            // Expected: ClientTimeout should be 45 seconds (no adjustment needed)
            var properties = CreateBaseProperties();
            properties["adbc.bigquery.client.timeout"] = "45";
            // Note: GetQueryResultsOptionsTimeout is intentionally not set

            var connection = new BigQueryConnection(properties);

            // Act
            var (effectiveTimeout, wasAdjusted, wasSetFromQueryResults) = connection.CalculateClientTimeout();

            // Assert
            Assert.NotNull(effectiveTimeout);
            Assert.Equal(TimeSpan.FromSeconds(45), effectiveTimeout.Value);
            Assert.False(wasAdjusted);
            Assert.False(wasSetFromQueryResults);
        }

        [Fact]
        public void CalculateClientTimeout_ReturnsNull_WhenNeitherTimeoutProvided()
        {
            // Arrange
            // Neither GetQueryResultsOptionsTimeout nor ClientTimeout is set
            // Expected: null timeout (uses default HttpClient behavior)
            var properties = CreateBaseProperties();
            // Note: Neither timeout is set

            var connection = new BigQueryConnection(properties);

            // Act
            var (effectiveTimeout, wasAdjusted, wasSetFromQueryResults) = connection.CalculateClientTimeout();

            // Assert
            Assert.Null(effectiveTimeout);
            Assert.False(wasAdjusted);
            Assert.False(wasSetFromQueryResults);
        }

        [Theory]
        [InlineData("0", "50")]   // QueryResultsTimeout = 0 (invalid), ClientTimeout = 50
        [InlineData("-1", "50")]  // QueryResultsTimeout = -1 (invalid), ClientTimeout = 50
        [InlineData("abc", "50")] // QueryResultsTimeout = invalid string, ClientTimeout = 50
        public void CalculateClientTimeout_UsesClientTimeoutAsIs_WhenQueryResultsTimeoutInvalid(string queryResultsTimeout, string clientTimeout)
        {
            // Arrange
            var properties = CreateBaseProperties();
            properties["adbc.bigquery.get_query_results_options.timeout"] = queryResultsTimeout;
            properties["adbc.bigquery.client.timeout"] = clientTimeout;

            var connection = new BigQueryConnection(properties);

            // Act
            var (effectiveTimeout, wasAdjusted, wasSetFromQueryResults) = connection.CalculateClientTimeout();

            // Assert
            Assert.NotNull(effectiveTimeout);
            Assert.Equal(TimeSpan.FromSeconds(50), effectiveTimeout.Value);
            Assert.False(wasAdjusted);
            Assert.False(wasSetFromQueryResults);
        }

        [Theory]
        [InlineData("60", "0")]    // ClientTimeout = 0 (invalid)
        [InlineData("60", "-1")]   // ClientTimeout = -1 (invalid)
        [InlineData("60", "abc")]  // ClientTimeout = invalid string
        public void CalculateClientTimeout_SetsFromQueryResults_WhenClientTimeoutInvalid(string queryResultsTimeout, string clientTimeout)
        {
            // Arrange
            // When ClientTimeout is invalid but QueryResultsTimeout is valid,
            // ClientTimeout should be set to QueryResultsTimeout + 30
            var properties = CreateBaseProperties();
            properties["adbc.bigquery.get_query_results_options.timeout"] = queryResultsTimeout;
            properties["adbc.bigquery.client.timeout"] = clientTimeout;

            var connection = new BigQueryConnection(properties);

            // Act
            var (effectiveTimeout, wasAdjusted, wasSetFromQueryResults) = connection.CalculateClientTimeout();

            // Assert
            Assert.NotNull(effectiveTimeout);
            // QueryResultsTimeout = 60, so ClientTimeout should be 60 + 30 = 90
            Assert.Equal(TimeSpan.FromSeconds(90), effectiveTimeout.Value);
            Assert.False(wasAdjusted);
            Assert.True(wasSetFromQueryResults);
        }

        [Fact]
        public void CalculateClientTimeout_DoesNotAdjust_WhenClientTimeoutExactlyAtMinimum()
        {
            // Arrange
            // GetQueryResultsOptionsTimeout = 60 seconds
            // ClientTimeout = 90 seconds (exactly 60 + 30)
            // Expected: ClientTimeout should remain 90 seconds (no adjustment needed)
            var properties = CreateBaseProperties();
            properties["adbc.bigquery.get_query_results_options.timeout"] = "60";
            properties["adbc.bigquery.client.timeout"] = "90";

            var connection = new BigQueryConnection(properties);

            // Act
            var (effectiveTimeout, wasAdjusted, wasSetFromQueryResults) = connection.CalculateClientTimeout();

            // Assert
            Assert.NotNull(effectiveTimeout);
            Assert.Equal(TimeSpan.FromSeconds(90), effectiveTimeout.Value);
            Assert.False(wasAdjusted);
            Assert.False(wasSetFromQueryResults);
        }

        [Fact]
        public void CalculateClientTimeout_AdjustsCorrectly_WithLargeQueryResultsTimeout()
        {
            // Arrange
            // GetQueryResultsOptionsTimeout = 300 seconds (5 minutes)
            // ClientTimeout = 100 seconds (less than 300 + 30 = 330)
            // Expected: ClientTimeout should be adjusted to 330 seconds
            var properties = CreateBaseProperties();
            properties["adbc.bigquery.get_query_results_options.timeout"] = "300";
            properties["adbc.bigquery.client.timeout"] = "100";

            var connection = new BigQueryConnection(properties);

            // Act
            var (effectiveTimeout, wasAdjusted, wasSetFromQueryResults) = connection.CalculateClientTimeout();

            // Assert
            Assert.NotNull(effectiveTimeout);
            Assert.Equal(TimeSpan.FromSeconds(330), effectiveTimeout.Value);
            Assert.True(wasAdjusted);
            Assert.False(wasSetFromQueryResults);
        }

        #endregion

        #region LargeDecimalsAsString Configuration Tests

        [Fact]
        public void Constructor_SetsLargeDecimalsAsString_WhenTrueProvided()
        {
            // Arrange
            var properties = CreateBaseProperties();
            properties["adbc.bigquery.large_decimals_as_string"] = "true";

            // Act
            var connection = new BigQueryConnection(properties);

            // Assert
            var propsValue = GetPropertiesDictionary(connection);
            Assert.True(propsValue.TryGetValue("adbc.bigquery.large_decimals_as_string", out string? value));
            Assert.Equal("True", value);
        }

        [Fact]
        public void Constructor_SetsLargeDecimalsAsString_WhenFalseProvided()
        {
            // Arrange
            var properties = CreateBaseProperties();
            properties["adbc.bigquery.large_decimals_as_string"] = "false";

            // Act
            var connection = new BigQueryConnection(properties);

            // Assert
            var propsValue = GetPropertiesDictionary(connection);
            Assert.True(propsValue.TryGetValue("adbc.bigquery.large_decimals_as_string", out string? value));
            Assert.Equal("False", value);
        }

        [Fact]
        public void Constructor_UsesDefaultLargeDecimalsAsString_WhenNotProvided()
        {
            // Arrange
            var properties = CreateBaseProperties();
            // Note: LargeDecimalsAsString is intentionally not set

            // Act
            var connection = new BigQueryConnection(properties);

            // Assert - should default to "true" (BigQueryConstants.TreatLargeDecimalAsString)
            var propsValue = GetPropertiesDictionary(connection);
            Assert.True(propsValue.TryGetValue("adbc.bigquery.large_decimals_as_string", out string? value));
            // Default is "true" based on BigQueryConstants.TreatLargeDecimalAsString
            Assert.Equal("true", value);
        }

        #endregion

        #region Helper Methods

        private Dictionary<string, string> CreateBaseProperties()
        {
            return new Dictionary<string, string>
            {
                ["adbc.bigquery.auth_type"] = "service",
                ["adbc.bigquery.project_id"] = "test-project",
            };
        }

        private T GetPrivateProperty<T>(object obj, string propertyName)
        {
            var property = obj.GetType().GetProperty(propertyName, BindingFlags.NonPublic | BindingFlags.Instance);
            if (property == null)
            {
                throw new ArgumentException($"Property '{propertyName}' not found on type '{obj.GetType().Name}'");
            }
            return (T)property.GetValue(obj)!;
        }

        private Dictionary<string, string> GetPropertiesDictionary(BigQueryConnection connection)
        {
            var propsField = typeof(BigQueryConnection)
                .GetField("properties", BindingFlags.NonPublic | BindingFlags.Instance);
            var propsValue = propsField?.GetValue(connection) as Dictionary<string, string>;
            return propsValue ?? throw new InvalidOperationException("Could not get properties dictionary");
        }

        #endregion
    }
}
