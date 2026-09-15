/*
 * Copyright (c) 2026 ADBC Drivers Contributors
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

using Apache.Arrow.Adbc;
using Xunit;

namespace AdbcDrivers.BigQuery.Tests
{
    /// <summary>
    /// ADBC search patterns give special meaning to "%" and "_" and to nothing else - the
    /// specification states that escaping is not supported. The driver applies them two ways: the
    /// catalog and dataset patterns are matched client-side through
    /// <see cref="BigQueryConnection.PatternToRegEx"/>, while the table and column patterns are
    /// bound into a BigQuery LIKE. These tests pin both to the same semantics.
    /// </summary>
    public class SearchPatternTests
    {
        [Theory]
        [InlineData("my_table", "my_table", true)]
        // "_" matches exactly one character.
        [InlineData("my_table", "myXtable", true)]
        [InlineData("my_table", "mytable", false)]
        // "%" matches zero or more.
        [InlineData("my%", "my", true)]
        [InlineData("my%", "my_long_table", true)]
        [InlineData("%table", "my_table", true)]
        [InlineData("%", "anything", true)]
        // Anchored at both ends: a pattern must match the whole name.
        [InlineData("my", "my_table", false)]
        [InlineData("table", "my_table", false)]
        public void PatternMatchesTheSameWayLikeWould(string pattern, string name, bool expected)
        {
            string regex = BigQueryConnection.PatternToRegEx(pattern);

            if (expected)
            {
                Assert.Matches(regex, name);
            }
            else
            {
                Assert.DoesNotMatch(regex, name);
            }
        }

        [Theory]
        // Regex metacharacters are literal in an ADBC search pattern.
        [InlineData(".", "x")]
        [InlineData("a.c", "abc")]
        [InlineData("a+", "aaa")]
        [InlineData("a*", "aaa")]
        [InlineData("(a)", "a")]
        [InlineData("a|b", "a")]
        public void RegexMetacharactersAreLiteral(string pattern, string name)
        {
            Assert.DoesNotMatch(BigQueryConnection.PatternToRegEx(pattern), name);
        }

        [Theory]
        [InlineData(".")]
        [InlineData("a.c")]
        [InlineData("[unclosed")]
        [InlineData("(unclosed")]
        [InlineData("back\\slash")]
        public void MetacharactersMatchThemselvesAndNeverThrow(string pattern)
        {
            // An unescaped "[" or "(" used to reach the regex engine and throw out of GetObjects.
            Assert.Matches(BigQueryConnection.PatternToRegEx(pattern), pattern);
        }

        [Fact]
        public void PatternMatchingIsCaseSensitive()
        {
            // Matches the case-sensitive LIKE applied to the table and column patterns, and
            // BigQuery's own case-sensitive dataset and table names.
            Assert.DoesNotMatch(BigQueryConnection.PatternToRegEx("my_dataset"), "MY_DATASET");
            Assert.Matches(BigQueryConnection.PatternToRegEx("my_dataset"), "my_dataset");
        }

        [Fact]
        public void NullPatternMatchesEverything()
        {
            Assert.Matches(BigQueryConnection.PatternToRegEx(null), "anything at all");
        }

        [Theory]
        // BigQuery's LIKE treats "\" as an escape character; ADBC does not. Doubling it keeps the
        // backslash literal without disturbing "%" and "_".
        [InlineData("a\\b", "a\\\\b")]
        [InlineData("\\%", "\\\\%")]
        [InlineData("plain", "plain")]
        [InlineData("with%wildcard_", "with%wildcard_")]
        public void LikePatternEscapesBackslashesOnly(string input, string expected)
        {
            Assert.Equal(expected, BigQueryConnection.EscapeLikePattern(input));
        }

        [Theory]
        [InlineData("my_table")]
        [InlineData("mock-project")]
        [InlineData("A1")]
        public void ValidIdentifiersAreAccepted(string input)
        {
            Assert.Equal(input, BigQueryConnection.SanitizeIdentifier(input));
        }

        [Theory]
        // Each of these starts with an allowed character, which the unanchored allowlist accepted.
        [InlineData("a'; DROP TABLE secrets; SELECT '")]
        [InlineData("a` UNION ALL SELECT 1 --")]
        [InlineData("a.b")]
        [InlineData("a b")]
        [InlineData("a%")]
        // .NET's "$" also matches before a trailing newline, so "\\z" is used instead.
        [InlineData("a\n DROP TABLE secrets")]
        public void InvalidIdentifiersAreRejected(string input)
        {
            AdbcException exception = Assert.Throws<AdbcException>(() => BigQueryConnection.SanitizeIdentifier(input));
            Assert.Equal(AdbcStatusCode.InvalidArgument, exception.Status);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        public void EmptyIdentifierIsPassedThrough(string? input)
        {
            Assert.Equal(string.Empty, BigQueryConnection.SanitizeIdentifier(input));
        }
    }
}
