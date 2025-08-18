// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bigquery_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/stretchr/testify/suite"
)

func TestErrorMapping(t *testing.T) {
	suite.Run(t, &ErrorTestSuite{})
}

type ErrorTestSuite struct {
	BigQueryTestSuite
}

func (s *ErrorTestSuite) TestBadQuery() {
	ctx := context.Background()

	s.NoError(s.stmt.SetSqlQuery("this syntax ain't right"))
	_, err := s.stmt.ExecuteUpdate(ctx)
	var adbcError adbc.Error
	s.ErrorAs(err, &adbcError)

	s.Equal(adbc.StatusInvalidArgument, adbcError.Code)
}

func (s *ErrorTestSuite) TestNonexistentTable() {
	ctx := context.Background()

	s.NoError(s.stmt.SetSqlQuery("SELECT * FROM thistabledoesnotexist"))
	_, err := s.stmt.ExecuteUpdate(ctx)
	var adbcError adbc.Error
	s.ErrorAs(err, &adbcError)

	s.Equal(adbc.StatusNotFound, adbcError.Code)
}
