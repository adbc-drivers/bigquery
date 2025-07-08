// Copyright (c) 2025 Columnar Technologies, Inc.  All rights reserved.

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
