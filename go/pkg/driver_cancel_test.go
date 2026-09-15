// Copyright (c) 2026 ADBC Drivers Contributors
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

//go:build driverlib

package main

import (
	"context"
	"testing"
)

func TestStatementExecutionContextIsSeparate(t *testing.T) {
	var statement cStmt
	statement.NewContext()
	defer statement.CancelContext()
	if statement.executionContext.CancelContext() {
		t.Fatal("a non-execution operation was treated as an active execution")
	}

	ctx := statement.executionContext.NewContext()
	if !statement.executionContext.CancelContext() {
		t.Fatal("current execution was not active")
	}
	if ctx.Err() != context.Canceled {
		t.Fatalf("execution context error = %v, want context.Canceled", ctx.Err())
	}
}
