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
	"sync"
	"testing"
)

func TestCancellableContextIsConcurrentSafe(t *testing.T) {
	var contexts cancellableContext
	var group sync.WaitGroup
	for range 100 {
		group.Add(2)
		go func() {
			defer group.Done()
			ctx := contexts.newContext()
			contexts.finishContext(ctx)
		}()
		go func() {
			defer group.Done()
			contexts.cancelContext()
		}()
	}
	group.Wait()
	contexts.cancelContext()
}

func TestCancellableContextFinishDoesNotClearNewContext(t *testing.T) {
	var contexts cancellableContext
	first := contexts.newContext()
	second := contexts.newContext()
	contexts.finishContext(first)

	select {
	case <-second.Done():
		t.Fatal("finishing an old operation cancelled the current one")
	default:
	}

	if !contexts.cancelContext() {
		t.Fatal("current operation was not active")
	}
	if second.Err() != context.Canceled {
		t.Fatalf("current operation context error = %v, want context.Canceled", second.Err())
	}
}

func TestStatementExecutionContextIsSeparate(t *testing.T) {
	var statement cStmt
	statement.newContext()
	defer statement.cancelContext()
	if statement.executionContext.cancelContext() {
		t.Fatal("a non-execution operation was treated as an active execution")
	}

	ctx := statement.executionContext.newContext()
	if !statement.executionContext.cancelContext() {
		t.Fatal("current execution was not active")
	}
	if ctx.Err() != context.Canceled {
		t.Fatalf("execution context error = %v, want context.Canceled", ctx.Err())
	}
}
