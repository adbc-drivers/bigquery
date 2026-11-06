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

package bigquery

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"google.golang.org/api/impersonate"
	"google.golang.org/api/option"
)

const testTargetPrincipal = "delegating@test-project.iam.gserviceaccount.com"

// openWith opens a connection on a database that has the given options and a project.
func openWith(t *testing.T, opts map[string]string) (adbc.ConnectionWithContext, error) {
	t.Helper()
	ctx := context.Background()
	opts[OptionProjectID] = "test-project"
	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(ctx, opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close(ctx)) })
	return db.Open(ctx)
}

func requireInvalidArgument(t *testing.T, err error, mentions string) {
	t.Helper()
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	require.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code, adbcErr.Msg)
	require.Contains(t, adbcErr.Msg, mentions)
}

// observeImpersonation replaces the impersonated token source with one that records its
// configuration and fails, so that no test reaches Google.
func observeImpersonation(t *testing.T) (*impersonate.CredentialsConfig, error) {
	t.Helper()
	var got impersonate.CredentialsConfig
	stop := errors.New("impersonation config observed")
	orig := newImpersonatedTokenSource
	t.Cleanup(func() { newImpersonatedTokenSource = orig })
	newImpersonatedTokenSource = func(_ context.Context, cfg impersonate.CredentialsConfig, _ ...option.ClientOption) (oauth2.TokenSource, error) {
		got = cfg
		return nil, stop
	}
	return &got, stop
}

func TestAccessTokenIsSentAsBearerToken(t *testing.T) {
	ctx := context.Background()
	seen := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The Storage Read client also connects here, but only the REST API has this prefix.
		if strings.HasPrefix(r.URL.Path, "/bigquery/v2/") {
			select {
			case seen <- r.Header.Get("Authorization"):
			default:
			}
		}
		http.Error(w, `{"error":{"code":404,"message":"not found"}}`, http.StatusNotFound)
	}))
	defer srv.Close()

	cnxn, err := openWith(t, map[string]string{
		OptionEndpoint:        srv.URL + "/bigquery/v2/",
		OptionStorageEndpoint: srv.Listener.Addr().String(),
		OptionAuthType:        OptionValueAuthTypeAccessToken,
		OptionAuthAccessToken: "caller-token",
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, cnxn.Close(ctx)) }()

	stmt, err := cnxn.NewStatement(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, stmt.Close(ctx)) }()
	require.NoError(t, stmt.SetSqlQuery(ctx, "SELECT 1"))
	_, _, err = stmt.ExecuteQuery(ctx)
	require.Error(t, err) // the fake endpoint has no job

	select {
	case got := <-seen:
		require.Equal(t, "Bearer caller-token", got)
	default:
		t.Fatal("no request reached the endpoint")
	}
}

func TestAccessTokenEmptyIsRefused(t *testing.T) {
	_, err := openWith(t, map[string]string{OptionAuthType: OptionValueAuthTypeAccessToken})
	requireInvalidArgument(t, err, OptionAuthAccessToken)
}

// Impersonation replaces the base credential, which would silently drop the token.
func TestAccessTokenWithImpersonationIsRefused(t *testing.T) {
	_, err := openWith(t, map[string]string{
		OptionAuthType:                   OptionValueAuthTypeAccessToken,
		OptionAuthAccessToken:            "caller-token",
		OptionImpersonateTargetPrincipal: testTargetPrincipal,
		OptionImpersonateScopes:          "https://www.googleapis.com/auth/bigquery",
	})
	requireInvalidArgument(t, err, "cannot be combined with impersonation")
}

// Every other auth type ignores the token, so each one must refuse it. Each case is
// otherwise a valid configuration, so only the refusal can stop it.
func TestAccessTokenWithAnotherAuthTypeIsRefused(t *testing.T) {
	credentials := `{"type":"authorized_user","client_id":"id","client_secret":"secret","refresh_token":"refresh"}`
	file := filepath.Join(t.TempDir(), "credentials.json")
	require.NoError(t, os.WriteFile(file, []byte(credentials), 0o600))

	for name, opts := range map[string]map[string]string{
		"unset":                   {},
		"auth_bigquery":           {OptionAuthType: OptionValueAuthTypeDefault},
		"app_default_credentials": {OptionAuthType: OptionValueAuthTypeAppDefaultCredentials},
		"json_credential_file":    {OptionAuthType: OptionValueAuthTypeJSONCredentialFile, OptionAuthCredentials: file},
		"json_credential_string":  {OptionAuthType: OptionValueAuthTypeJSONCredentialString, OptionAuthCredentials: credentials},
		"user_authentication": {
			OptionAuthType:         OptionValueAuthTypeUserAuthentication,
			OptionAuthClientID:     "id",
			OptionAuthClientSecret: "secret",
			OptionAuthRefreshToken: "refresh",
		},
		"anonymous": {OptionAuthType: OptionValueAuthTypeAnonymous},
	} {
		t.Run(name, func(t *testing.T) {
			opts[OptionAuthAccessToken] = "caller-token"
			_, err := openWith(t, opts)
			requireInvalidArgument(t, err, "requires `bigquery.auth_type=access_token`")
		})
	}
}

func TestImpersonateSubjectReachesTheImpersonationConfig(t *testing.T) {
	got, stop := observeImpersonation(t)
	_, err := openWith(t, map[string]string{
		OptionImpersonateTargetPrincipal: testTargetPrincipal,
		OptionImpersonateScopes:          "https://www.googleapis.com/auth/bigquery",
		OptionImpersonateSubject:         "user@example.com",
	})
	require.ErrorContains(t, err, stop.Error())
	require.Equal(t, "user@example.com", got.Subject)
	require.Equal(t, testTargetPrincipal, got.TargetPrincipal)
}

// Without a subject, the config keeps the target principal's own identity.
func TestImpersonateWithoutSubjectLeavesItEmpty(t *testing.T) {
	got, stop := observeImpersonation(t)
	_, err := openWith(t, map[string]string{
		OptionImpersonateTargetPrincipal: testTargetPrincipal,
		OptionImpersonateScopes:          "https://www.googleapis.com/auth/bigquery",
	})
	require.ErrorContains(t, err, stop.Error())
	require.Empty(t, got.Subject)
	require.Equal(t, testTargetPrincipal, got.TargetPrincipal)
}

// A subject alone never falls through to the deployment's own credential.
func TestImpersonateSubjectWithoutTargetPrincipalIsRefused(t *testing.T) {
	_, err := openWith(t, map[string]string{OptionImpersonateSubject: "user@example.com"})
	requireInvalidArgument(t, err, OptionImpersonateTargetPrincipal)
}

func TestCallerCredentialOptionsRoundTrip(t *testing.T) {
	ctx := context.Background()
	opts := map[string]string{
		OptionAuthType:           OptionValueAuthTypeAccessToken,
		OptionAuthAccessToken:    "caller-token",
		OptionImpersonateSubject: "user@example.com",
	}

	db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(ctx, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close(ctx)) }()
	require.NoError(t, db.SetOptions(ctx, opts))

	cnxn := &connectionImpl{}
	for key, want := range opts {
		got, err := db.(adbc.GetSetOptionsWithContext).GetOption(ctx, key)
		require.NoError(t, err)
		require.Equal(t, want, got, "database %s", key)

		require.NoError(t, cnxn.SetOption(ctx, key, want))
		got, err = cnxn.GetOption(ctx, key)
		require.NoError(t, err)
		require.Equal(t, want, got, "connection %s", key)
	}
}
