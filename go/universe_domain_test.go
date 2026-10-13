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
	"io"
	"net/http"
	"strings"
	"testing"

	"cloud.google.com/go/auth/credentials/impersonate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingTransport struct {
	urls []string
}

func (rt *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.urls = append(rt.urls, req.URL.String())
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"accessToken":"impersonated","expireTime":"2099-01-01T00:00:00Z"}`)),
		Request:    req,
	}, nil
}

// IAM Credentials is not universe-specific: S3NS still serves it from
// iamcredentials.googleapis.com, while the credentials must belong to S3NS.
func TestImpersonationStaysOnGoogleAPIsInOtherUniverses(t *testing.T) {
	ctx := context.Background()
	transport := &recordingTransport{}

	creds, err := newImpersonatedCredentials(&impersonate.CredentialsOptions{
		TargetPrincipal: "sa@my-project.iam.gserviceaccount.com",
		Scopes:          []string{iamCredentialsScope},
	}, "s3nsapis.fr", transport)
	require.NoError(t, err)

	tok, err := creds.Token(ctx)
	require.NoError(t, err)
	assert.Equal(t, "impersonated", tok.Value)
	require.Len(t, transport.urls, 1)
	assert.Equal(t, "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/sa@my-project.iam.gserviceaccount.com:generateAccessToken", transport.urls[0])

	universe, err := creds.UniverseDomain(ctx)
	require.NoError(t, err)
	assert.Equal(t, "s3nsapis.fr", universe)
}
