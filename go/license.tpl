Columnar Technologies License
Copyright (c) 2025 Columnar Technologies, Inc.  All rights reserved.
{{/*
  License template for binary artifacts.

  To use:

  go install github.com/google/go-licenses/v2@latest
  go-licenses report ./... --template ./license.tpl > ./LICENSE.txt
*/}}
--------------------------------------------------------------------------------

This project includes code from Apache Arrow ADBC.

Copyright: 2022 The Apache Software Foundation.
Home page: https://arrow.apache.org/
License: http://www.apache.org/licenses/LICENSE-2.0
{{ range .}}
--------------------------------------------------------------------------------

3rdparty dependency {{ .Name }}
is statically linked in binary distributions.
{{ .Name }} is under the {{ .LicenseName }} license.
{{ if ne .LicenseName "Apache-2.0" -}}
{{.LicenseText}}
{{ else }}
The Apache 2.0 license can be found at:
http://www.apache.org/licenses/LICENSE-2.0
{{- end }}
{{- end }}
