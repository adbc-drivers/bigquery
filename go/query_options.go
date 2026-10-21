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
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
)

// queryDefaults is copied when a connection is created. Setters replace enum
// pointers so changing defaults does not mutate existing children.
type queryDefaults struct {
	config bigquery.QueryConfig
}

func newQueryDefaults() queryDefaults {
	return queryDefaults{config: bigquery.QueryConfig{
		QueryResultsFormat: new(bigquery.QueryResultsFormatArrow),
	}}
}

func (qd *queryDefaults) getOption(key string) (bool, string, error) {
	switch key {
	case OptionQueryJobCreationMode, OptionQueryResultsFormat, OptionQueryArrowSerializationOptionsBufferCompression:
		v, err := getQueryOption(&qd.config, key)
		return true, v, err
	default:
		return false, "", nil
	}
}

func (qd *queryDefaults) setOption(key, value string) (bool, error) {
	switch key {
	case OptionQueryJobCreationMode, OptionQueryResultsFormat, OptionQueryArrowSerializationOptionsBufferCompression:
		return true, setQueryOption(&qd.config, key, value)
	default:
		return false, nil
	}
}

func getQueryOption(config *bigquery.QueryConfig, key string) (string, error) {
	switch key {
	case OptionQueryJobCreationMode:
		if config.JobCreationMode == nil {
			return JobCreationModeRequired, nil
		}
		switch *config.JobCreationMode {
		case bigquery.JobCreationModeRequired:
			return JobCreationModeRequired, nil
		case bigquery.JobCreationModeOptional:
			return JobCreationModeOptional, nil
		default:
			return "", adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  fmt.Sprintf("[bq] unknown job creation mode: %v", *config.JobCreationMode),
			}
		}
	case OptionQueryResultsFormat:
		if config.QueryResultsFormat == nil {
			return ResultsFormatStructEncoding, nil
		}
		switch *config.QueryResultsFormat {
		case bigquery.QueryResultsFormatArrow:
			return ResultsFormatArrow, nil
		case bigquery.QueryResultsFormatStructEncoding:
			return ResultsFormatStructEncoding, nil
		default:
			return "", adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  fmt.Sprintf("[bq] unknown query results format: %v", *config.QueryResultsFormat),
			}
		}
	case OptionQueryArrowSerializationOptionsBufferCompression:
		if config.QueryResultsCompressionCodec == nil {
			return "", nil
		}
		switch *config.QueryResultsCompressionCodec {
		case bigquery.QueryResultsCompressionCodecLZ4:
			return ResultsCompressionLz4, nil
		case bigquery.QueryResultsCompressionCodecZSTD:
			return ResultsCompressionZstd, nil
		default:
			return "", adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  fmt.Sprintf("[bq] unknown Arrow results compression: %v", *config.QueryResultsCompressionCodec),
			}
		}
	default:
		return "", adbc.Error{Code: adbc.StatusNotFound, Msg: fmt.Sprintf("[bq] unknown query option %q", key)}
	}
}

func setQueryOption(config *bigquery.QueryConfig, key, v string) error {
	switch key {
	case OptionQueryJobCreationMode:
		switch strings.ToUpper(v) {
		case string(bigquery.JobCreationModeUnspecified):
			config.JobCreationMode = nil
		case "REQUIRED", string(bigquery.JobCreationModeRequired):
			config.JobCreationMode = new(bigquery.JobCreationModeRequired)
		case "OPTIONAL", string(bigquery.JobCreationModeOptional):
			config.JobCreationMode = new(bigquery.JobCreationModeOptional)
		default:
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] invalid job creation mode: %s (expected required, optional, JOB_CREATION_MODE_UNSPECIFIED, JOB_CREATION_REQUIRED, or JOB_CREATION_OPTIONAL; case-insensitive)", v),
			}
		}
	case OptionQueryResultsFormat:
		switch strings.ToUpper(v) {
		case "QUERY_RESULTS_FORMAT_UNSPECIFIED":
			config.QueryResultsFormat = nil
		case string(bigquery.QueryResultsFormatArrow):
			config.QueryResultsFormat = new(bigquery.QueryResultsFormatArrow)
		case string(bigquery.QueryResultsFormatStructEncoding):
			config.QueryResultsFormat = new(bigquery.QueryResultsFormatStructEncoding)
		default:
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] invalid query results format: %s (expected ARROW, STRUCT_ENCODING, or QUERY_RESULTS_FORMAT_UNSPECIFIED; case-insensitive)", v),
			}
		}
	case OptionQueryArrowSerializationOptionsBufferCompression:
		switch strings.ToUpper(v) {
		case "COMPRESSION_UNSPECIFIED":
			config.QueryResultsCompressionCodec = nil
		case "LZ4", string(bigquery.QueryResultsCompressionCodecLZ4):
			config.QueryResultsCompressionCodec = new(bigquery.QueryResultsCompressionCodecLZ4)
		case string(bigquery.QueryResultsCompressionCodecZSTD):
			config.QueryResultsCompressionCodec = new(bigquery.QueryResultsCompressionCodecZSTD)
		default:
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("[bq] invalid Arrow results compression: %s (expected lz4, LZ4_FRAME, ZSTD, or COMPRESSION_UNSPECIFIED; case-insensitive)", v),
			}
		}
	default:
		return adbc.Error{Code: adbc.StatusNotImplemented, Msg: fmt.Sprintf("[bq] unknown query option %q", key)}
	}
	return nil
}
