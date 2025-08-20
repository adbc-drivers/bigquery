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

package bigquery

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/suite"
)

func TestSchema(t *testing.T) {
	suite.Run(t, &SchemaSuite{})
}

type SchemaSuite struct {
	suite.Suite
}

func (s *SchemaSuite) TestInt64() {
	field, err := buildField(&bigquery.FieldSchema{
		Name: "ints",
		Type: bigquery.IntegerFieldType,
	}, 0)
	s.NoError(err)
	s.Equal("ints", field.Name)
	s.Equal(arrow.INT64, field.Type.ID())
	s.True(field.Nullable)

	field, err = buildField(&bigquery.FieldSchema{
		Name:     "ints",
		Type:     bigquery.IntegerFieldType,
		Required: true,
	}, 0)
	s.NoError(err)
	s.Equal("ints", field.Name)
	s.Equal(arrow.INT64, field.Type.ID())
	s.False(field.Nullable)
}

func (s *SchemaSuite) TestListInt64() {
	field, err := buildField(&bigquery.FieldSchema{
		Name:     "ints",
		Type:     bigquery.IntegerFieldType,
		Repeated: true,
	}, 0)
	expectedType := arrow.ListOf(arrow.PrimitiveTypes.Int64)
	s.NoError(err)
	s.Equal("ints", field.Name)
	s.Truef(arrow.TypeEqual(expectedType, field.Type), field.Type.String())
	s.True(field.Nullable)
}

func (s *SchemaSuite) TestListGeography() {
	field, err := buildField(&bigquery.FieldSchema{
		Name:     "geos",
		Type:     bigquery.GeographyFieldType,
		Repeated: true,
	}, 0)
	expectedType := arrow.ListOfField(arrow.Field{
		Name:     "item",
		Type:     arrow.BinaryTypes.String,
		Nullable: true,
		Metadata: arrow.MetadataFrom(map[string]string{"ARROW:extension:name": "geoarrow.wkt"}),
	})
	s.NoError(err)
	s.Equal("geos", field.Name)
	s.Truef(arrow.TypeEqual(expectedType, field.Type), field.Type.String())
	ext, ok := field.Type.(*arrow.ListType).ElemField().Metadata.GetValue("ARROW:extension:name")
	s.True(ok)
	s.Equal("geoarrow.wkt", ext)
	s.True(field.Nullable)
}

func (s *SchemaSuite) TestStruct() {
	field, err := buildField(&bigquery.FieldSchema{
		Name: "record",
		Type: bigquery.RecordFieldType,
		Schema: []*bigquery.FieldSchema{
			{
				Name:     "geo",
				Type:     bigquery.GeographyFieldType,
				Required: true,
			},
			{
				Name:     "json",
				Type:     bigquery.JSONFieldType,
				Repeated: true,
			},
		},
	}, 0)
	expectedType := arrow.StructOf(
		arrow.Field{
			Name:     "geo",
			Type:     arrow.BinaryTypes.String,
			Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{"ARROW:extension:name": "geoarrow.wkt"}),
		},
		arrow.Field{
			Name: "json",
			Type: arrow.ListOfField(arrow.Field{
				Name:     "item",
				Type:     arrow.BinaryTypes.String,
				Nullable: true,
				Metadata: arrow.MetadataFrom(map[string]string{"ARROW:extension:name": "arrow.json"}),
			}),
			Nullable: true,
		},
	)
	s.NoError(err)
	s.Equal("record", field.Name)
	s.Truef(arrow.TypeEqual(expectedType, field.Type), field.Type.String())
	s.True(field.Nullable)

	record := field.Type.(*arrow.StructType)
	ext, _ := record.Field(0).Metadata.GetValue("ARROW:extension:name")
	s.Equal("geoarrow.wkt", ext)

	ext, _ = record.Field(1).Type.(*arrow.ListType).ElemField().Metadata.GetValue("ARROW:extension:name")
	s.Equal("arrow.json", ext)
}

func (s *SchemaSuite) TestStructList() {
	field, err := buildField(&bigquery.FieldSchema{
		Name: "record",
		Type: bigquery.RecordFieldType,
		Schema: []*bigquery.FieldSchema{
			{
				Name:     "ints",
				Type:     bigquery.IntegerFieldType,
				Required: true,
			},
		},
		Repeated: true,
	}, 0)
	expectedType := arrow.ListOf(arrow.StructOf(
		arrow.Field{
			Name:     "ints",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
		},
	))
	s.NoError(err)
	s.Equal("record", field.Name)
	s.Truef(arrow.TypeEqual(expectedType, field.Type), field.Type.String())
	s.True(field.Nullable)
}

func (s *SchemaSuite) TestNumeric() {
	var expectedType arrow.DataType

	field, err := buildField(&bigquery.FieldSchema{
		Name:     "decimals",
		Type:     bigquery.NumericFieldType,
		Repeated: true,
	}, 0)
	expectedType = arrow.ListOf(&arrow.Decimal128Type{Precision: 38, Scale: 9})
	s.NoError(err)
	s.Equal("decimals", field.Name)
	s.Truef(arrow.TypeEqual(expectedType, field.Type), field.Type.String())
	s.True(field.Nullable)

	// Ignore precision/scale because in terms of Arrow, we only ever get
	// back data as decimal128(38, 9).  BigQuery's precision/scale is only
	// used to validate incoming data.
	field, err = buildField(&bigquery.FieldSchema{
		Name:      "decimals",
		Type:      bigquery.NumericFieldType,
		Precision: 10,
		Scale:     2,
		Required:  true,
	}, 0)
	expectedType = &arrow.Decimal128Type{Precision: 38, Scale: 9}
	s.NoError(err)
	s.Equal("decimals", field.Name)
	s.Truef(arrow.TypeEqual(expectedType, field.Type), field.Type.String())
	s.False(field.Nullable)
}
