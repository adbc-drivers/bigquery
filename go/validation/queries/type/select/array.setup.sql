DROP TABLE IF EXISTS test_array;

CREATE TABLE test_array (
    idx INTEGER,
    res_bool ARRAY<BOOL>,
    res_bool_null ARRAY<BOOL>,
    res_int64 ARRAY<INT64>,
    res_int64_null ARRAY<INT64>,
    res_string ARRAY<STRING>,
    res_string_null ARRAY<STRING>
);

INSERT INTO test_array (idx, res_bool, res_bool_null, res_int64, res_int64_null, res_string, res_string_null)
VALUES (1, [true, false], CAST(NULL AS ARRAY<BOOL>), [1, 2, 3], CAST(NULL AS ARRAY<INT64>), ['', 'foobar'], CAST(NULL AS ARRAY<STRING>));
