DROP TABLE IF EXISTS test_struct;

CREATE TABLE test_struct (
    idx INTEGER,
    res STRUCT<a BOOL, b INT64, c STRING, d ARRAY<INT64>>
);

INSERT INTO test_struct (idx, res)
VALUES (1, (false, CAST(NULL AS INT64), 'foobar', [1, 2]));
