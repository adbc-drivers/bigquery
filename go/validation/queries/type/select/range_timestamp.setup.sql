DROP TABLE IF EXISTS test_range_timestamp;

CREATE TABLE test_range_timestamp (
    idx INTEGER,
    res RANGE<TIMESTAMP>
);

INSERT INTO test_range_timestamp (idx, res)
VALUES (1, RANGE<TIMESTAMP> '[2020-01-01 00:01:02.345678, 2020-12-31 09:01:23.456789)');
