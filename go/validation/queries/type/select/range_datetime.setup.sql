DROP TABLE IF EXISTS test_range_datetime;

CREATE TABLE test_range_datetime (
    idx INTEGER,
    res RANGE<DATETIME>
);

INSERT INTO test_range_datetime (idx, res)
VALUES (1, RANGE<DATETIME> '[2020-01-01 00:01:02.345678, 2020-12-31 09:01:23.456789)');
