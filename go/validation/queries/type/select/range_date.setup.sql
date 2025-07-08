DROP TABLE IF EXISTS test_range_date;

CREATE TABLE test_range_date (
    idx INTEGER,
    res RANGE<DATE>
);

INSERT INTO test_range_date (idx, res)
VALUES (1, RANGE<DATE> '[2020-01-01, 2021-01-02)');
