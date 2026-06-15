DROP TABLE IF EXISTS test_range_date;

CREATE TABLE test_range_date (
    idx INTEGER,
    res RANGE<DATE>
);

INSERT INTO test_range_date (idx, res)
VALUES
  (0, NULL),
  (1, RANGE<DATE> '[UNBOUNDED, UNBOUNDED)'),
  (2, RANGE<DATE> '[UNBOUNDED, 2021-01-02)'),
  (3, RANGE<DATE> '[2020-01-01, UNBOUNDED)'),
  (4, RANGE<DATE> '[2020-01-01, 2021-01-02)'),
  (5, RANGE<DATE> '[0001-01-01, 9999-12-31)')
;
