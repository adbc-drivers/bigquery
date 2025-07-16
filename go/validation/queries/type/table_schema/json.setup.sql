DROP TABLE IF EXISTS test_table_schema;

CREATE TABLE test_table_schema (
    res JSON
);

INSERT INTO test_table_schema (res)
VALUES
  (JSON '{}'),
  (JSON '{"a": 1, "b": 2}'),
  (JSON '{"a": [1, 2, 3], "b": {"c": "d"}}'),
  (JSON '{"x": null, "y": true, "z": false}'),
  (JSON '{"nested": {"array": [1, 2, 3]}}'),
  (JSON '[1, 2, {"key": "value"}]'),
  (JSON 'null'),
  (JSON 'true'),
  (JSON 'false'),
  (JSON '"string value"'),
  (JSON '12345')
;
