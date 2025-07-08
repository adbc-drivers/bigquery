DROP TABLE IF EXISTS test_json;

CREATE TABLE test_json (
    idx INTEGER,
    res JSON
);

INSERT INTO test_json (idx, res)
VALUES
  (0, JSON '{}'),
  (1, JSON '{"a": 1, "b": 2}'),
  (2, JSON '{"a": [1, 2, 3], "b": {"c": "d"}}'),
  (3, JSON '{"x": null, "y": true, "z": false}'),
  (4, JSON '{"nested": {"array": [1, 2, 3]}}'),
  (5, JSON '[1, 2, {"key": "value"}]'),
  (6, JSON 'null'),
  (7, JSON 'true'),
  (8, JSON 'false'),
  (9, JSON '"string value"'),
  (10, JSON '12345')
;
