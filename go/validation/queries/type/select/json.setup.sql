DROP TABLE IF EXISTS test_json;

CREATE TABLE test_json (
    idx INTEGER,
    res JSON,
    res2 ARRAY<JSON>,
    res3 STRUCT<a JSON, b ARRAY<JSON>>
);

INSERT INTO test_json (idx, res, res2, res3)
VALUES
  (0, JSON '{}', [JSON '{}'], (JSON '{}', [JSON '{}'])),
  (1, JSON '{"a": 1, "b": 2}', [JSON '{"a": 1, "b": 2}'], (JSON '{"a": 1, "b": 2}', [JSON '{"a": 1, "b": 2}'])),
  (2, JSON '{"a": [1, 2, 3], "b": {"c": "d"}}', [JSON '{"a": [1, 2, 3], "b": {"c": "d"}}'], (JSON '{"a": [1, 2, 3], "b": {"c": "d"}}', [JSON '{"a": [1, 2, 3], "b": {"c": "d"}}'])),
  (3, JSON '{"x": null, "y": true, "z": false}', [JSON '{"x": null, "y": true, "z": false}'], (JSON '{"x": null, "y": true, "z": false}', [JSON '{"x": null, "y": true, "z": false}'])),
  (4, JSON '{"nested": {"array": [1, 2, 3]}}', [JSON '{"nested": {"array": [1, 2, 3]}}'], (JSON '{"nested": {"array": [1, 2, 3]}}', [JSON '{"nested": {"array": [1, 2, 3]}}'])),
  (5, JSON '[1, 2, {"key": "value"}]', [JSON '[1, 2, {"key": "value"}]'], (JSON '[1, 2, {"key": "value"}]', [JSON '[1, 2, {"key": "value"}]'])),
  (6, JSON 'null', [JSON 'null'], (JSON 'null', [JSON 'null'])),
  (7, JSON 'true', [JSON 'true'], (JSON 'true', [JSON 'true'])),
  (8, JSON 'false', [JSON 'false'], (JSON 'false', [JSON 'false'])),
  (9, JSON '"string value"', [JSON '"string value"'], (JSON '"string value"', [JSON '"string value"'])),
  (10, JSON '12345', [JSON '12345'], (JSON '12345', [JSON '12345']))
;
