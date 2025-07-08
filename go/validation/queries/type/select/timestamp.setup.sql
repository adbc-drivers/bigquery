DROP TABLE IF EXISTS test_timestamp;

CREATE TABLE test_timestamp (
    idx INTEGER,
    res DATETIME
);

INSERT INTO test_timestamp (idx, res) VALUES (1, DATETIME '2023-05-15 13:45:30');
INSERT INTO test_timestamp (idx, res) VALUES (2, DATETIME '2000-01-01 00:00:00');
INSERT INTO test_timestamp (idx, res) VALUES (3, DATETIME '1969-07-20 20:17:40');
INSERT INTO test_timestamp (idx, res) VALUES (4, DATETIME '9999-12-31 23:59:59');
INSERT INTO test_timestamp (idx, res) VALUES (5, NULL);
