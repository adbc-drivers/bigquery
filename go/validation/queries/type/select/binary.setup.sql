DROP TABLE IF EXISTS test_binary;

CREATE TABLE test_binary (
    idx INTEGER,
    res BYTES
);

INSERT INTO test_binary (idx, res) VALUES (1, b'\xe3\x81\x93\xe3\x82\x93\xe3\x81\xab\xe3\x81\xa1\xe3\x81\xaf\xe3\x80\x81\xe4\xb8\x96\xe7\x95\x8c\xef\xbc\x81');  -- 'こんにちは、世界！' in UTF-8
INSERT INTO test_binary (idx, res) VALUES (2, b'\x00');  -- Single zero byte
INSERT INTO test_binary (idx, res) VALUES (3, b'\xde\xad\xbe\xef');  -- Common pattern in debug
INSERT INTO test_binary (idx, res) VALUES (4, b'');  -- Empty binary
INSERT INTO test_binary (idx, res) VALUES (5, NULL);
