DROP TABLE IF EXISTS test_table_schema;

CREATE TABLE test_table_schema (
    res BYTES
);

INSERT INTO test_table_schema (res) VALUES (b'\xe3\x81\x93\xe3\x82\x93\xe3\x81\xab\xe3\x81\xa1\xe3\x81\xaf\xe3\x80\x81\xe4\xb8\x96\xe7\x95\x8c\xef\xbc\x81');
INSERT INTO test_table_schema (res) VALUES (b'\x00');
INSERT INTO test_table_schema (res) VALUES (b'\xde\xad\xbe\xef');
INSERT INTO test_table_schema (res) VALUES (b'');
INSERT INTO test_table_schema (res) VALUES (NULL);
