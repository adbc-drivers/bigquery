CREATE TABLE test_bigdecimal (
    idx INTEGER,
    res BIGNUMERIC(60,30)
);

INSERT INTO test_bigdecimal (idx, res) VALUES (1, 123.45);
INSERT INTO test_bigdecimal (idx, res) VALUES (2, 0.00);
INSERT INTO test_bigdecimal (idx, res) VALUES (3, -999.99);
INSERT INTO test_bigdecimal (idx, res) VALUES (4, BIGNUMERIC '999999999999999999999999999999.999999999999999999999999999999');
INSERT INTO test_bigdecimal (idx, res) VALUES (5, NULL);
