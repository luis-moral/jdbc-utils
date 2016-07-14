/* Table Definitions */
CREATE TABLE IF NOT EXISTS test_table1
(
	id BIGINT(19) AUTO_INCREMENT NOT NULL,
	varchar_field VARCHAR(80) NOT NULL,
	clob_field CLOB NOT NULL,
	
	CONSTRAINT test_table1_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS test_table2
(
	id BIGINT(19) AUTO_INCREMENT NOT NULL,
	varchar_field VARCHAR(80) NOT NULL,
	clob_field CLOB NOT NULL,
	
	CONSTRAINT test_table2_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS test_table3
(
	id BIGINT(19) AUTO_INCREMENT NOT NULL,
	varchar_field VARCHAR(80) NOT NULL,
	clob_field CLOB NOT NULL,
	
	CONSTRAINT test_table3_pk PRIMARY KEY (id)
);

/* Data Definitions */
INSERT INTO test_table1 (varchar_field, clob_field) VALUES ('varchar_value1', 'clob_value1');
INSERT INTO test_table1 (varchar_field, clob_field) VALUES ('varchar_value2', 'clob_value2');
INSERT INTO test_table1 (varchar_field, clob_field) VALUES ('varchar_value3', 'clob_value3');
INSERT INTO test_table1 (varchar_field, clob_field) VALUES ('varchar_value4', 'clob_value4');
INSERT INTO test_table1 (varchar_field, clob_field) VALUES ('varchar_value5', 'clob_value5');