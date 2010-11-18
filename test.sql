USE testDatabase;
IF EXISTS(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'test') DROP TABLE test;
CREATE TABLE test (
       binary binary(256),
       varbinary varbinary(256),
       -- boolean boolean,
       bit bit,
       char char,
       varchar varchar(256),
       datetime datetime,
       smalldatetime smalldatetime,
       tinyint tinyint,
       smallint smallint,
       int int,
       bigint bigint,
       decimal decimal,
       numeric numeric,
       float float,
       real real,
       money money,
       smallmoney smallmoney,
       -- sensitivity sensitivity,
       text text,
       image image,
       nchar nchar,
       nvarchar nvarchar(256),
       ntext ntext
       );
INSERT INTO test (binary, varbinary, bit, varchar) VALUES (
       CONVERT(VARBINARY(256), 24987234987),
       24987234987,
       1,
       CONVERT(VARCHAR(256), 'harro')
       );
