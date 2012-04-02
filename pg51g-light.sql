CREATE SCHEMA pg51g; 
CREATE TABLE pg51g.alternatives
(
   t_schema character varying, 
   t_table character varying, 
   t_key_sql text, 
   t_val_sql text, 
   CONSTRAINT alternatives_pkey PRIMARY KEY (t_schema, t_table)
) WITH (OIDS=FALSE)
;

CREATE OR REPLACE FUNCTION pg51g.is_md5(VARCHAR) RETURNS BOOLEAN AS 'pg51g.so', 'is_md5' LANGUAGE 'C';


CREATE OR REPLACE FUNCTION pg51g.xor_md5(VARCHAR(32),VARCHAR(32)) RETURNS VARCHAR(32) AS 'pg51g.so', 'xor_md5' LANGUAGE 'C';
CREATE AGGREGATE pg51g.xor_md5 (VARCHAR(32)) (
   SFUNC = pg51g.xor_md5,
   STYPE = VARCHAR(32),
   INITCOND = '00000000000000000000000000000000'
);

CREATE OR REPLACE FUNCTION pg51g.group_md5(VARCHAR, VARCHAR) RETURNS VARCHAR AS 'pg51g.so', 'group_md5' LANGUAGE 'C' IMMUTABLE;

CREATE OR REPLACE FUNCTION pg51g.tmp(VARCHAR, VARCHAR, VARCHAR, INT) RETURNS VARCHAR AS 'pg51g.so', 'temp_sigtbl' LANGUAGE 'C';

CREATE OR REPLACE FUNCTION pg51g.key(varchar, varchar) RETURNS VARCHAR AS 'pg51g.so', 'define_pkey' LANGUAGE 'C';

CREATE OR REPLACE FUNCTION pg51g.val(varchar, varchar) RETURNS VARCHAR AS 'pg51g.so', 'define_val' LANGUAGE 'C';

CREATE OR REPLACE FUNCTION pg51g.unkey(varchar) RETURNS VARCHAR AS 'pg51g.so', 'undef_pkey' LANGUAGE 'C';

CREATE OR REPLACE FUNCTION pg51g.unval(varchar) RETURNS VARCHAR AS 'pg51g.so', 'undef_val' LANGUAGE 'C';

-- This function is called by external clients

CREATE OR REPLACE FUNCTION pg51g.mask4level(int,int) RETURNS TEXT AS $$
   SELECT to_hex((128^($1-$2)-1)::int) AS result;
$$ LANGUAGE 'sql';


