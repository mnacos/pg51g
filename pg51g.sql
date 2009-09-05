
-- This is all the SQL you need for pg51g

CREATE SCHEMA pg51g; 
CREATE OR REPLACE FUNCTION pg51g.is_md5(VARCHAR) RETURNS BOOLEAN AS 'pg51g.so', 'is_md5' LANGUAGE 'C';


CREATE OR REPLACE FUNCTION pg51g.xor_md5(VARCHAR(32),VARCHAR(32)) RETURNS VARCHAR(32) AS 'pg51g.so', 'xor_md5' LANGUAGE 'C';
CREATE AGGREGATE pg51g.xor_md5 (VARCHAR(32)) (
   SFUNC = pg51g.xor_md5,
   STYPE = VARCHAR(32),
   INITCOND = '00000000000000000000000000000000'
);

CREATE OR REPLACE FUNCTION pg51g.group_md5(VARCHAR, VARCHAR) RETURNS VARCHAR AS 'pg51g.so', 'group_md5' LANGUAGE 'C' IMMUTABLE;

CREATE OR REPLACE FUNCTION pg51g.on_change() RETURNS trigger AS 'pg51g.so', 'on_change' LANGUAGE 'C';

CREATE OR REPLACE FUNCTION pg51g.dummy() RETURNS trigger AS 'pg51g.so', 'dummy_trigger' LANGUAGE 'C';

CREATE OR REPLACE FUNCTION pg51g.freeze() RETURNS trigger AS 'pg51g.so', 'freeze_trigger' LANGUAGE 'C';

CREATE OR REPLACE FUNCTION pg51g.add(varchar) RETURNS VARCHAR AS 'pg51g.so', 'add_table' LANGUAGE 'C';

CREATE OR REPLACE FUNCTION pg51g.del(varchar) RETURNS VARCHAR AS 'pg51g.so', 'rm_table' LANGUAGE 'C';

CREATE OR REPLACE FUNCTION pg51g.diff(IN VARCHAR, OUT key VARCHAR, OUT op VARCHAR) RETURNS SETOF RECORD AS 'pg51g.so', 'diff_table' LANGUAGE 'C';

CREATE OR REPLACE FUNCTION pg51g.push(varchar) RETURNS VARCHAR AS 'pg51g.so', 'diff_push' LANGUAGE 'C';

CREATE OR REPLACE FUNCTION pg51g.do(varchar) RETURNS VARCHAR AS 'pg51g.so', 'do_table' LANGUAGE 'C';

CREATE OR REPLACE FUNCTION pg51g.snap(varchar) RETURNS VARCHAR AS 'pg51g.so', 'snap_table' LANGUAGE 'C';

CREATE OR REPLACE FUNCTION pg51g.monitor_table(varchar) RETURNS VARCHAR AS 'pg51g.so', 'monitor_table' LANGUAGE 'C';

CREATE OR REPLACE FUNCTION pg51g.unmonitor_table(varchar) RETURNS VARCHAR AS 'pg51g.so', 'unmonitor_table' LANGUAGE 'C';

CREATE OR REPLACE FUNCTION pg51g.freeze_table(varchar) RETURNS VARCHAR AS 'pg51g.so', 'freeze_table' LANGUAGE 'C';

CREATE OR REPLACE FUNCTION pg51g.unfreeze_table(varchar) RETURNS VARCHAR AS 'pg51g.so', 'unfreeze_table' LANGUAGE 'C';

CREATE OR REPLACE FUNCTION pg51g.key(varchar, varchar) RETURNS VARCHAR AS 'pg51g.so', 'define_pkey' LANGUAGE 'C';

CREATE OR REPLACE FUNCTION pg51g.val(varchar, varchar) RETURNS VARCHAR AS 'pg51g.so', 'define_val' LANGUAGE 'C';

CREATE OR REPLACE FUNCTION pg51g.unkey(varchar) RETURNS VARCHAR AS 'pg51g.so', 'undef_pkey' LANGUAGE 'C';

CREATE OR REPLACE FUNCTION pg51g.unval(varchar) RETURNS VARCHAR AS 'pg51g.so', 'undef_val' LANGUAGE 'C';

CREATE TABLE pg51g.metadata (
   id SERIAL NOT NULL, 
   t_schema character varying NOT NULL, 
   t_table character varying NOT NULL, 
   t_count bigint NOT NULL, 
   t_persists boolean NOT NULL, 
   t_view boolean NOT NULL, 
   t_definition text, 
   t_key_sql text, 
   t_val_sql text, 
   s_schema character varying NOT NULL, 
   s_table character varying NOT NULL, 
   s_depth int NOT NULL, 
   created timestamp without time zone DEFAULT now() NOT NULL, 
   updated timestamp without time zone, 
   CONSTRAINT metadata_pkey PRIMARY KEY (id)
) WITH (OIDS=FALSE)
;
CREATE INDEX pg51g_metadata_idx ON pg51g.metadata USING btree( t_schema, t_table );

CREATE TABLE pg51g.alternatives
(
   t_schema character varying, 
   t_table character varying, 
   t_key_sql text, 
   t_val_sql text, 
   CONSTRAINT alternatives_pkey PRIMARY KEY (t_schema, t_table)
) WITH (OIDS=FALSE)
;


CREATE LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION pg51g_list_tables(OUT target TEXT)
  RETURNS SETOF text AS
$$
   DECLARE
       myrec   RECORD;
       schname TEXT;
       tblname TEXT;
   BEGIN
       FOR myrec IN SELECT table_schema, table_name FROM information_schema.tables
       WHERE table_type = 'BASE TABLE' AND table_schema NOT IN
             ('pg_catalog','information_schema','pg51g')
       LOOP
          SELECT quote_ident(myrec.table_schema)||'.'||quote_ident(myrec.table_name) INTO target;
          RETURN NEXT;
       END LOOP;
       RETURN;
   END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION pg51g_list_tables_with_count(OUT target TEXT, OUT rows INTEGER)
  RETURNS SETOF record AS
$$
   DECLARE
       mysql TEXT;
   BEGIN
       FOR target IN SELECT * FROM pg51g_list_tables()
       LOOP
          SELECT 'SELECT COUNT(1) FROM '||target INTO mysql;
          EXECUTE mysql INTO STRICT rows;
          RETURN NEXT;
       END LOOP;
       RETURN;
   END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION pg51g_bulk_add(OUT target TEXT, OUT result TEXT)
  RETURNS SETOF record AS
$$
    BEGIN
         FOR target IN SELECT * FROM pg51g_list_remaining()
         LOOP
             BEGIN
                 SELECT pg51g.add(target) INTO result;
                 EXCEPTION WHEN OTHERS THEN
             END;
             RETURN NEXT;
         END LOOP;
         RETURN;
    END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION pg51g_bulk_del(OUT target TEXT, OUT result TEXT)
  RETURNS SETOF record AS
$$
    BEGIN
         FOR target IN SELECT t_schema||'.'||t_table FROM pg51g.metadata
         LOOP
             BEGIN
                 SELECT pg51g.del(target) INTO result;
                 EXCEPTION WHEN OTHERS THEN
             END;
             RETURN NEXT;
         END LOOP;
         RETURN;
    END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION pg51g_bulk_snap(OUT target TEXT, OUT result TEXT)
  RETURNS SETOF record AS
$$
    BEGIN
         FOR target IN SELECT t_schema||'.'||t_table FROM pg51g.metadata
         LOOP
             BEGIN
                 SELECT pg51g.snap(target) INTO result;
                 EXCEPTION WHEN OTHERS THEN
             END;
             RETURN NEXT;
         END LOOP;
         RETURN;
    END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION pg51g_bulk_unmonitor(OUT target TEXT, OUT result TEXT)
  RETURNS SETOF record AS
$$
    BEGIN
         FOR target IN SELECT t_schema||'.'||t_table FROM pg51g.metadata
         LOOP
             BEGIN
                 SELECT pg51g.unmonitor_table(target) INTO result;
                 EXCEPTION WHEN OTHERS THEN
             END;
             RETURN NEXT;
         END LOOP;
         RETURN;
    END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION pg51g_bulk_monitor(OUT target TEXT, OUT result TEXT)
  RETURNS SETOF record AS
$$
    BEGIN
         FOR target IN SELECT t_schema||'.'||t_table FROM pg51g.metadata
         LOOP
             BEGIN
                 SELECT pg51g.monitor_table(target) INTO result;
                 EXCEPTION WHEN OTHERS THEN
             END;
             RETURN NEXT;
         END LOOP;
         RETURN;
    END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION pg51g_bulk_freeze(OUT target TEXT, OUT result TEXT)
  RETURNS SETOF record AS
$$
    BEGIN
         FOR target IN SELECT t_schema||'.'||t_table FROM pg51g.metadata
         LOOP
             BEGIN
                 SELECT pg51g.freeze_table(target) INTO result;
                 EXCEPTION WHEN OTHERS THEN
             END;
             RETURN NEXT;
         END LOOP;
         RETURN;
    END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION pg51g_bulk_unfreeze(OUT target TEXT, OUT result TEXT)
  RETURNS SETOF record AS
$$
    BEGIN
         FOR target IN SELECT t_schema||'.'||t_table FROM pg51g.metadata
         LOOP
             BEGIN
                 SELECT pg51g.unfreeze_table(target) INTO result;
                 EXCEPTION WHEN OTHERS THEN
             END;
             RETURN NEXT;
         END LOOP;
         RETURN;
    END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION pg51g_bulk_diff(OUT target TEXT, OUT keydef TEXT, OUT key TEXT, OUT op TEXT)
  RETURNS SETOF record AS
$$
   DECLARE
       myrec   RECORD;
    BEGIN
         FOR target, keydef IN SELECT t_schema||'.'||t_table, t_key_sql FROM pg51g.metadata
         LOOP
             BEGIN
                 FOR myrec IN SELECT (pg51g.diff(target)).* ORDER BY op
                 LOOP
                     SELECT quote_literal(myrec.key), quote_literal(myrec.op) INTO key, op;
                     RETURN NEXT;
                 END LOOP;
                 EXCEPTION WHEN OTHERS THEN
             END;
         END LOOP;
         RETURN;
    END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION pg51g_list_remaining(OUT target TEXT, OUT result TEXT)
  RETURNS SETOF record AS
$$
    BEGIN
         FOR target IN SELECT mylist.* FROM (SELECT * FROM pg51g_list_tables()) AS mylist
             LEFT JOIN pg51g.metadata AS metadata
             ON mylist.target = metadata.t_schema||'.'||metadata.t_table
             WHERE metadata.t_schema IS NULL AND metadata.t_table IS NULL
         LOOP
             BEGIN
                 SELECT constraint_name FROM information_schema.table_constraints
                     WHERE constraint_type = 'PRIMARY KEY'
                     AND table_schema||'.'||table_name = lower(target) INTO result;
                 EXCEPTION WHEN OTHERS THEN
             END;
             RETURN NEXT;
         END LOOP;
         RETURN;
    END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION pg51g_auth_user(someuser TEXT)
  RETURNS BOOLEAN AS
$$
    DECLARE
         target TEXT;
    BEGIN
             EXECUTE 'GRANT USAGE ON SCHEMA pg51g TO '||quote_ident(someuser);
         EXECUTE 'GRANT SELECT, UPDATE ON pg51g.metadata TO '||quote_ident(someuser);
         FOR target IN SELECT quote_ident(s_schema)||'.'||quote_ident(s_table) FROM pg51g.metadata
         LOOP
             EXECUTE 'GRANT SELECT, UPDATE, INSERT, DELETE ON '||target||' TO '||quote_ident(someuser);
         END LOOP;
         RETURN true;
    END;
$$ LANGUAGE 'plpgsql';

-- The only reason this function is here is because it is called by our sample Java code

CREATE OR REPLACE FUNCTION pg51g.mask4level(int,int) RETURNS TEXT AS $$
   SELECT to_hex((1024^($1-$2)-1)::int) AS result;
$$ LANGUAGE 'sql';

-- Essential for defer() / sync() functionality

CREATE OR REPLACE FUNCTION pg51g.higher_levels(my_schema TEXT, my_table TEXT) RETURNS TEXT AS $$
   DECLARE
                depth INT;
                i INT;
                mask TEXT;
                sigtbl TEXT;
   BEGIN
                 -- getting rid of all higher level checksums
         EXECUTE 'DELETE FROM pg51g.'||my_schema||'_'||my_table||' WHERE level > 0';
                 -- getting the depth from the pg51g.metadata table
                 SELECT s_depth INTO depth FROM pg51g.metadata WHERE t_schema = my_schema AND t_table = my_table;
                 i := 1;
                 WHILE i <= depth
                 LOOP
                        -- foreach folding level, we need mask, sigtbl, group
                        SELECT pg51g.mask4level(depth, i) INTO mask;
                        SELECT 'pg51g.'||quote_ident(my_schema)||'_'||quote_ident(my_table) INTO sigtbl;
                        EXECUTE 'INSERT INTO '||sigtbl||' SELECT '||i||' AS level, pg51g.group_md5('||quote_literal(mask)||', key) AS mypri, pg51g.group_md5('||
                                                quote_literal(mask)||', key) AS mykey, pg51g.xor_md5(val) AS myval FROM '||sigtbl||
                                                ' WHERE level = '|| (i-1) ||' GROUP BY pg51g.group_md5('||quote_literal(mask)||',key)';
                        i := i + 1;
         END LOOP;
                 EXECUTE 'UPDATE pg51g.metadata SET updated = '||quote_literal(now())||' WHERE t_schema = '||quote_literal(my_schema)||
                        ' AND t_table = '||quote_literal(my_table);
                 RETURN i::text;
   END;
$$ LANGUAGE 'plpgsql';

-- Helpful for generating indices for components of multi-column primary keys, speeding up trigger

CREATE OR REPLACE FUNCTION pg51g_fix_tables(OUT target TEXT)
  RETURNS SETOF TEXT AS
$$
        DECLARE
                mylist text[];
                sql TEXT;
                tmp RECORD;
                idx TEXT;
                counter INT;
    BEGIN
         FOR target IN SELECT mytab FROM (SELECT COUNT(column_name) AS columns, table_schema||'.'||table_name AS mytab FROM information_schema.key_column_usage GROUP BY table_schema||'.'||table_name) AS mycount WHERE columns > 1
         LOOP
                        BEGIN
                                SELECT regexp_split_to_array(target, E'\\.') INTO mylist;
                                counter := 1;
                                SELECT 'SELECT column_name, table_schema, table_name FROM information_schema.key_column_usage WHERE table_schema = '
                                                        ||quote_literal(mylist[array_lower(mylist,1)])|| ' AND table_name = '
                                                        ||quote_literal(mylist[array_upper(mylist,1)])||' ORDER BY ordinal_position' INTO sql;
                                FOR tmp IN EXECUTE sql
                                LOOP
                                                SELECT 'CREATE INDEX pg51g_fix_'||tmp.table_schema||'_'||tmp.table_name||'_'||counter||' ON '||quote_ident(tmp.table_schema)||'.'||
                                                        quote_ident(tmp.table_name)||' USING btree('||tmp.column_name||')' INTO idx;
                                                EXECUTE idx;
                                                counter := counter + 1;
                                END LOOP;
                                RETURN NEXT;
                EXCEPTION WHEN OTHERS THEN
                        END;
         END LOOP;
         RETURN;
    END;
$$ LANGUAGE 'plpgsql';

-- These functions provide the pg51g.defer(), pg51g.sync() interface

CREATE OR REPLACE FUNCTION pg51g_bulk_defer(OUT myschema TEXT, OUT mytable TEXT)
  RETURNS SETOF record AS
$$
    BEGIN
         FOR myschema, mytable IN SELECT t_schema, t_table FROM pg51g.metadata
         LOOP
             BEGIN
                                 EXECUTE 'UPDATE pg51g.metadata SET s_depth = -5 WHERE t_schema = '||quote_literal(myschema)||' AND t_table = '||quote_literal(mytable);
             END;
                         RETURN NEXT;
         END LOOP;
         RETURN;
    END;
$$ LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION pg51g_bulk_sync(OUT myschema TEXT, OUT mytable TEXT)
  RETURNS SETOF record AS
$$
        DECLARE
                depth INT;
    BEGIN
         FOR myschema, mytable IN SELECT t_schema, t_table FROM pg51g.metadata
         LOOP
             BEGIN
                                 EXECUTE 'SELECT max(level) FROM pg51g.'||myschema||'_'||mytable INTO depth;
                                 IF depth IS NOT NULL THEN
                                        EXECUTE 'UPDATE pg51g.metadata SET s_depth = '||depth||' WHERE t_schema = '||quote_literal(myschema)||' AND t_table = '||quote_literal(mytable);
                                 ELSE
                                        EXECUTE 'UPDATE pg51g.metadata SET s_depth = -15 WHERE t_schema = '||quote_literal(myschema)||' AND t_table = '||quote_literal(mytable);
                                 END IF;
                                 EXECUTE 'SELECT pg51g.higher_levels('||quote_literal(myschema)||', '||quote_literal(mytable)||')';
             END;
                         RETURN NEXT;
         END LOOP;
         RETURN;
    END;
$$ LANGUAGE 'plpgsql';



