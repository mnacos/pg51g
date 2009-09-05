/*
** Filename: pg51g.c
*/
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "executor/spi.h"
#include "nodes/execnodes.h"
#include "commands/trigger.h"
#include "utils/builtins.h"
#include <ctype.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/*
  helper function which takes a SQL statement as its only argument and returns the first field of the first tuple from
  the results as a varchar (length of string should stay <= 8192) -- what is max length for a varchar in Postgresql?
*/

/* this function retrieves the first field of the first tuple returned by a supplied SQL statement */

static char * return_val(char *sql)
{
    char *buf = (char *) palloc (8192); int ret; int proc;
    /* trying to connect */
    if ((ret = SPI_connect()) < 0) { ereport(ERROR,
                                                   ( errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                                     errmsg("[pg51g] return_val: connection error"),
                                                     errhint("pg51g_return_val could not SPI connect!")  )
                                     );
    }

    /* trying to execute sql */
    ret = SPI_exec(sql,0);
    if (ret < 0) { ereport(ERROR,
                                  ( errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                    errmsg("[pg51g] return_val: sql error"),
                                    errhint("Please be careful about the SQL you throw at pg51g_return_val!")  )
                          );
    }

    proc = SPI_processed;
    if (SPI_tuptable != NULL && proc > 0)
    {
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;
        HeapTuple tuple = tuptable->vals[0]; // we only need the first field of the first tuple
        if (!(tuple == 0)) {
            char *mystring = SPI_getvalue(tuple, tupdesc, 1);
            if (!(mystring == 0)) { strcpy(buf, mystring); } else { strcpy(buf, ""); }
        }
    }
    else { strcpy(buf, ""); } // should this be something else? this way we are confusing empty strings with no rows
    SPI_finish();

    int buf_s = strlen(buf); char *res = (char *) palloc (buf_s+1);
    strncpy(res, buf, buf_s); res[buf_s] = '\0'; pfree(buf);
    return res;
}

/* this is similar to return_val(), but we don't need to read any results,
   as an error will be raised if there is anything wrong with the SQL statement;
   we are also targeting exactly one row in the sigtbl, so we can do SPI_exec(sql,1)
*/
static int on_change_exec(char *sql)
{
    int ret;
    /* trying to connect */
    if ((ret = SPI_connect()) < 0) { ereport(ERROR,
                                                   ( errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                                     errmsg("[pg51g] on_change_exec: connection error"),
                                                     errhint("pg51g_on_change_exec could not SPI connect!")  )
                                     );
    }

    /* trying to execute sql */
    ret = SPI_exec(sql,1);
    if (ret < 0) { ereport(ERROR,
                                  ( errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                    errmsg("[pg51g] on_change_exec: sql error"),
                                    errhint("Please be careful about the SQL you throw at pg51g_on_change_exec!")  )
                          );
    }
    SPI_finish();
    return ret;
}

static char * concat_fields(char *sql)
{
    char *buf = (char *) palloc (8192); int ret; int proc; int i;
    strcpy(buf, ""); char *mystring;

    /* trying to connect */
    if ((ret = SPI_connect()) < 0) { ereport(ERROR,
                                                   ( errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                                     errmsg("[pg51g] concat_fields: connection error"),
                                                     errhint("pg51g_concat_fields could not SPI connect!")  )
                                     );
    }

    /* trying to execute sql */
    ret = SPI_exec(sql,0);
    if (ret < 0) { ereport(ERROR,
                                  ( errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                    errmsg("[pg51g] concat_fields: sql error"),
                                    errhint("Please be careful about the SQL you throw at pg51g_concat_fields!")  )
                          );
    }

    proc = SPI_processed;
    if (SPI_tuptable != NULL && proc > 0)
    {
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;
        for (i=0; i<proc; i++) {
            HeapTuple tuple = tuptable->vals[i]; // we only need the first field of the first tuple
            if (!(tuple == 0)) {
                mystring = SPI_getvalue(tuple, tupdesc, 1);
                if (!(mystring == 0)) { strcat(buf, mystring); strcat(buf, "||\'-\'||"); } else { strcat(buf, ""); }
            }
        }
    }
    else { strcpy(buf, ""); } // should this be something else? this way we are confusing empty strings with no rows
    SPI_finish();

    char *res; int buf_s;
    if (strlen(buf) > 0) { buf_s = strlen(buf) - 7; } else { buf_s = strlen(buf); }
    res = palloc (buf_s+1); strncpy(res, buf, buf_s);
    res[buf_s] = '\0'; pfree(sql); pfree(buf);
    return res;
}

/* this function simply checks the existence of an entry for a { schema, table } combination in the metadata table */

static bool table_in_metadata(char * schema, char * table)
{
   int sch_s = strlen(schema); int tbl_s = strlen(table);
   int sql_s = 80 + sch_s + tbl_s; char *sql = (char *) palloc (sql_s); memset(sql, 0, sql_s);
   sprintf(sql, "SELECT COUNT(*) FROM pg51g.metadata WHERE t_schema = '%s' AND t_table = '%s';", schema, table);
   char *val = return_val(sql); int res;
   sscanf(val,"%d",&res);
   if (res > 0) { return true; } else { return false; }
}

/* this function simply checks to see if a { schema, table } correpsonds to a view */

static bool table_is_view(char * schema, char * table)
{
   int sch_s = strlen(schema); int tbl_s = strlen(table);
   int sql_s = 94 + sch_s + tbl_s; char *sql = (char *) palloc (sql_s); memset(sql, 0, sql_s);
   sprintf(sql, "SELECT COUNT(*) FROM information_schema.views WHERE table_schema = lower('%s') AND table_name = lower('%s');", schema, table);
   char *val = return_val(sql); int rows = 0; sscanf(val,"%d",&rows);
   if ( rows > 0 ) { return true; }
   else { return false; }
}

/* this function simply checks to see if the relation is a temporary one */

static bool table_is_temp(char * table)
{
   int tbl_s = strlen(table); int sql_s = 111 + tbl_s; char *sql = (char *) palloc (sql_s + 1);
   sprintf(sql, "SELECT table_type FROM information_schema.tables WHERE table_schema = 'pg_temp_1' AND table_name = lower('%s');", table);
   char *val = return_val(sql);
   if ( abs(strcmp(val,"LOCAL TEMPORARY")) ) { pfree(sql); pfree(val); return false; }
   else { pfree(sql); pfree(val); return true; }
}

/* this function simply checks if a table exists */

static bool table_exists(char * schema, char * table)
{
   int sch_s = strlen(schema); int tbl_s = strlen(table);
   int sql_s = 114 + sch_s + tbl_s; char *sql = (char *) palloc (sql_s);
   sprintf(sql, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = lower('%s') AND table_name = lower('%s');", schema, table);
   char *val = return_val(sql); int res; sscanf(val,"%d",&res);
   if (res > 0) { return true; }
   else { return false; }
}

/* this function simply checks if a table exists and then it tries to drop it */

static bool drop_if_exists(char * schema, char * table)
{
   int sch_s = strlen(schema); int tbl_s = strlen(table);
   int sql_s = 114 + sch_s + tbl_s; char *sql = (char *) palloc (sql_s);
   sprintf(sql, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = lower('%s') AND table_name = lower('%s');", schema, table);
   char *val = return_val(sql); int res; sscanf(val,"%d",&res); char *string = "DROP %s%s.%s;"; char *type, *dsql, *result;
   if (res > 0) { 
       if( table_is_view(schema, table) ) { type = "VIEW "; } else { type = "TABLE "; }
       dsql = palloc( strlen(string) + strlen(type) + strlen(schema) + strlen(table) - 5 );
       sprintf(dsql, string, type, schema, table);
       result = return_val(dsql);
       return true;
   }
   else { return false; }
}

/* this function simply checks whether sigtbl persists from a value in pg51g.metadata */

static bool table_persists(char * schema, char * table)
{
   char *string = "SELECT t_persists FROM pg51g.metadata WHERE t_schema = '%s' AND t_table = '%s';";
   int sql_s = strlen(string) + strlen(schema) + strlen(table); char *sql = (char *) palloc (sql_s + 1);
   sprintf(sql, string, schema, table);
   char *val = return_val(sql); bool result = false; 
   if ( abs(strcmp(val,"t")) ) { result = false; } else { result = true; }
   return result;
}

/* this function simply returns the signature table as defined in pg51g.metadata */

static char * read_sig_table(char * schema, char * table)
{
   char *string = "SELECT s_table FROM pg51g.metadata WHERE t_schema = '%s' AND t_table = '%s';";
   int sql_s = strlen(string) + strlen(schema) + strlen(table); char *sql = (char *) palloc (sql_s + 1);
   sprintf(sql, string, schema, table);
   char *val = return_val(sql);
   return val;
}

/* this function simply returns the definition of a view or table from the metadata table */

static char * read_table_def(char * schema, char * table)
{
   int sch_s = strlen(schema); int tbl_s = strlen(table);
   int sql_s = 77 + sch_s + tbl_s; char *sql = (char *) palloc (sql_s + 1);
   sprintf(sql, "SELECT t_definition FROM pg51g.metadata WHERE t_schema = '%s' AND t_table = '%s';", schema, table);
   return return_val(sql);
}

/* this function attempts to properly escape backslashes in SQL statements */

static char * escape_backslashes(char * sometext)
{
    unsigned int size = strlen(sometext); int d_s = 11 * strlen(sometext);
    char *newtxt = (char *) palloc (d_s + 1); int i; strcpy(newtxt,"");
    char *tempo = (char *) palloc (2 * sizeof(char));
    for(i=0; i<size; i++) {
        if (sometext[i] == '\\') {
             strcat(newtxt, "\'||E");
             strcat(newtxt, "\'");
             strcat(newtxt, "\\\\");
             strcat(newtxt, "\'||");
             strcat(newtxt, "\'");
        }
        else {
            strncpy(tempo, &sometext[i], 1); tempo[1] = '\0';
            strcat(newtxt, tempo);
        }
    }
    pfree(tempo);
    return newtxt;
}

/* this function simply duplicates all single quotes in order to escape them for the construction of SQL statements */

static char * escape_quotes(char * sometext)
{
    unsigned int size = strlen(sometext); int d_s = 2 * strlen(sometext);
    char *newtxt = (char *) palloc (d_s + 1); int k = 0; int i;
    for(i=0; i<size; i++) {strncpy(&newtxt[k++], &sometext[i], 1); if (sometext[i] == '\'') {strncpy(&newtxt[k++], &sometext[i], 1);}}
    newtxt[k] = '\0';
    return newtxt;
}

/* this function simply extracts the name of an attribute from a COALESCE statement -- assuming it is surrounded by double quotes */

static char * extract_from_double_quotes(char * sometext)
{
    unsigned int size = strlen(sometext); int d_s = size;
    char *newtxt = (char *) palloc (d_s + 1); int k = 0; int i;
        bool active = false;
    for(i=0; i<size; i++) {
                if (!active) { if (sometext[i] == '\"') { active = true; } else { continue; } }
                else {
                        if (sometext[i] == '\"') { break; }
                        else { strncpy(&newtxt[k++], &sometext[i], 1); }
                }
        }
    newtxt[k] = '\0';
    return newtxt;
}

/* this function simply removes all single quotes from a SQL statement */

static char * strip_quotes(char * sometext)
{
    unsigned int size = strlen(sometext); int d_s = strlen(sometext);
    char *newtxt = (char *) palloc (d_s + 1); int k = 0; int i;
    for(i=0; i<size; i++) { if (sometext[i] != '\'') {strncpy(&newtxt[k++], &sometext[i], 1);}}
    newtxt[k] = '\0';
    return newtxt;
}

/* this function looks for a primary key constraint for the target table in the information_schema tables */

static char * find_pkey(char * schema, char * table)
{
   int sch_s = strlen(schema); int tbl_s = strlen(table);
   char *string1 = "SELECT constraint_name FROM information_schema.table_constraints WHERE constraint_type = 'PRIMARY KEY' AND table_schema = lower('') AND table_name = lower('');";
   int sql_s = strlen(string1) + sch_s + tbl_s; char *sql = (char *) palloc (sql_s + 1); memset(sql, 0, sql_s + 1);

   strcpy(sql, "SELECT constraint_name FROM information_schema.table_constraints WHERE constraint_type = 'PRIMARY KEY' AND table_schema = lower('");
   strcat(sql, schema); strcat(sql, "') AND table_name = lower('"); strcat(sql, table); strcat(sql, "');"); sql[sql_s] = '\0';

   char *val = return_val(sql); pfree(sql); char *orig, *string2, *col; int col_s; int val_len = strlen(val);
   if (val_len > 1) {
       string2 = "SELECT column_name FROM information_schema.key_column_usage WHERE table_schema = lower('') AND table_name = lower('') AND constraint_name = '' ORDER BY ordinal_position;";
       col_s = strlen(string2) + sch_s + tbl_s + val_len; char *col = (char *) palloc (col_s + 1); memset(col, 0, col_s + 1);
       strcpy(col, "SELECT column_name FROM information_schema.key_column_usage WHERE table_schema = lower('"); strcat(col, schema);
       strcat(col, "') AND table_name = lower('"); strcat(col, table); strcat(col, "') AND constraint_name = '"); strcat(col, val); strcat(col, "' ");
       strcat(col, "ORDER BY ordinal_position;");
       col[col_s] = '\0';
       orig = concat_fields(col); // instead of return_val, we want to support multi-attribute primary keys
       return orig;
   }
   else { 
       orig = palloc (1); strcpy(orig, ""); return orig;
   }
}

/* this function generates SQL text for the concatenation of all fields for each row of a particular table, using information_schema data */

static char * gen_val(char * schema, char * table)
{
    int sch_s = strlen(schema); int tbl_s = strlen(table);
    char *string1 = "SELECT column_name FROM information_schema.columns WHERE table_schema = lower('%s') AND table_name = lower('%s') ORDER BY ordinal_position;";
    int sql_s = strlen(string1) + sch_s + tbl_s - 4;
    char *sql = (char *) palloc (sql_s + 1); memset(sql, 0, sql_s + 1);
    sprintf(sql, string1, schema, table); sql[sql_s] = '\0';
    char *buf = (char *) palloc (8192); int ret, proc, i; strcpy(buf, "");

    /* trying to connect */
    if ((ret = SPI_connect()) < 0) { ereport(ERROR,
                                                   ( errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                                     errmsg("[pg51g] gen_val: connection error"),
                                                     errhint("pg51g_return_val could not SPI connect!")  )
                                     );
    }

    /* trying to execute sql */
    ret = SPI_exec(sql,0);
    if (ret < 0) { ereport(ERROR,
                                  ( errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                    errmsg("[pg51g] gen_val: sql error"),
                                    errhint("Please be careful about the SQL you throw at pg51g_gen_val!")  )
                          );
    }

    proc = SPI_processed;
    if (SPI_tuptable != NULL && proc > 0)
    {
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;
        for (i=0; i<proc; i++) {
            HeapTuple tuple = tuptable->vals[i];
            if (!(tuple == 0)) {
                char *mystring = SPI_getvalue(tuple, tupdesc, 1);
                if (!(mystring == 0)) { strcat(buf, "COALESCE(\""); strcat(buf, mystring); strcat(buf, "\"::TEXT,'#--NULL--#')||"); }
                else { strcat(buf, ""); }
            }
        }
    }
    else { strcat(buf, ""); }
    SPI_finish();

    char *res; int buf_s;
    if (strlen(buf) > 0) { buf_s = strlen(buf) - 2; } else { buf_s = strlen(buf); }
    res = palloc (buf_s+1); strncpy(res, buf, buf_s);
    res[buf_s] = '\0'; pfree(sql); pfree(buf);
    return res;
}

/* this function looks for the presence of a pkey definition in the pg51g.alternatives table to return
   if there is no such thing defined in the alternatives table, it calls find_pkey() and returns its result */

static char * pkey_text(char * schema, char * table)
{
   int sch_s = strlen(schema); int tbl_s = strlen(table);

   char *string1 = "SELECT t_key_sql FROM pg51g.alternatives WHERE t_schema = '' AND t_table = '';";

   int sql_s = strlen(string1) + sch_s + tbl_s; char *sql = (char *) palloc (sql_s + 1); memset(sql, 0, sql_s + 1);
   strcpy(sql, "SELECT t_key_sql FROM pg51g.alternatives WHERE t_schema = '"); strcat(sql, schema);
   strcat(sql, "' AND t_table = '"); strcat(sql, table); strcat(sql, "';"); sql[sql_s] = '\0';

   char *val = return_val(sql); pfree(sql); char *res, *orig; int res_s; int val_len = strlen(val);
   if ( val_len > 1 ) {
      res_s = strlen(val) + 6; res = palloc( res_s + 1 ); memset(res, 0, res_s + 1);
      strcpy(res, val); strcat(res, "::TEXT"); res[res_s] = '\0';
      return res;
   }
   else {
      orig = find_pkey(schema, table);
      if (strlen(orig) > 0) {
          res_s = strlen(orig) + 6; res = palloc( res_s + 1 ); memset(res, 0, res_s + 1);
          strcpy(res, orig); strcat(res, "::TEXT"); res[res_s] = '\0';
          return res;
      } else { return orig; } // if it's an empty string we don't append '::TEXT' at the end
   }
}

/* this function looks for the presence of a pkey definition in the pg51g.alternatives table to return
   if there is no such thing defined in the alternatives table, it calls find_pkey() and returns its result */

static char * pkey_pure(char * schema, char * table)
{
   int sch_s = strlen(schema); int tbl_s = strlen(table);

   char *string1 = "SELECT t_key_sql FROM pg51g.alternatives WHERE t_schema = '' AND t_table = '';";

   int sql_s = strlen(string1) + sch_s + tbl_s; char *sql = (char *) palloc (sql_s + 1); memset(sql, 0, sql_s + 1);
   strcpy(sql, "SELECT t_key_sql FROM pg51g.alternatives WHERE t_schema = '"); strcat(sql, schema);
   strcat(sql, "' AND t_table = '"); strcat(sql, table); strcat(sql, "';"); sql[sql_s] = '\0';

   char *val = return_val(sql); pfree(sql); char *res, *orig; int res_s; int val_len = strlen(val);
   if ( val_len > 1 ) { return val; }
   else { orig = find_pkey(schema, table); return orig; }
}

/* this function looks for the presence of a pkey definition in the pg51g.alternatives table to return
   if there is no such thing defined in the alternatives table, it calls find_pkey() and returns its result */

static char * val_text(char * schema, char * table)
{
   int sch_s = strlen(schema); int tbl_s = strlen(table);

   char *string1 = "SELECT t_val_sql FROM pg51g.alternatives WHERE t_schema = '' AND t_table = '';";

   int sql_s = strlen(string1) + sch_s + tbl_s; char *sql = (char *) palloc (sql_s + 1); memset(sql, 0, sql_s + 1);
   strcpy(sql, "SELECT t_val_sql FROM pg51g.alternatives WHERE t_schema = '"); strcat(sql, schema);
   strcat(sql, "' AND t_table = '"); strcat(sql, table); strcat(sql, "';"); sql[sql_s] = '\0';

   char *val = return_val(sql); pfree(sql); char *res, *orig; int res_s; int val_len = strlen(val);
   if ( val_len > 1 ) { return val; }
   else {
      orig = gen_val(schema, table);
      return orig;
   }
}

static char * strip_text(char *name)
{
    unsigned int size = strlen(name);
    int res = 0; int i, my_s; char *pkey; for(i=0; i<size; i++) { if (!res && name[i] == ':') { res = i; } } // finding the first colon
    if (res) { my_s = res; pkey = palloc(size + 1); strncpy(pkey, &name[0], my_s); pkey[my_s] = '\0'; }
    else { my_s = size; pkey = palloc(size + 1); strncpy(pkey, name, my_s); pkey[my_s] = '\0'; }
    return pkey;
}

static char * read_pkey_pure (char * schname, char * tblname)
{
    char *string = "SELECT t_key_sql FROM pg51g.metadata WHERE t_schema = '%s' AND t_table = '%s';";
    int sql_s = strlen(string) + strlen(schname) + strlen(tblname) - 4;
    char *sql = (char *) palloc (sql_s + 1);
    sprintf(sql, string, schname, tblname);
    char *res = return_val(sql);
    char *pure = strip_text(res);
    return pure;
}

static char * read_pkey_sql (char * schname, char * tblname)
{
    char *string = "SELECT t_key_sql FROM pg51g.metadata WHERE t_schema = '%s' AND t_table = '%s';";
    int sql_s = strlen(string) + strlen(schname) + strlen(tblname) - 4;
    char *sql = (char *) palloc (sql_s + 1);
    sprintf(sql, string, schname, tblname);
    char *res = return_val(sql);
    return res;
}

static char * read_val_sql (char * schname, char * tblname)
{
    char *string = "SELECT t_val_sql FROM pg51g.metadata WHERE t_schema = '%s' AND t_table = '%s';";
    int sql_s = strlen(string) + strlen(schname) + strlen(tblname) - 4;
    char *sql = (char *) palloc (sql_s + 1);
    sprintf(sql, string, schname, tblname);
    char *res = return_val(sql);
    return res;
}

/* this function attempts to produce and return a valid SQL definition for the structure of a given table */

static char * table_definition(char * schema, char * table)
{
   int sch_s = strlen(schema); int tbl_s = strlen(table);
   char *string = "SELECT column_name, data_type, is_nullable, column_default, is_updatable FROM information_schema.columns WHERE table_schema = lower('%s') AND table_name = lower('%s') ORDER BY ordinal_position;";
   int sql_s = strlen(string) + sch_s + tbl_s - 4; char *sql = (char *) palloc (sql_s + 1);
   sprintf(sql, string, schema, table);

   char *def = (char *) palloc (8192);
   bool perm = !table_is_temp(table);
   char *temp; if ( !perm ) { temp = "TEMP "; } else { temp = ""; }
   bool is_view = table_is_view( schema, table );
   char *type; if (is_view) { type = "VIEW"; } else { type = "TABLE"; }

   // postgresql does not allow schema specification for temporary relations
   if (perm) { sprintf(def, "CREATE %s%s %s.%s", temp, type, schema, table); }
   else { sprintf(def, "CREATE %s%s %s", temp, type, table); }

   if( is_view ) {
      strcat(def, " AS ");
      char *string = "SELECT view_definition FROM information_schema.views WHERE table_schema = lower('%s') AND table_name = lower('%s');";
      char *view_def = (char *) palloc( strlen(string) + sch_s + tbl_s - 3 );
      sprintf(view_def, string, schema, table);
      strcat(def, return_val(view_def));
   }
   else {
      strcat(def, " (");
      char *pkey = find_pkey(schema, table);
      int ret, proc, i;

      /* trying to connect */
      if ((ret = SPI_connect()) < 0) { ereport(ERROR,
                                                     ( errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                                       errmsg("[pg51g] table_definition: connection error"),
                                                       errhint("pg51g_return_val could not SPI connect!")  )
                                       );
      }
  
      /* trying to execute sql */
      ret = SPI_exec(sql,0);
      if (ret < 0) { ereport(ERROR,
                                    ( errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                      errmsg("[pg51g] table_definition: sql error"),
                                      errhint("Please be careful about the SQL you throw at pg51g_table_definition!")  )
                            );
      }
  
      proc = SPI_processed;
      if (SPI_tuptable != NULL && proc > 0)
      {
          TupleDesc tupdesc = SPI_tuptable->tupdesc;
          SPITupleTable *tuptable = SPI_tuptable;
          for (i=0; i<proc; i++) {
              HeapTuple tuple = tuptable->vals[i];
              if (!(tuple == 0)) {
                  char *name = SPI_getvalue(tuple, tupdesc, 1);
                  char *type = SPI_getvalue(tuple, tupdesc, 2);
                  char *nullable = SPI_getvalue(tuple, tupdesc, 3);
                  char *defaultval = SPI_getvalue(tuple, tupdesc, 4);
                  char *notnull; if ( abs(strcmp(nullable,"YES")) ) { notnull = "NOT NULL"; } else { notnull = ""; }
                  if (!(name == 0)) { strcat(def, " "); strcat(def, name); }
                  if (!(type == 0)) { strcat(def, " "); strcat(def, type); }
                  if (!( abs(strcmp(name,pkey)) )) { strcat(def, " "); strcat(def, "PRIMARY KEY"); }
                  else { if (!(notnull == 0)) { strcat(def, " "); strcat(def, notnull); } }
                  if (!(defaultval == 0)) { strcat(def, " "); strcat(def, defaultval); }
                  strcat(def, " ,");
              }
          }
          def[strlen(def)-1] = '\0'; strcat(def, ");");
      }
      else { strcat(def, ""); }
      SPI_finish();
   }
   return escape_quotes(def);
}

static char * get_table(char *name)
{
    unsigned int size = strlen(name);
    int res = 0; int i; for(i=0; i<size; i++) { if (!res && name[i] == '.') { res = i+1; } } // finding the first dot
    int my_s = size-res; char *tblname = (char *) palloc(size + 1); strncpy(tblname, &name[res], my_s); tblname[my_s] = '\0';
    return tblname;
}

static char * get_schema(char *name)
{
    unsigned int size = strlen(name);
    int res = 0; int i, my_s; char *schname; for(i=0; i<size; i++) { if (!res && name[i] == '.') { res = i; } } // finding the first dot
    if (res) { my_s = res; schname = palloc(size + 1); strncpy(schname, &name[0], my_s); schname[my_s] = '\0'; }
    else { my_s = 6; schname = palloc(size + 1); strncpy(schname, "public", my_s); schname[my_s] = '\0'; }
    if ( table_is_temp( get_table(name) ) ) { pfree(schname); schname = palloc(10); strcpy(schname, "pg_temp_1"); }
    return schname;
}

static char * get_sig_target(char *schname, char *tblname)
{
    unsigned int size = strlen(schname) + strlen(tblname) + 7;
    char *sigtbl = (char *) palloc(size + 1);
    sprintf(sigtbl, "pg51g.%s_%s", schname, tblname); sigtbl[size] = '\0';
    return sigtbl;
}

static char * get_snap_target(char *schname, char *tblname)
{
    unsigned int size = strlen(schname) + strlen(tblname) + 13;
    char *snaptbl = (char *) palloc(size + 1);
    sprintf(snaptbl, "pg51g.saved_%s_%s", schname, tblname); snaptbl[size] = '\0';
    return snaptbl;
}

static char * get_sig_table(char *schname, char *tblname)
{
    unsigned int size = strlen(schname) + strlen(tblname) + 1;
    char *sigtbl = (char *) palloc(size + 1);
    sprintf(sigtbl, "%s_%s", schname, tblname); sigtbl[size] = '\0';
    return sigtbl;
}

static char * get_sig_schema(char *schname, char *tblname)
{
    char *sigsch = "pg51g";
    return sigsch;
}

static char * get_mask(int depth, int level)
{
    if( level > depth ) { char *mask = (char *) palloc (2); sprintf(mask, "%d", 0); return mask; }
    text *myhex = DatumGetTextPCopy( DirectFunctionCall1(to_hex32, Int32GetDatum( pow(1024, depth-level)-1 )));
    unsigned int myhexlen = VARSIZE(myhex) - VARHDRSZ;
    char *mask = (char *) palloc(myhexlen+1); strncpy(mask, &VARDATA(myhex)[0], myhexlen); mask[myhexlen] = '\0';
    return mask;
}

static char * get_group(char *mask, char *value)
{

   /* storing the size of the bit mask string in variable masksize -- we need to align the two strings for bitwise AND */
   unsigned int masksize = strlen(mask); int pad = 0; char *zero = (char *) palloc((1+1)*sizeof(char)); strcpy(zero, "0");
   char *mynull = (char *) palloc((1+1)*sizeof(char)); strcpy(mynull, "X");

   if (masksize > 32) { return mynull; } // masksize should never be greater than 32
   if (masksize % 2 > 0) { pad = 1; } // we need an even number of hex digits

   /* just checking if input value is a valid md5 checksum (we expect 32 hex digits) */
   unsigned int size = strlen(value); int i;
   if (size == 32) { for(i=0; i<32; i++) { if(!isxdigit(value[i])) { return mynull; } } } else { return mynull; }

   // we convert pairs of chars --> unsigned int values (bytes)
   unsigned int bytes1[16]; unsigned int bytes2[16]; char *temp = (char *) palloc((2+1)*sizeof(char));

   int pos = 16-(masksize+pad)/2; // we fill every array element up to this particular position with zero values

   for(i=0; i < pos; i++) { bytes1[i] = 0; }
   /* the following four lines are supposed to deal with odd masksize values */

   if(pad) { memset(temp,0,2); strncpy(temp, &mask[0], 1); temp[2] = '\0'; sscanf(temp,"%x",&bytes1[pos]); }
   else { memset(temp,0,2); strncpy(temp, &mask[0], 2); temp[2] = '\0'; sscanf(temp,"%x",&bytes1[pos]); }

   for(i=pos+1; i<16; i++) { memset(temp,0,2); strncpy(temp,&mask[(i-pos)*2-pad],2); temp[2] = '\0'; sscanf(temp,"%x",&bytes1[i]); }

   /* on to populating bytes2 -- we don't need to worry about an odd number of hex digits here */
   for(i=0; i < 16; i++) { memset(temp, 0, 2); strncpy(temp, &value[i*2], 2); temp[2] = '\0'; sscanf(temp,"%x",&bytes2[i]); }

   pfree(mynull); pfree(zero); pfree(temp);

   // this is where we apply the bit mask through an AND operation and produce the new chars
   char *chars = (char *) palloc((32+1)*sizeof(char));
   for(i=0; i<16; i++) { sprintf(&chars[i*2], "%02x", bytes1[i] & bytes2[i]); }
   chars[32] = '\0';

   return chars;
}

static int get_depth(int rows) { return ceil(log(rows)/log(1024)); }

static int read_depth (char * target)
{
    char *schname = get_schema(target); char *tblname = get_table(target);
    char *string = "SELECT s_depth FROM pg51g.metadata WHERE t_schema = '%s' AND t_table = '%s';";
    int sql_s = strlen(string) + strlen(schname) + strlen(tblname) - 4;
    char *sql = (char *) palloc (sql_s + 1);
    sprintf(sql, string, schname, tblname);
    char *res = return_val(sql);
    int depth = 0; sscanf(res,"%d",&depth);
    return depth;
}

static bool read_is_view (char * schname, char * tblname)
{
    char *string = "SELECT t_view FROM pg51g.metadata WHERE t_schema = '%s' AND t_table = '%s';";
    int sql_s = strlen(string) + strlen(schname) + strlen(tblname) - 4;
    char *sql = (char *) palloc (sql_s + 1);
    sprintf(sql, string, schname, tblname);
    char *res = return_val(sql);
    if ( !abs(strcmp(res,"t")) ) { return true; } else { return false; }
}

static bool read_persists (char * schname, char * tblname)
{
    char *string = "SELECT t_persists FROM pg51g.metadata WHERE t_schema = '%s' AND t_table = '%s';";
    int sql_s = strlen(string) + strlen(schname) + strlen(tblname) - 4;
    char *sql = (char *) palloc (sql_s + 1);
    sprintf(sql, string, schname, tblname);
    char *res = return_val(sql);
    if ( !abs(strcmp(res,"t")) ) { return true; } else { return false; }
}

static inline int higher_levels_for(char *sigtbl, char *target, int depth, char *newkey)
{
    unsigned int tbllen = strlen(sigtbl); unsigned int keylen = strlen(newkey); int i; int mynull = -1;
    // Checking for the validity of the md5(key) value
    if (keylen == 32) { for(i=0; i<32; i++) { if(!isxdigit(newkey[i])) { return mynull; } } } else { return mynull; }

    // we start updating the right groups, one level at a time
    char *mask, *string, *pri; int temp_size;
    for(i=1; i<=depth; i++) {
        mask = get_mask(depth,i);
        pri = get_group(mask,newkey);
                temp_size = 240 + 2*strlen(sigtbl) + 3*strlen(pri) + strlen(mask);
        string = palloc(temp_size); memset(string, 0, temp_size);
        sprintf(string, "UPDATE %s SET key = '%s', val = nova.newval FROM ( SELECT pg51g.xor_md5(key) AS newkey, pg51g.xor_md5(val) AS newval FROM %s WHERE level = %d AND '%s' = pg51g.group_md5('%s',key) ) AS nova WHERE level = %d AND pri = '%s';", sigtbl, pri, sigtbl, i-1, pri, mask, i, pri);
        return_val(string);
        pfree(mask); pfree(string); pfree(pri);
    }
    return --i; // returning the highest level processed
}

/*
  process_table creates a (potentially TEMPORARY) signature table (s_schema.s_table) using a custom number of folding levels

  The C version of process_table is not meant to be called directly. (the add_table() method will call it)
  Besides t_schema, t_table as arguments, it should also require s_schema, s_table, so that we are able to
  keep snapshots, create temporary signature tables on the fly etc. [boolean temp argument] -- plus folding
  depth should probably be an argument, so that we are able to cope with folding level changes.

  We shall also need a reg_table() function (updates metadata table), functions for a new snapshots table
  and general lookup functions for the set reconciliation process (folding levels etc.)
*/

static char * process_table (char * t_schema, char * t_table, char * pkey, char * val, int depth, char * sigtbl, bool perm)
{
    // we have the name of the target table, plus the name of the signature table to be created
    char *string1 = "CREATE %s TABLE %s AS SELECT 0 AS level, %s AS pri, md5(%s) AS key, md5(%s) AS val FROM %s.%s;";
    char *sql, *mytemp; if (!perm) { mytemp = palloc(10); strcpy(mytemp, "TEMPORARY"); } else { mytemp = palloc(1); strcpy(mytemp, ""); }
    int sql_s = strlen(string1)+strlen(mytemp)+strlen(t_schema)+strlen(t_table)+strlen(pkey)+strlen(pkey)+strlen(val)+strlen(sigtbl)-8;
    sql = palloc(sql_s + 1); memset(sql, 0, sql_s + 1);
    sprintf(sql, string1, mytemp, sigtbl, pkey, pkey, val, t_schema, t_table); int ret;

    if ((ret = SPI_connect()) < 0) { ereport(ERROR,
                                                   ( errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                                     errmsg("[pg51g] process_table: connection error"),
                                                     errhint("pg51g_process_table could not SPI connect!")  )
                                     );
    }

    ret = SPI_exec(sql,0);
    if (ret < 0) { ereport(ERROR,
                                  ( errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                    errmsg("[pg51g] process_table: sql error"),
                                    errhint("Please be careful about the SQL you throw at pg51g_process_table!")  )
                          );
    }
    int rows = SPI_processed; // int depth = get_depth(rows);

    char *string2 = "SELECT %d AS level, pg51g.group_md5('%s', key) AS mypri, pg51g.group_md5('%s', key) AS mykey, pg51g.xor_md5(val) AS myval FROM %s WHERE level = %d GROUP BY pg51g.group_md5('%s', key);";
    char *string3 = "INSERT INTO ";
    char *string4 = " VALUES (%s, '%s', '%s', '%s');";
    char *tblname = get_table(sigtbl);
    char *string5 = "CREATE INDEX pg51g_%s_level_%d ON %s USING btree( pg51g.group_md5('%s', key) );";
    char *string6 = "CREATE INDEX pg51g_%s_level_pri ON %s USING btree( level, pri );";
    char *string7 = "CREATE INDEX pg51g_%s_level_%d_mask ON %s USING btree( level, pg51g.group_md5('%s', key) ) WHERE level = %d;";

    int q_s = strlen(string2) + strlen(sigtbl) + 99;
    int i, ins_s; char *mask, *q, *ins, *tmp, *idx;

    char *res = (char *) palloc(8192); strcpy(res,"");
    for(i=1; i<=depth; i++) {
        mask = get_mask(depth, i);
        q = palloc(q_s + 1);
        sprintf(q, string2, i, mask, mask, sigtbl, i-1, mask);

        ins_s = strlen(string3) + strlen(string4) + strlen(sigtbl);
        ins = palloc(ins_s + 1);
        strcpy(ins, string3); strcat(ins, sigtbl); strcat(ins, string4);

        ret = SPI_exec(q,0);
        if (ret < 0) { ereport(ERROR,
                                      ( errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                        errmsg("[pg51g] self_fold: sql error"),
                                        errhint("Please be careful about the SQL you throw at pg51g_self_fold!")  )
                              );
        }

        int count = SPI_processed; int k;
        if (SPI_tuptable != NULL && count > 0) {
            TupleDesc tupdesc = SPI_tuptable->tupdesc;
            SPITupleTable *tuptable = SPI_tuptable;
            for(k=0; k<count; k++) {
                HeapTuple tuple = tuptable->vals[k]; // we only need the first field of the first tuple
                if (!(tuple == 0)) {
                    char *one = SPI_getvalue(tuple, tupdesc, 1);
                    char *two = SPI_getvalue(tuple, tupdesc, 2);
                    char *three = SPI_getvalue(tuple, tupdesc, 3);
                    char *four = SPI_getvalue(tuple, tupdesc, 4);
                    char *five = palloc( strlen(ins) + strlen(one) + strlen(two) + strlen(three) + strlen(four) );
                    sprintf(five, ins, one, two, three, four);
                    SPI_push(); char *tmp = return_val(five); strcpy(res, tmp); SPI_pop();
                }
            }
            char *six = palloc( strlen(string5) + strlen(tblname) + strlen(sigtbl) + strlen(mask) + 2 );
            sprintf(six, string5, tblname, i, sigtbl, mask);
            SPI_push(); char *hello = return_val(six); SPI_pop();
            char *eight = palloc( strlen(string7) + strlen(tblname) + strlen(sigtbl) + strlen(mask) + 4 );
            sprintf(eight, string7, tblname, i, sigtbl, mask, i-1);
            SPI_push(); char *any = return_val(eight); SPI_pop();
        }
    }
    char *seven = palloc( strlen(string6) + strlen(tblname) + strlen(sigtbl) + 2 );
    sprintf(seven, string6, tblname, sigtbl);
    SPI_push(); char *bye = return_val(seven); SPI_pop();

    SPI_finish();
    return res;
}

static char * reg_table (char * t_schema, char * t_table, char * t_key, char * t_val, int depth, bool persists, bool is_view) 
{
    // count, definition, s_schema, s_table have to be deduced by this function

    // here we discover s_schema, s_table
    char *s_schema = get_sig_schema(t_schema, t_table);
    char *s_table = get_sig_table(t_schema, t_table);

    // here we discover count
    char *string1 = "SELECT COUNT(*) FROM %s.%s;";
    int sql_s = strlen(string1) + strlen(t_schema) + strlen(t_table) - 4;
    char *sql = (char *) palloc( sql_s + 1 );
    sprintf(sql, string1, t_schema, t_table);
    char *count = return_val(sql); int rows = 0; sscanf(count,"%d",&rows);

    // here is definition
    char *mydef = table_definition(t_schema, t_table);

    // we need to find out if another row for t_schema, t_table already exists in metadata table
    char *string2 = "SELECT COUNT(*) FROM pg51g.metadata WHERE t_schema = '%s' AND t_table = '%s';";
    int counting_s = strlen(string2) + strlen(t_schema) + strlen(t_table) - 4;
    char *counting = (char *) palloc(counting_s + 1);
    sprintf(counting, string2, t_schema, t_table);
    char *meta = return_val(counting); int meta_rows = 0; sscanf(meta,"%d",&meta_rows);

    // problem areas: length of boolean to text, length of integer to text
    // at the moment t_persists and s_depth are static

    char *mt_key = escape_quotes(t_key); char *mt_val = escape_quotes(t_val);
    char *string3 = "INSERT INTO pg51g.metadata (t_schema, t_table, t_count, t_persists, t_view, t_definition, t_key_sql, t_val_sql, s_schema, s_table, s_depth) VALUES ('%s', '%s', %s, %s, %s, '%s', '%s', '%s', '%s', '%s', %d);";
    int ins_s =strlen(string3)+strlen(t_schema)+strlen(t_table)+strlen(count)+strlen(mydef)+strlen(mt_key)+strlen(mt_val)+strlen(s_schema)+strlen(s_table);
    char *ins = (char *) palloc (ins_s + 1);
    sprintf(ins, string3, t_schema, t_table, count, (persists)?"true":"false", (is_view)?"true":"false", mydef, mt_key, mt_val, s_schema, s_table, depth);

    char *res;
    if (!(meta_rows > 0)) { res = return_val(ins); }
    else { res = "whatever"; }

    pfree(sql); pfree(counting); pfree(ins);

    return count;
}

static char * unreg_table (char * t_schema, char * t_table) 
{
   char *string = "DELETE FROM pg51g.metadata WHERE t_schema = '%s' AND t_table = '%s';";
   int sql_s = strlen(string) + strlen(t_schema) + strlen(t_table) - 4;
   char *sql = (char *) palloc (sql_s + 1);
   sprintf(sql, string, t_schema, t_table);
   char *val = return_val(sql);
   return val;
}

/* this function saves a copy of the signature table for future reference / diffing */

static char * save_table (char * t_schema, char * t_table)
{
    char *sigtbl = get_sig_target(t_schema, t_table);
    char *target = get_snap_target(t_schema, t_table);
    char *string = "CREATE TABLE %s AS SELECT * FROM %s;";
    int sql_s = strlen(string)+strlen(sigtbl)+strlen(target)-4;
    char *sql = palloc(sql_s + 1);
    sprintf(sql, string, target, sigtbl);
    char *res = return_val(sql);

    // *orig holds full name of table with real data 
    char *orig = (char *) palloc( strlen(t_schema) + strlen(t_table) + 2 );
    sprintf(orig, "%s.%s", t_schema, t_table);

    // generating some indices
    char *target_tbl = get_table(target);
    int depth = read_depth(orig); int i; char *mask, *ret, *six, *seven;
    char *string5 = "CREATE INDEX saved_pg51g_%s_level_%d ON %s USING btree( pg51g.group_md5('%s', key) );";
    char *string6 = "CREATE INDEX saved_pg51g_%s_level_pri ON %s USING btree( level, pri );";

    for(i=1; i<=depth; i++) {
        mask = get_mask(depth, i);
        six = palloc( strlen(string5) + strlen(target_tbl) + strlen(target) + strlen(mask) + 2 );
        sprintf(six, string5, target_tbl, i, target, mask);
        ret = return_val(six);
    }
    seven = palloc( strlen(string6) + strlen(target_tbl) + strlen(target) + 2 );
    sprintf(seven, string6, target_tbl, target);
    ret = return_val(seven);
    return res;
}

static char * install_trigger (char * t_schema, char * t_table)
{
    char *string = "CREATE TRIGGER pg51g_%s_%s_on_change AFTER INSERT OR UPDATE OR DELETE ON %s.%s FOR EACH ROW EXECUTE PROCEDURE pg51g.on_change();";
    int sql_s = strlen(string) + strlen(t_schema) + strlen(t_table) + strlen(t_schema) + strlen(t_table);
    char *sql = (char *) palloc (sql_s + 1);
    sprintf(sql, string, t_schema, t_table, t_schema, t_table);
    int ret;

    if ((ret = SPI_connect()) < 0) { ereport(ERROR,
                                                   ( errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                                     errmsg("[pg51g] install_trigger: connection error"),
                                                     errhint("pg51g_install_trigger could not SPI connect!")  )
                                     );
    }

    ret = SPI_exec(sql,0);
    if (ret < 0) { ereport(ERROR,
                                  ( errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                    errmsg("[pg51g] install_trigger: sql error"),
                                    errhint("Please be careful about the SQL you throw at pg51g_install_trigger!")  )
                          );
    }

    SPI_finish();
    return sql;
}

static char * remove_trigger (char * t_schema, char * t_table)
{
    char *string = "DROP TRIGGER pg51g_%s_%s_on_change ON %s.%s;";
    int sql_s = strlen(string) + strlen(t_schema) + strlen(t_table) + strlen(t_schema) + strlen(t_table) - 8;
    char *sql = (char *) palloc (sql_s + 1);
    sprintf(sql, string, t_schema, t_table, t_schema, t_table);
    int ret;

    if ((ret = SPI_connect()) < 0) { ereport(ERROR,
                                                   ( errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                                     errmsg("[pg51g] remove_trigger: connection error"),
                                                     errhint("pg51g_remove_trigger could not SPI connect!")  )
                                     );
    }

    ret = SPI_exec(sql,0);
    if (ret < 0) { ereport(ERROR,
                                  ( errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                    errmsg("[pg51g] remove_trigger: sql error"),
                                    errhint("Please be careful about the SQL you throw at pg51g_remove_trigger!")  )
                          );
    }

    SPI_finish();
    return sql;
}

static char * install_freeze (char * t_schema, char * t_table)
{
    char *string = "CREATE TRIGGER pg51g_%s_%s_freeze AFTER INSERT OR UPDATE OR DELETE ON %s.%s FOR EACH ROW EXECUTE PROCEDURE pg51g.freeze();";
    int sql_s = strlen(string) + strlen(t_schema) + strlen(t_table) + strlen(t_schema) + strlen(t_table);
    char *sql = (char *) palloc (sql_s + 1);
    sprintf(sql, string, t_schema, t_table, t_schema, t_table);
    int ret;

    if ((ret = SPI_connect()) < 0) { ereport(ERROR,
                                                   ( errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                                     errmsg("[pg51g] install_freeze: connection error"),
                                                     errhint("pg51g_install_freeze could not SPI connect!")  )
                                     );
    }

    ret = SPI_exec(sql,0);
    if (ret < 0) { ereport(ERROR,
                                  ( errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                    errmsg("[pg51g] install_freeze: sql error"),
                                    errhint("Please be careful about the SQL you throw at pg51g_install_freeze!")  )
                          );
    }

    SPI_finish();
    return sql;
}

static char * remove_freeze (char * t_schema, char * t_table)
{
    char *string = "DROP TRIGGER pg51g_%s_%s_freeze ON %s.%s;";
    int sql_s = strlen(string) + strlen(t_schema) + strlen(t_table) + strlen(t_schema) + strlen(t_table) - 8;
    char *sql = (char *) palloc (sql_s + 1);
    sprintf(sql, string, t_schema, t_table, t_schema, t_table);
    int ret;

    if ((ret = SPI_connect()) < 0) { ereport(ERROR,
                                                   ( errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                                     errmsg("[pg51g] remove_freeze: connection error"),
                                                     errhint("pg51g_remove_freeze could not SPI connect!")  )
                                     );
    }

    ret = SPI_exec(sql,0);
    if (ret < 0) { ereport(ERROR,
                                  ( errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                    errmsg("[pg51g] remove_freeze: sql error"),
                                    errhint("Please be careful about the SQL you throw at pg51g_remove_freeze!")  )
                          );
    }

    SPI_finish();
    return sql;
}

static char * stamp_metadata (char * t_schema, char * t_table)
{
    char *string = "UPDATE pg51g.metadata SET updated = now() WHERE t_schema = '%s' AND t_table = '%s';";
    int sql_s = strlen(string) + strlen(t_schema) + strlen(t_table) - 4;
    char *sql = (char *) palloc (sql_s + 1);
    sprintf(sql, string, t_schema, t_table);
    char *res = return_val(sql); 
    return res;
}

static int num_of_concats (char * arg)
{
    int size = strlen(arg); int i, num = 0;
    char *temp = (char *) palloc (2 * sizeof(char) + 1);
    for(i=0; i<size-1; i++) {
        strncpy(temp, &arg[i], 2); temp[2] = '\0';
        if ( !abs(strcmp(temp,"||")) ) { num++; }
    }
    return num;
}

static char * append_val_to_string (char * field, HeapTuple tuple, TupleDesc desc, char * string)
{
    int i, res; bool search = true; char *result;
    for (i = 0; i < desc->natts; i++) {
        if ( search && !abs(strcmp(field, NameStr(desc->attrs[i]->attname))) ) {
            search = false; res = i;
        }
    }
    if (!search) { result = SPI_getvalue(tuple, desc, res+1); } else { result = ""; }
    char *output = (char *) palloc( strlen(string) + strlen(result) + 1 );
    strcpy(output, string); strcat(output, result); pfree(string);
    return output;
}

static char * eval_pri_from_tuple (HeapTuple tuple, TupleDesc desc, char * key)
{
    int i, res; bool search = true; char *result;
    char *ekey = escape_quotes(key); // perhaps we want some delimiters in our concatenation
    if ( strlen(key) != strlen(ekey) ) { return strip_quotes(key) ; }
    for (i = 0; i < desc->natts; i++) {
        if ( search && !abs(strcmp(key, NameStr(desc->attrs[i]->attname))) ) {
            search = false; res = i;
        }
    }
    if (!search) { result = SPI_getvalue(tuple, desc, res+1); } else { result = ""; }
    return result;
}

static char * eval_val_from_tuple (HeapTuple tuple, TupleDesc desc, char * val)
{
    int i, res; bool search = true; char *result;
    char *token = extract_from_double_quotes(val);  // we assume token is enclosed in double quotes
                                                                                                        // like so: COALESCE("myserial1"::TEXT,'#--NULL--#')
    if ( strlen(val) == strlen(token) ) { return strip_quotes(val) ; }
    for (i = 0; i < desc->natts; i++) {
        if ( search && !abs(strcmp(token, NameStr(desc->attrs[i]->attname))) ) {
            search = false; res = i;
        }
    }
    if (!search) { result = SPI_getvalue(tuple, desc, res+1); }
        if (result) { return result; } else { return "#--NULL--#"; }
}

static char * super_eval_pri_from_tuple (HeapTuple tuple, TupleDesc desc, char * key)
{
    char *result = (char *) palloc (8192);
    strcpy(result, "");

    int size = strlen(key); int i, left, right, tsize;
    left = 0; right = 0; tsize = 0; char *token;
    char *temp = (char *) palloc (2 * sizeof(char) + 1);

    for(i=0; i<size-1; i++) {
        strncpy(temp, &key[i], 2); temp[2] = '\0';
        if ( !abs(strcmp(temp,"||")) ) {
            tsize = i - left;
            token = palloc ( tsize * sizeof(char) + 1 );
            strncpy(token, &key[left], tsize); token[tsize] = '\0';
            strcat(result, eval_pri_from_tuple(tuple, desc, token));
            pfree(token);
            left = i + 2;
        }
    }
    tsize = size - left;
    if(tsize) {
        token = palloc ( tsize * sizeof(char) + 1 );
        strncpy(token, &key[left], tsize); token[tsize] = '\0';
        strcat(result, eval_pri_from_tuple(tuple, desc, token));
    }
    return result;
}

static char * super_eval_val_from_tuple (HeapTuple tuple, TupleDesc desc, char * val)
{
    char *result = (char *) palloc (8192);
    strcpy(result, "");

    int size = strlen(val); int i, left, right, tsize;
    left = 0; right = 0; tsize = 0; char *token;
    char *temp = (char *) palloc (2 * sizeof(char) + 1);

    for(i=0; i<size-1; i++) {
        strncpy(temp, &val[i], 2); temp[2] = '\0';
        if ( !abs(strcmp(temp,"||")) ) {
            tsize = i - left;
            token = palloc ( tsize * sizeof(char) + 1 );
            strncpy(token, &val[left], tsize); token[tsize] = '\0';
            strcat(result, eval_val_from_tuple(tuple, desc, token));
            pfree(token);
            left = i + 2;
        }
    }
    tsize = size - left;
    if(tsize) {
        token = palloc ( tsize * sizeof(char) + 1 );
        strncpy(token, &val[left], tsize); token[tsize] = '\0';
        strcat(result, eval_val_from_tuple(tuple, desc, token));
    }
    return result;
}

static char * increment_count (char * t_schema, char * t_table)
{
    char *string = "UPDATE pg51g.metadata SET t_count = t_count + 1 WHERE t_schema = '%s' AND t_table = '%s';";
    int sql_s = strlen(string) + strlen(t_schema) + strlen(t_table);
    char *sql = (char *) palloc (sql_s + 1);
    sprintf(sql, string, t_schema, t_table);
    char *res = return_val(sql);
    return res;
}

static char * decrement_count (char * t_schema, char * t_table)
{
    char *string = "UPDATE pg51g.metadata SET t_count = t_count - 1 WHERE t_schema = '%s' AND t_table = '%s';";
    int sql_s = strlen(string) + strlen(t_schema) + strlen(t_table);
    char *sql = (char *) palloc (sql_s + 1);
    sprintf(sql, string, t_schema, t_table);
    char *res = return_val(sql);
    return res;
}

static char * sync_saved_row( char *current_tbl, char *saved_tbl, char *op, char *key, int level )
{
        int somesize = strlen(current_tbl) + strlen(saved_tbl) + strlen(op) + strlen(key);
        char *somesql = (char *) palloc ( 2*somesize * sizeof(char) ); strcpy(somesql, "");

    if ( !abs(strcmp(op,"INSERT")) ) {
                sprintf(somesql, "INSERT INTO %s SELECT * FROM %s WHERE level = %d AND pri = '%s';", saved_tbl, current_tbl, level, key);
        }
    if ( !abs(strcmp(op,"DELETE")) ) {
                sprintf(somesql, "DELETE FROM %s WHERE level = %d AND pri = '%s';", saved_tbl, level, key);
        }
    if ( !abs(strcmp(op,"UPDATE")) ) {
                sprintf(somesql, "UPDATE %s SET val = (SELECT val FROM %s WHERE level = %d AND pri = '%s') WHERE level = %d AND pri = '%s';", saved_tbl, current_tbl, level, key, level, key);
        }
    char *res = return_val(somesql);
    pfree(somesql);

        // elog(NOTICE, "[pg51g] '%s' - %s '%s' level %d", current_tbl, op, key, level );
        return res;
}

int test_push( char *current_tbl, char *saved_tbl, char *op, char *key, int level )
{
        elog(NOTICE, "[pg51g] '%s' - %s '%s' level %d", current_tbl, op, key, level );
        return 1;
}

PG_FUNCTION_INFO_V1(xor_md5);
Datum xor_md5(PG_FUNCTION_ARGS) {

   int agg = 0; // has this function been called as an aggregate function? agg = 1 if it was, agg = 0 otherwise
   if (fcinfo->context && IsA(fcinfo->context, AggState)) { agg = 1; }

   /* let's check if the first argument is null - even if this is the first iteration of an aggregate, we expect all zeros not null */
   if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }

   int i;
   VarChar *state = PG_GETARG_VARCHAR_P(0);
   unsigned int size1 = VARSIZE(state) - VARHDRSZ;
   if (size1 == 32) { for(i=0; i<32; i++) { if(!isxdigit(VARDATA(state)[i])) { PG_RETURN_NULL(); } } } else { PG_RETURN_NULL(); }
   /* if we have made it this far, we must have a valid first argument */

   /* on to checking the second argument */
   if (PG_ARGISNULL(1)) { PG_RETURN_NULL(); } // a single null will propagate to the very end if this is an aggregate call
   VarChar *arg = PG_GETARG_VARCHAR_P(1);
   unsigned int size2 = VARSIZE(arg) - VARHDRSZ;

   /* checking if the variable arg points to what we expect (32 hex digits) */
   if (size2 == 32) { for(i=0; i<32; i++) { if(!isxdigit(VARDATA(arg)[i])) { PG_RETURN_NULL(); } } } else { PG_RETURN_NULL(); }

   // we convert each pair of chars in (state,arg) --> unsigned int values (bytes)
   unsigned int bytes1[16]; unsigned int bytes2[16];
   char *temp = (char *) palloc(2+1);
   for(i=0; i < 16; i++) { memset(temp, 0, 2); strncpy(temp, &VARDATA(state)[i*2], 2); temp[2] = '\0'; sscanf(temp,"%x",&bytes1[i]); }
   for(i=0; i < 16; i++) { memset(temp, 0, 2); strncpy(temp, &VARDATA(arg)[i*2], 2); temp[2] = '\0'; sscanf(temp,"%x",&bytes2[i]); }

   // this is were we XOR each byte and produce the new chars
   char *chars = (char *) palloc(32+1);
   for(i=0; i<16; i++) { sprintf(&chars[i*2], "%02x", bytes1[i] ^ bytes2[i]); }

   // if pg51g.xor_md5 is called as aggregate, we'll modify the first argument in place, else return a new value
   if(agg) {
      memcpy(VARDATA(state), chars, size2);
      PG_RETURN_POINTER(state);
   }
   else {
      int32 new_size = size2 + VARHDRSZ;
      VarChar *new_varchar = (VarChar *) palloc(new_size);
      SET_VARSIZE(new_varchar, new_size);
      memcpy(VARDATA(new_varchar), chars, size2);
      PG_RETURN_VARCHAR_P(new_varchar);
   }
}

PG_FUNCTION_INFO_V1(is_md5);
Datum is_md5(PG_FUNCTION_ARGS) {
   if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }
   VarChar *value = PG_GETARG_VARCHAR_P(0);
   unsigned int size = VARSIZE(value) - VARHDRSZ; int i;
   if (size == 32) {
      for(i=0; i<size; i++) { if(!isxdigit(VARDATA(value)[i])) { PG_RETURN_BOOL(0); } }
      PG_RETURN_BOOL(1);
   }
   else { PG_RETURN_BOOL(0); }
}

PG_FUNCTION_INFO_V1(group_md5);
Datum group_md5(PG_FUNCTION_ARGS) {
   if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); } if (PG_ARGISNULL(1)) { PG_RETURN_NULL(); }

   VarChar *mask = PG_GETARG_VARCHAR_P(0);
   VarChar *value = PG_GETARG_VARCHAR_P(1);

   /* storing the size of the bit mask string in variable masksize -- we need to align the two strings for bitwise AND */
   unsigned int masksize = VARSIZE(mask) - VARHDRSZ; int pad = 0; char *zero = (char *) palloc((1+1)*sizeof(char)); strncpy(zero, "0", 1);

   if (masksize > 32) { PG_RETURN_NULL(); } // masksize should never be greater than 32
   if (masksize % 2 > 0) { pad = 1; } // we need an even number of hex digits

   /* just checking if input value is a valid md5 checksum (we expect 32 hex digits) */
   unsigned int size = VARSIZE(value) - VARHDRSZ; int i;
   if (size == 32) { for(i=0; i<32; i++) { if(!isxdigit(VARDATA(value)[i])) { PG_RETURN_NULL(); } } } else { PG_RETURN_NULL(); }

   // we convert pairs of chars --> unsigned int values (bytes)
   unsigned int bytes1[16]; unsigned int bytes2[16]; char *temp = (char *) palloc((2+1)*sizeof(char));

   int pos = 16-(masksize+pad)/2; // we fill every array element up to this particular position with zero values

   for(i=0; i < pos; i++) { bytes1[i] = 0; }
   /* the following four lines are supposed to deal with odd masksize values */

   if(pad) { memset(temp,0,2); strncpy(temp, &VARDATA(mask)[0], 1); temp[2] = '\0'; sscanf(temp,"%x",&bytes1[pos]); }
   else { memset(temp,0,2); strncpy(temp, &VARDATA(mask)[0], 2); temp[2] = '\0'; sscanf(temp,"%x",&bytes1[pos]); }

   for(i=pos+1; i<16; i++) { memset(temp,0,2); strncpy(temp,&VARDATA(mask)[(i-pos)*2-pad],2); temp[2] = '\0'; sscanf(temp,"%x",&bytes1[i]); }

   /* on to populating bytes2 -- we don't need to worry about an odd number of hex digits here */
   for(i=0; i < 16; i++) { memset(temp, 0, 2); strncpy(temp, &VARDATA(value)[i*2], 2); temp[2] = '\0'; sscanf(temp,"%x",&bytes2[i]); }

   // this is where we apply the bit mask through an AND operation and produce the new chars
   char *chars = (char *) palloc((32+1)*sizeof(char));
   for(i=0; i<16; i++) { sprintf(&chars[i*2], "%02x", bytes1[i] & bytes2[i]); }
   chars[32] = '\0';

   int32 new_size = size + VARHDRSZ;
   VarChar *new_varchar = (VarChar *) palloc(new_size);
   SET_VARSIZE(new_varchar, new_size);
   memcpy(VARDATA(new_varchar), chars, size);
   PG_RETURN_VARCHAR_P(new_varchar);
}

PG_FUNCTION_INFO_V1(on_change);
Datum on_change(PG_FUNCTION_ARGS)
{
    TriggerData *trigdata = (TriggerData *) fcinfo->context;
    TupleDesc tupdesc;
    HeapTuple rettuple;
    bool checknull = false;
    bool isnull;
    int ret, i;
    char *mysql;

    /* make sure it's called as a trigger at all */
    if (!CALLED_AS_TRIGGER(fcinfo)) elog (ERROR, "pg51g.on_change(): not called by trigger manager");
    
    /* tuple to return to executor */
    if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event)) rettuple = trigdata->tg_newtuple;
    else rettuple = trigdata->tg_trigtuple;

    /* this trigger is supposed to operate after the insertion/update/deletion, not before */
    if (TRIGGER_FIRED_BEFORE(trigdata->tg_event)) { rettuple = NULL; return PointerGetDatum(rettuple); }

    tupdesc = trigdata->tg_relation->rd_att;

    char *tblname = SPI_getrelname(trigdata->tg_relation);
    char *schname = SPI_getnspname(trigdata->tg_relation);

    // *target holds full name: schema.table, *sigtbl holds signature table: pg51g.schema_table
    char *target = (char *) palloc( strlen(schname) + strlen(tblname) + 2 );
    sprintf(target, "%s.%s", schname, tblname);
    char *sigtbl = get_sig_target(schname, tblname);
   
    char *key = read_pkey_pure(schname, tblname);
    char *val = read_val_sql(schname, tblname);
 
    // we need to support complex (alternative) pkey definitions, hence eval_pri_from_tuple()
    char *mypri = super_eval_pri_from_tuple(rettuple, tupdesc, key);

        // trying to do something similar for the val checksum, to avoid table scans / increase perf
    char *myval = super_eval_val_from_tuple(rettuple, tupdesc, val);

    // just in case we are using a text field as a primary or pseudo-primary key
    char *pri = escape_backslashes(escape_quotes(mypri));

    // we are going to use a Direct Function Call to the md5_text Postgresql function to get newkey

    text *rawstr_text; rawstr_text = DatumGetTextP(DirectFunctionCall1(textin, CStringGetDatum(mypri)));
    text *mytext = DatumGetTextP( DirectFunctionCall1( md5_text, PointerGetDatum(rawstr_text) ) );
    unsigned int mytextlen = VARSIZE(mytext) - VARHDRSZ;
    char *newkey = (char *) palloc(mytextlen+1); strncpy(newkey, &VARDATA(mytext)[0], mytextlen); newkey[mytextlen] = '\0';

    text *rawstr_val; rawstr_val = DatumGetTextP(DirectFunctionCall1(textin, CStringGetDatum(myval)));
    text *myvaltext = DatumGetTextP( DirectFunctionCall1( md5_text, PointerGetDatum(rawstr_val) ) );
    unsigned int myvallen = VARSIZE(myvaltext) - VARHDRSZ;
    char *newval = (char *) palloc(myvallen+1); strncpy(newval, &VARDATA(myvaltext)[0], myvallen); newval[myvallen] = '\0';

    // once we start using a metadata table for record counts, we should probably increment/decrement the counters here
    if (!TRIGGER_FIRED_BY_DELETE(trigdata->tg_event)) {

        if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event)) {
            // *key hold the name of the target table's primary key, *val describes the concatenation of fields for the val calculation
            mysql = palloc ( 256 + strlen(sigtbl) + 3*strlen(key) + strlen(val) + 3*strlen(pri) + 2*strlen(target) );
            sprintf(mysql, "INSERT INTO %s (level, pri, key, val) VALUES ( 0, '%s', '%s', '%s' );", sigtbl, pri, newkey, newval);
            on_change_exec(mysql); pfree(mysql);
            increment_count(schname, tblname);
        } else {
            // *key holds the name of the target table's primary key, *val describes the concatenation of fields for the val calculation
            mysql = palloc ( 256 + strlen(target) + strlen(key) + strlen(val) );
            sprintf(mysql, "UPDATE %s SET key = '%s', val = '%s' WHERE level = 0 AND pri = '%s';", sigtbl, newkey, newval, pri);
            on_change_exec(mysql); pfree(mysql);
       }
   } else {
        mysql = palloc ( 256 + strlen(target) );
        sprintf(mysql, "DELETE FROM %s WHERE level = 0 AND pri = '%s';", sigtbl, pri);
        on_change_exec(mysql); pfree(mysql);
        decrement_count(schname, tblname);
    }

    // Getting folding depth for this record count -- from pg51g.metadata
    int depth = read_depth(target);
    
        if (depth > 0) {
        higher_levels_for(sigtbl, target, depth, newkey);
        stamp_metadata(schname, tblname);
        }

    return PointerGetDatum(rettuple);
}

// if (!RelationGetForm(resultRelation)->relhasindex)

PG_FUNCTION_INFO_V1(dummy_trigger);
Datum dummy_trigger(PG_FUNCTION_ARGS)
{
    TriggerData *trigdata = (TriggerData *) fcinfo->context;
    TupleDesc tupdesc;
    HeapTuple rettuple;
    bool checknull = false;
    bool isnull;
    int ret, i;
    char *mysql;

    /* make sure it's called as a trigger at all */
    if (!CALLED_AS_TRIGGER(fcinfo)) elog (ERROR, "pg51g.dummy_trigger(): not called by trigger manager");
    
    /* tuple to return to executor */
    if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event)) rettuple = trigdata->tg_newtuple;
    else rettuple = trigdata->tg_trigtuple;

    /* this trigger is supposed to operate after the insertion/update/deletion, not before */
    if (TRIGGER_FIRED_BEFORE(trigdata->tg_event)) { rettuple = NULL; return PointerGetDatum(rettuple); }

    tupdesc = trigdata->tg_relation->rd_att;

    char *tblname = SPI_getrelname(trigdata->tg_relation);
    char *schname = SPI_getnspname(trigdata->tg_relation);

    return PointerGetDatum(rettuple);
}

PG_FUNCTION_INFO_V1(freeze_trigger);
Datum freeze_trigger(PG_FUNCTION_ARGS)
{
    TriggerData *trigdata = (TriggerData *) fcinfo->context;
    TupleDesc tupdesc;
    HeapTuple rettuple;

    char *tblname = SPI_getrelname(trigdata->tg_relation);
    char *schname = SPI_getnspname(trigdata->tg_relation);

    /* make sure it's called as a trigger at all */
    if (!CALLED_AS_TRIGGER(fcinfo)) elog (ERROR, "pg51g.freeze_trigger(): not called by trigger manager");

    elog (ERROR, "Table %s.%s has been frozen by pg51g.freeze()", schname, tblname);
    
    rettuple = NULL; return PointerGetDatum(rettuple);
}

PG_FUNCTION_INFO_V1(add_table);
Datum add_table(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); } 

    VarChar *sql = PG_GETARG_VARCHAR_P(0); unsigned int size = VARSIZE(sql) - VARHDRSZ;
    char *buf = (char *) palloc(size+1); int ret; char *res = (char *) palloc(8192);
    strncpy(buf,VARDATA(sql),size); buf[size] = '\0'; // converting varchar into native string

    char *schname = get_schema(buf);
    char *tblname = get_table(buf);

    // if the table has been already added to the metadata table, return null
    if( table_in_metadata(schname, tblname)  ) {
        elog(ERROR, "[pg51g] add_table('%s'): already exists in pg51g.metadata", buf);
        PG_RETURN_NULL();
    }

    char *pkey = pkey_text(schname, tblname);
    char *val = val_text(schname, tblname);

    // if the table doesn't exist, has no primary key or no pkey,val definitions in the alternatives table, return null
    if( !( strlen(pkey) > 0 && strlen(val) > 0 )  ) {
        elog(ERROR, "[pg51g] add_table('%s'): does this table have a primary key?", buf);
        PG_RETURN_NULL();
    }

    char *string = "SELECT COUNT(*) FROM %s;";
    int csql_s = strlen(string) + strlen(buf) + 1;
    char *csql = (char *) palloc (csql_s + 1); sprintf(csql, string, buf);
    char *count = return_val(csql); int trows = 0; sscanf(count,"%d",&trows);

    // Getting folding depth for this record count
    int depth = get_depth(trows);

    // schema-related stuff... [maybe]
    // CREATE VIEW pg51g.public_test1_columns AS SELECT ordinal_position, column_name, data_type, is_nullable, column_default, is_updatable FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'test1' ORDER BY ordinal_position;

    char *sigtbl = get_sig_target(schname, tblname);

    // generating the signature table -- false as last argument will create TEMPORARY signature table
    res = process_table(schname, tblname, pkey, val, depth, sigtbl, true);

    // registering the new signature table in the metadata table
    char *reg = reg_table(schname, tblname, pkey, val, depth, !table_is_temp(tblname), table_is_view(schname, tblname));

    int trig_s = strlen(schname) + strlen(tblname) + 17;
    char *trig = (char *) palloc (trig_s + 1);
    sprintf(trig, "pg51g_%s_%s_on_change", schname, tblname);

    // the trigger may already exist in the system
    char *string1 = "SELECT COUNT(*) FROM pg_trigger WHERE tgname = '%s';";
    int tgl_s = strlen(string1) + strlen(trig) - 2;
    char *tgl = (char *) palloc (tgl_s + 1);
    sprintf(tgl, string1, trig);
    char *returned = return_val(tgl); int rows = 0; sscanf(returned,"%d",&rows);

    // what if our relation is a view? we should skip the trigger installation
    if ( !( rows > 0 || table_is_view(schname, tblname) || table_is_temp(tblname) ) ) { res = install_trigger(schname, tblname); }

    int res_s = strlen(sigtbl);
    int32 new_size = res_s + VARHDRSZ;
    VarChar *new_varchar = (VarChar *) palloc(new_size);
    SET_VARSIZE(new_varchar, new_size);
    memcpy(VARDATA(new_varchar), sigtbl, res_s); pfree(buf);
    PG_RETURN_VARCHAR_P(new_varchar);
}

PG_FUNCTION_INFO_V1(rm_table);
Datum rm_table(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); } 

    VarChar *sql = PG_GETARG_VARCHAR_P(0); unsigned int size = VARSIZE(sql) - VARHDRSZ;
    char *buf = (char *) palloc(size+1); int ret; char *res = (char *) palloc(8192);
    strncpy(buf,VARDATA(sql),size); buf[size] = '\0'; // converting varchar into native string

    char *schname = get_schema(buf);
    char *tblname = get_table(buf);

    // if the table is not in the metadata table, return null -- but check for temporary tables, too
    if( !table_in_metadata(schname, tblname) ) {
        if( table_in_metadata("pg_temp_1", tblname) ) { schname = "pg_temp_1"; }
        else {
            elog(ERROR, "[pg51g] rm_table('%s'): no such entry in pg51g.metadata", buf);
            PG_RETURN_NULL();
        }
    }

    // char *sigtbl; if( table_persists(schname, tblname) ) { sigtbl = "true"; } else { sigtbl = "false"; }

    // 1. getting rid of the trigger

    int trig_s = strlen(schname) + strlen(tblname) + 17;
    char *trig = (char *) palloc (trig_s + 1);
    sprintf(trig, "pg51g_%s_%s_on_change", schname, tblname);

    char *string1 = "SELECT COUNT(*) FROM pg_trigger WHERE tgname = '%s';";
    int csql_s = strlen(string1) + strlen(trig) - 2;
    char *csql = (char *) palloc (csql_s + 1);
    sprintf(csql, string1, trig);
    char *returned = return_val(csql); int rows = 0; sscanf(returned,"%d",&rows);

    if ( rows > 0 ) { res = remove_trigger(schname, tblname); }

    // 2. drop the signatures table

    char *sigtbl = read_sig_table(schname, tblname);
    char *string2 = "DROP TABLE IF EXISTS pg51g.%s;";
    int dsql_s = strlen(string2) + strlen(sigtbl) - 2;
    char *dsql = (char *) palloc (dsql_s + 1);
    sprintf(dsql, string2, sigtbl);
    char *dres = return_val(dsql);

    // 3. delete the metadata entry

    char *mres = unreg_table(schname, tblname);

    int res_s = strlen(sigtbl);
    int32 new_size = res_s + VARHDRSZ;
    VarChar *new_varchar = (VarChar *) palloc(new_size);
    SET_VARSIZE(new_varchar, new_size);
    memcpy(VARDATA(new_varchar), sigtbl, res_s); pfree(buf);
    PG_RETURN_VARCHAR_P(new_varchar);
}


typedef struct {
    int pos;
    int capacity;
    char **keys;
    char **ops;
    char **matched;
} dlist;

dlist * new_dlist()
{
    dlist *g = (dlist *) palloc( sizeof(dlist) );
    g->pos = 0;
    g->capacity = 4;
    g->keys = (char**)palloc(4 * sizeof(char*));
    g->ops = (char**)palloc(4 * sizeof(char*));
    g->matched = (char**)palloc(4 * sizeof(char*));
    return g;
}

dlist * clone_dlist(dlist *old)
{
    dlist *g = (dlist *) palloc( old->capacity * sizeof(dlist) );
    g->pos = old->pos;
    g->capacity = old->capacity;
    g->keys = (char**)palloc( old->capacity * sizeof(char*));
    g->ops = (char**)palloc( old->capacity * sizeof(char*));
    g->matched = (char**)palloc( old->capacity * sizeof(char*));
    int i;
    for(i=0; i < g->pos; i++) {
        g->keys[i] = old->keys[i];
        g->ops[i] = old->ops[i];
        g->matched[i] = old->matched[i];
    }
    return g;
}

void del_dlist(dlist *g)
{
    g->capacity = 0;
    g->pos = 0;
    pfree(g->keys);
    pfree(g->ops);
    pfree(g->matched);
    pfree(g);
}

void add_to_dlist (dlist *g, char * pri, char * op)
{
    if (g->pos == g->capacity) {
        int newcapacity = g->capacity * 2; int i;
        char ** newkeys = (char**)palloc(newcapacity * sizeof(char*));
        char ** newops  = (char**)palloc(newcapacity * sizeof(char*));
        char ** newmatched  = (char**)palloc(newcapacity * sizeof(char*));
        for(i=0; i < g->capacity; i++) { newkeys[i] = g->keys[i]; newops[i] = g->ops[i]; newmatched[i] = g->matched[i]; }
        pfree(g->keys); pfree(g->ops); pfree(g->matched);
        g->capacity = newcapacity;
        g->keys = newkeys; g->ops = newops; g->matched = newmatched;
    }
    int pos = g->pos;
    g->keys[pos] = pri;
    g->ops[pos] = op;
    g->matched[pos] = "f";
    g->pos++;
}

void append_dlist(dlist *g, dlist *new)
{
    int elements = new->pos; int i;
    for(i=0; i < elements; i++) { add_to_dlist(g, new->keys[i], new->ops[i]); }
}

int get_dlist_current_pos (dlist *g) { return g->pos; }

char * get_dlist_key (dlist *g, int pos) { return g->keys[pos]; }
char * get_dlist_op (dlist *g, int pos) { return g->ops[pos]; }

void set_dlist_matched (dlist *g, int pos) { g->matched[pos] = "t"; }

bool get_dlist_matched (dlist *g, int pos) {
    char *m = g->matched[pos]; if( !abs(strcmp(m, "f")) ) { return false; } else { return true; }
}

dlist * getRecordsForGroup(MemoryContext ctx, char *tbl, int level, char *mask, char *pri)
{
    MemoryContext orig_ctx;

    char *string = "SELECT * FROM %s WHERE level = %d AND '%s' = pg51g.group_md5('%s',key) ORDER BY pri, key, val;";
    char *sql = (char *) palloc( strlen(string) + strlen(tbl) + strlen(mask) + strlen(pri) + 2 );
    sprintf(sql, string, tbl, level, pri, mask); int ret, proc, i;

    if ((ret = SPI_connect()) < 0) { ereport(ERROR,
                                                   ( errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                                     errmsg("[pg51g] getRecordsForGroup: connection error"),
                                                     errhint("pg51g_getRecordsForGroup could not SPI connect!")  )
                                     );
    }

    /* trying to execute sql */
    ret = SPI_exec(sql,0);
    if (ret < 0) { ereport(ERROR,
                                  ( errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                    errmsg("[pg51g] getRecordsForGroup: sql error"),
                                    errhint("Please be careful about the SQL you throw at pg51g_getRecordsForGroup!")  )
                          );
    }

    proc = SPI_processed;

    orig_ctx = MemoryContextSwitchTo(ctx);

    dlist *dl = new_dlist();
    char *mypri, *mykey;

    MemoryContextSwitchTo(orig_ctx);

    if (SPI_tuptable != NULL && proc > 0)
    {
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;
        for (i=0; i<proc; i++) {
            HeapTuple tuple = tuptable->vals[i];
            if (!(tuple == 0)) {
                char *temppri = SPI_getvalue(tuple, tupdesc, 2);
                char *tempkey = SPI_getvalue(tuple, tupdesc, 4);

                orig_ctx = MemoryContextSwitchTo(ctx);

                mypri = palloc( strlen(temppri) + 1 ); sprintf(mypri,"%s",temppri);
                mykey = palloc( strlen(tempkey) + 1 ); sprintf(mykey,"%s",tempkey);

                add_to_dlist(dl, mypri, mykey);

                MemoryContextSwitchTo(orig_ctx);

            }
        }
    }
    SPI_finish();

    return dl;
}

dlist * diff_dlists(dlist *saved, dlist *current)
{
    int refsize = get_dlist_current_pos(saved);
    int proc = get_dlist_current_pos(current);
    int i, k;

    dlist *dl = new_dlist();

    for (i=0; i<proc; i++) {
        char *mypri = get_dlist_key(current, i);
        char *mykey = get_dlist_op(current, i);

        // hasPri --> looking for INSERTions
        bool matched = false;
        for(k=0; k<refsize; k++) {
                char *refpri = get_dlist_key(saved, k);
                if ( !abs(strcmp(mypri, refpri)) ) { matched = true; set_dlist_matched(saved, k); break;  }
        }
        if (!matched) { add_to_dlist(dl, mypri, "INSERT"); }
        else { // hasA --> looking for UPDATEs
            bool upmatched = false;
            for(k=0; k<refsize; k++) {
                    char *refpri = get_dlist_key(saved, k);
                    char *refkey = get_dlist_op(saved, k);
                    if ( !abs(strcmp(mypri, refpri)) && !abs(strcmp(mykey, refkey)) ) { upmatched = true; break; }
            }
            if (!upmatched) { add_to_dlist(dl, mypri, "UPDATE"); }
        }

    }
    // getUnmatched --> looking for DELETEions
    for(k=0; k<refsize; k++) { if (!get_dlist_matched(saved, k)) { add_to_dlist(dl, get_dlist_key(saved,k), "DELETE"); } }

    return dl;
}

PG_FUNCTION_INFO_V1(diff_table);
Datum diff_table(PG_FUNCTION_ARGS)
{
    FuncCallContext *srf;
    int              call_cntr;
    int              max_calls;
    TupleDesc        tupdesc;
    AttInMetadata   *attinmeta;
    dlist           *dl, *pris, *saved, *current, *diff, *tmp;
    MemoryContext    oldcontext;

    if( SRF_IS_FIRSTCALL() )
    {
        /* create a function context for cross-call persistence */
        srf = SRF_FIRSTCALL_INIT();

        oldcontext = MemoryContextSwitchTo(srf->multi_call_memory_ctx);

        VarChar *sql = PG_GETARG_VARCHAR_P(0); unsigned int size = VARSIZE(sql) - VARHDRSZ;
        char *buf = (char *) palloc(size+1);
        strncpy(buf,VARDATA(sql),size); buf[size] = '\0'; // converting varchar into native string

        MemoryContextSwitchTo(oldcontext);

        char *tblname = get_table(buf);
        char *schname = get_schema(buf);

        oldcontext = MemoryContextSwitchTo(srf->multi_call_memory_ctx);

        /* Build a tuple descriptor for our result type */
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                      errmsg("function returning record called in context "
                             "that cannot accept type record")));

        attinmeta = TupleDescGetAttInMetadata(tupdesc);
        srf->attinmeta = attinmeta;

        // Storing state for subsequent invocations 

        dl = new_dlist();
        srf->user_fctx = (dlist *) dl;

        MemoryContextSwitchTo(oldcontext);

        char *saved_tbl = get_snap_target(schname, tblname);
        char *current_tbl = get_sig_target(schname, tblname);

        int depth = read_depth(buf); int level, mysize, i, k; char *mask; bool ops = true;

        /* until we decide what to do when depth < 0 */
        if (depth < 0)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                      errmsg("Attempting to diff a table with depth < 0"
                             " -- stopping right here")));

        pris = new_dlist();
        add_to_dlist(pris, "00000000000000000000000000000000", "00000000000000000000000000000000");

        for(level=depth+1; level > 0; level--) {

            diff = new_dlist();
            mask = get_mask(depth, level);
            mysize = get_dlist_current_pos(pris);

            for(i=0; i<mysize; i++) {
                char *pri = get_dlist_key(pris, i);
                // saved = null;
                saved = getRecordsForGroup(CurrentMemoryContext, saved_tbl, level-1, mask, pri);
                current = getRecordsForGroup(CurrentMemoryContext, current_tbl, level-1, mask, pri);
                tmp = diff_dlists(saved, current);
                append_dlist(diff, tmp);
                del_dlist(saved); del_dlist(current); del_dlist(tmp);
            }
            
            del_dlist(pris); pris = clone_dlist(diff); del_dlist(diff);
        }

        // populating the result list with the items in *pris

        mysize = get_dlist_current_pos(pris);
        for(i=0; i<mysize; i++) { add_to_dlist( dl, get_dlist_key(pris, i), get_dlist_op(pris, i) ); }

        oldcontext = MemoryContextSwitchTo(srf->multi_call_memory_ctx);

        srf->max_calls = get_dlist_current_pos(dl);
        srf->call_cntr = 0;

        MemoryContextSwitchTo(oldcontext);

        pfree(buf); pfree(tblname); pfree(schname); pfree(saved_tbl); pfree(mask);
   }

   /* stuff done on every call of the function */
   srf = SRF_PERCALL_SETUP();

   call_cntr = srf->call_cntr;
   max_calls = srf->max_calls;
   attinmeta = srf->attinmeta;
   dl = (dlist *)srf->user_fctx;

   if (call_cntr < max_calls)     /* do when there is more left to send */
   {
       char       **values;
       HeapTuple    tuple;
       Datum        result;
       /*
        * Prepare a values array for building the returned tuple.
        * This should be an array of C strings which will
        * be processed later by the type input functions.
        */
       values = (char **) palloc(2 * sizeof(char *));
       values[0] = (char *) palloc(128 * sizeof(char));
       values[1] = (char *) palloc(128 * sizeof(char));

       snprintf(values[0], 128, "%s", get_dlist_key(dl, call_cntr) );
       snprintf(values[1], 128, "%s", get_dlist_op(dl, call_cntr) );

       /* build a tuple */
       tuple = BuildTupleFromCStrings(attinmeta, values);

       /* make the tuple into a datum */
       result = (Datum) HeapTupleGetDatum(tuple);

       /* clean up (this is not really necessary) */
       pfree(values[0]);
       pfree(values[1]);
       pfree(values);

       SRF_RETURN_NEXT(srf, result);
   }
   else    /* do when there is no more left */
   {
       del_dlist( dl );
       SRF_RETURN_DONE(srf);
   }
}

static char * sync_saved (char * schname, char * tblname)
{
    dlist           *pris, *saved, *current, *diff, *tmp;

    char *saved_tbl = get_snap_target(schname, tblname);
    char *current_tbl = get_sig_target(schname, tblname);
        int t_size = strlen(schname) + strlen(tblname) + 2;
        char *buf = (char *) palloc(t_size * sizeof(char));
        sprintf(buf, "%s.%s", schname, tblname);

    int depth = read_depth(buf); int level, mysize, tempsize, i, k, j; char *mask; bool ops = true;

    /* until we decide what to do when depth < 0 */
    if (depth < 0)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("Attempting to diff a table with depth < 0"
                         " -- stopping right here")));

    pris = new_dlist();
    add_to_dlist(pris, "00000000000000000000000000000000", "00000000000000000000000000000000");

    for(level=depth+1; level > 0; level--) {

        diff = new_dlist();
        mask = get_mask(depth, level);
        mysize = get_dlist_current_pos(pris);

        for(i=0; i<mysize; i++) {
            char *pri = get_dlist_key(pris, i);
            // saved = null;
            saved = getRecordsForGroup(CurrentMemoryContext, saved_tbl, level-1, mask, pri);
            current = getRecordsForGroup(CurrentMemoryContext, current_tbl, level-1, mask, pri);
            tmp = diff_dlists(saved, current);
            append_dlist(diff, tmp);
            del_dlist(saved); del_dlist(current); del_dlist(tmp);
        }

        del_dlist(pris); pris = clone_dlist(diff); del_dlist(diff);
                tempsize = get_dlist_current_pos(pris);
        for(j=0; j<tempsize; j++) { sync_saved_row( current_tbl, saved_tbl, get_dlist_op( pris, j ), get_dlist_key( pris, j ), level-1 ); }
    }

        del_dlist(pris); pfree(buf); pfree(current_tbl); pfree(saved_tbl); pfree(mask);

        return "sync_saved";
}

static char * sync_saved_and_do (char * schname, char * tblname, int (*proc)(char *, char *, char *, char *, int))
{
    dlist           *pris, *saved, *current, *diff, *tmp;

    char *saved_tbl = get_snap_target(schname, tblname);
    char *current_tbl = get_sig_target(schname, tblname);
        int t_size = strlen(schname) + strlen(tblname) + 2;
        char *buf = (char *) palloc(t_size * sizeof(char));
        sprintf(buf, "%s.%s", schname, tblname);

    int depth = read_depth(buf); int level, mysize, tempsize, i, k, j; char *mask; bool ops = true;

    /* until we decide what to do when depth < 0 */
    if (depth < 0)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("Attempting to diff a table with depth < 0"
                         " -- stopping right here")));

    pris = new_dlist();
    add_to_dlist(pris, "00000000000000000000000000000000", "00000000000000000000000000000000");

    for(level=depth+1; level > 0; level--) {

        diff = new_dlist();
        mask = get_mask(depth, level);
        mysize = get_dlist_current_pos(pris);

        for(i=0; i<mysize; i++) {
            char *pri = get_dlist_key(pris, i);
            // saved = null;
            saved = getRecordsForGroup(CurrentMemoryContext, saved_tbl, level-1, mask, pri);
            current = getRecordsForGroup(CurrentMemoryContext, current_tbl, level-1, mask, pri);
            tmp = diff_dlists(saved, current);
            append_dlist(diff, tmp);
            del_dlist(saved); del_dlist(current); del_dlist(tmp);
        }

        del_dlist(pris); pris = clone_dlist(diff); del_dlist(diff);
                tempsize = get_dlist_current_pos(pris);
        for(j=0; j<tempsize; j++) { sync_saved_row( current_tbl, saved_tbl, get_dlist_op( pris, j ), get_dlist_key( pris, j ), level-1 ); }
    }

        tempsize = get_dlist_current_pos(pris);
        for(j=0; j<tempsize; j++) { int rc = (*(proc))( current_tbl, saved_tbl, get_dlist_op( pris, j ), get_dlist_key( pris, j ), level ); }

        del_dlist(pris); pfree(buf); pfree(current_tbl); pfree(saved_tbl); pfree(mask);

        return "sync_saved_and_do";
}

PG_FUNCTION_INFO_V1(diff_push);
Datum diff_push(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); } 

    VarChar *sql = PG_GETARG_VARCHAR_P(0); unsigned int size = VARSIZE(sql) - VARHDRSZ;
    char *buf = (char *) palloc(size+1); int ret;
    strncpy(buf,VARDATA(sql),size); buf[size] = '\0'; // converting varchar into native string

    char *schname = get_schema(buf);
    char *tblname = get_table(buf);

    // if the table has been already added to the metadata table, return null
    if( !table_in_metadata(schname, tblname)  ) {
        elog(ERROR, "[pg51g] diff_push('%s'): does not exist in pg51g.metadata", buf);
        PG_RETURN_NULL();
    }

    char *target = get_snap_target(schname, tblname); char *res;
    if ( table_exists( get_schema(target), get_table(target) ) ) { res = sync_saved_and_do(schname, tblname, &test_push); }
        else { PG_RETURN_NULL(); }

    int res_s = strlen(res);
    int32 new_size = res_s + VARHDRSZ;
    VarChar *new_varchar = (VarChar *) palloc(new_size);
    SET_VARSIZE(new_varchar, new_size);
    memcpy(VARDATA(new_varchar), res, res_s); pfree(buf);
    PG_RETURN_VARCHAR_P(new_varchar);
}

PG_FUNCTION_INFO_V1(do_table);
Datum do_table(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); } 

    VarChar *sql = PG_GETARG_VARCHAR_P(0); unsigned int size = VARSIZE(sql) - VARHDRSZ;
    char *buf = (char *) palloc(size+1); int ret;
    strncpy(buf,VARDATA(sql),size); buf[size] = '\0'; // converting varchar into native string

    char *schname = get_schema(buf);
    char *tblname = get_table(buf);

   // if the relation cannot be found in the metadata table...
    if( !table_in_metadata(schname, tblname)  ) {
        // it could be a temp relation that doesn't exist anymore
        if ( table_in_metadata("pg_temp_1", tblname) ) { pfree(schname); schname = palloc(10); strcpy(schname, "pg_temp_1"); }
        else {
            // or we'll just complain and return
            elog(ERROR, "[pg51g] do_table('%s'): does not exist in pg51g.metadata", buf);
            PG_RETURN_NULL();
        }
    }

    char *key = read_pkey_sql(schname, tblname);
    char *val = read_val_sql(schname, tblname);
    int depth = read_depth(buf);
    char *sigtbl = get_sig_target(schname, tblname);

    bool perm = read_persists(schname, tblname); char *res; res = sigtbl;

    if( read_is_view(schname, tblname) ) {
        // recreating the TEMP view if it's not there
        if( !perm ) {
            drop_if_exists( schname, tblname );
            char *def = read_table_def(schname, tblname);
            return_val(def);
        }
        // replacing an existing sigtbl with a fresh one
        drop_if_exists( get_schema(sigtbl), get_table(sigtbl) ); // getting rid of the table, if it's already there
        process_table (schname, tblname, key, val, depth, sigtbl, true);
    }
    else {
        if (!perm) { 
            drop_if_exists( get_schema(sigtbl), get_table(sigtbl) ); // getting rid of the table, if it's already there
            process_table (schname, tblname, key, val, depth, sigtbl, true);
        }
    }

    int res_s = strlen(res);
    int32 new_size = res_s + VARHDRSZ;
    VarChar *new_varchar = (VarChar *) palloc(new_size);
    SET_VARSIZE(new_varchar, new_size);
    memcpy(VARDATA(new_varchar), res, res_s); pfree(buf);
    PG_RETURN_VARCHAR_P(new_varchar);
}

PG_FUNCTION_INFO_V1(snap_table);
Datum snap_table(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); } 

    VarChar *sql = PG_GETARG_VARCHAR_P(0); unsigned int size = VARSIZE(sql) - VARHDRSZ;
    char *buf = (char *) palloc(size+1); int ret;
    strncpy(buf,VARDATA(sql),size); buf[size] = '\0'; // converting varchar into native string

    char *schname = get_schema(buf);
    char *tblname = get_table(buf);

    // if the table has been already added to the metadata table, return null
    if( !table_in_metadata(schname, tblname)  ) {
        elog(ERROR, "[pg51g] snap_table('%s'): does not exist in pg51g.metadata", buf);
        PG_RETURN_NULL();
    }

    char *target = get_snap_target(schname, tblname); char *res;
    if ( table_exists( get_schema(target), get_table(target) ) ) { res = sync_saved(schname, tblname); }
        else { res = save_table(schname, tblname); }

    int res_s = strlen(res);
    int32 new_size = res_s + VARHDRSZ;
    VarChar *new_varchar = (VarChar *) palloc(new_size);
    SET_VARSIZE(new_varchar, new_size);
    memcpy(VARDATA(new_varchar), res, res_s); pfree(buf);
    PG_RETURN_VARCHAR_P(new_varchar);
}

PG_FUNCTION_INFO_V1(monitor_table);
Datum monitor_table(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }

    VarChar *name = PG_GETARG_VARCHAR_P(0); unsigned int size = VARSIZE(name) - VARHDRSZ;
    char *buf = (char *) palloc(size+1); strncpy(buf,VARDATA(name),size); buf[size] = '\0';

    int ret; char *res;

    char *schname = get_schema(buf);
    char *tblname = get_table(buf);

    int trig_s = strlen(schname) + strlen(tblname) + 17;
    char *trig = (char *) palloc (trig_s + 1);
    sprintf(trig, "pg51g_%s_%s_on_change", schname, tblname);

    char *string1 = "SELECT COUNT(*) FROM pg_trigger WHERE tgname = '%s';";
    int sql_s = strlen(string1) + strlen(trig) - 2;
    char *sql = (char *) palloc (sql_s + 1);
    sprintf(sql, string1, trig);
    char *returned = return_val(sql); int rows = 0; sscanf(returned,"%d",&rows);

    if ( !(rows > 0) ) {
         res = install_trigger(schname, tblname);
    }
    else {
         int ret_s = strlen(trig) + 8;
         res = palloc(ret_s + 1);
         strcpy(res, trig); strcat(res, " exists!");
         PG_RETURN_NULL();
    }

    int res_s = strlen(res);
    int32 new_size = res_s + VARHDRSZ;
    VarChar *new_varchar = (VarChar *) palloc(new_size);
    SET_VARSIZE(new_varchar, new_size);
    memcpy(VARDATA(new_varchar), res, res_s); pfree(buf);
    PG_RETURN_VARCHAR_P(new_varchar);
}

PG_FUNCTION_INFO_V1(unmonitor_table);
Datum unmonitor_table(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }

    VarChar *name = PG_GETARG_VARCHAR_P(0); unsigned int size = VARSIZE(name) - VARHDRSZ;
    char *buf = (char *) palloc(size+1); strncpy(buf,VARDATA(name),size); buf[size] = '\0';

    int ret; char *res;

    char *schname = get_schema(buf);
    char *tblname = get_table(buf);

    int trig_s = strlen(schname) + strlen(tblname) + 17;
    char *trig = (char *) palloc (trig_s + 1);
    sprintf(trig, "pg51g_%s_%s_on_change", schname, tblname);

    char *string1 = "SELECT COUNT(*) FROM pg_trigger WHERE tgname = '%s';";
    int sql_s = strlen(string1) + strlen(trig) - 2;
    char *sql = (char *) palloc (sql_s + 1);
    sprintf(sql, string1, trig);
    char *returned = return_val(sql); int rows = 0; sscanf(returned,"%d",&rows);
    char *string2;

    if ( rows > 0 ) {
         res = remove_trigger(schname, tblname);
    }
    else {
         int ret_s = strlen(trig) + 15;
         res = palloc(ret_s + 1);
         strcpy(res, trig); strcat(res, " does not exist!");
         PG_RETURN_NULL();
    }

    int res_s = strlen(res);
    int32 new_size = res_s + VARHDRSZ;
    VarChar *new_varchar = (VarChar *) palloc(new_size);
    SET_VARSIZE(new_varchar, new_size);
    memcpy(VARDATA(new_varchar), res, res_s); pfree(buf);
    PG_RETURN_VARCHAR_P(new_varchar);
}

PG_FUNCTION_INFO_V1(freeze_table);
Datum freeze_table(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }

    VarChar *name = PG_GETARG_VARCHAR_P(0); unsigned int size = VARSIZE(name) - VARHDRSZ;
    char *buf = (char *) palloc(size+1); strncpy(buf,VARDATA(name),size); buf[size] = '\0';

    int ret; char *res;

    char *schname = get_schema(buf);
    char *tblname = get_table(buf);

    int trig_s = strlen(schname) + strlen(tblname) + 17;
    char *trig = (char *) palloc (trig_s + 1);
    sprintf(trig, "pg51g_%s_%s_freeze", schname, tblname);

    char *string1 = "SELECT COUNT(*) FROM pg_trigger WHERE tgname = '%s';";
    int sql_s = strlen(string1) + strlen(trig) - 2;
    char *sql = (char *) palloc (sql_s + 1);
    sprintf(sql, string1, trig);
    char *returned = return_val(sql); int rows = 0; sscanf(returned,"%d",&rows);

    if ( !(rows > 0) ) {
         res = install_freeze(schname, tblname);
    }
    else {
         int ret_s = strlen(trig) + 8;
         res = palloc(ret_s + 1);
         strcpy(res, trig); strcat(res, " exists!");
         PG_RETURN_NULL();
    }

    int res_s = strlen(res);
    int32 new_size = res_s + VARHDRSZ;
    VarChar *new_varchar = (VarChar *) palloc(new_size);
    SET_VARSIZE(new_varchar, new_size);
    memcpy(VARDATA(new_varchar), res, res_s); pfree(buf);
    PG_RETURN_VARCHAR_P(new_varchar);
}

PG_FUNCTION_INFO_V1(unfreeze_table);
Datum unfreeze_table(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }

    VarChar *name = PG_GETARG_VARCHAR_P(0); unsigned int size = VARSIZE(name) - VARHDRSZ;
    char *buf = (char *) palloc(size+1); strncpy(buf,VARDATA(name),size); buf[size] = '\0';

    int ret; char *res;

    char *schname = get_schema(buf);
    char *tblname = get_table(buf);

    int trig_s = strlen(schname) + strlen(tblname) + 17;
    char *trig = (char *) palloc (trig_s + 1);
    sprintf(trig, "pg51g_%s_%s_freeze", schname, tblname);

    char *string1 = "SELECT COUNT(*) FROM pg_trigger WHERE tgname = '%s';";
    int sql_s = strlen(string1) + strlen(trig) - 2;
    char *sql = (char *) palloc (sql_s + 1);
    sprintf(sql, string1, trig);
    char *returned = return_val(sql); int rows = 0; sscanf(returned,"%d",&rows);
    char *string2;

    if ( rows > 0 ) {
         res = remove_freeze(schname, tblname);
    }
    else {
         int ret_s = strlen(trig) + 15;
         res = palloc(ret_s + 1);
         strcpy(res, trig); strcat(res, " does not exist!");
         PG_RETURN_NULL();
    }

    int res_s = strlen(res);
    int32 new_size = res_s + VARHDRSZ;
    VarChar *new_varchar = (VarChar *) palloc(new_size);
    SET_VARSIZE(new_varchar, new_size);
    memcpy(VARDATA(new_varchar), res, res_s); pfree(buf);
    PG_RETURN_VARCHAR_P(new_varchar);
}

PG_FUNCTION_INFO_V1(define_pkey);
Datum define_pkey(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); } if (PG_ARGISNULL(1)) { PG_RETURN_NULL(); } 

    VarChar *name = PG_GETARG_VARCHAR_P(0); unsigned int size = VARSIZE(name) - VARHDRSZ;
    char *buf = (char *) palloc(size+1); strncpy(buf,VARDATA(name),size); buf[size] = '\0';

    VarChar *pkey = PG_GETARG_VARCHAR_P(1); unsigned int pkey_s = VARSIZE(pkey) - VARHDRSZ;
    char *arg = (char *) palloc(pkey_s+1); strncpy(arg,VARDATA(pkey),pkey_s); arg[pkey_s] = '\0';

    char *earg = escape_quotes(arg); // perhaps we want some delimiters in our concatenation

    int ret; char *res;

    char *schname = get_schema(buf);
    char *tblname = get_table(buf);

    char *string1 = "SELECT COUNT(*) FROM pg51g.alternatives WHERE t_schema = '%s' AND t_table = '%s';";
    int sql_s = strlen(string1) + strlen(schname) + strlen(tblname);
    char *sql = (char *) palloc (sql_s + 1);
    sprintf(sql, string1, schname, tblname);
    char *returned = return_val(sql); int rows = 0; sscanf(returned,"%d",&rows);
    char *string2;

    if ( rows > 0 ) {
         string2 = "UPDATE pg51g.alternatives SET t_key_sql = '%s' WHERE t_schema = '%s' AND t_table = '%s';";
         int update_s = strlen(string2) + strlen(earg) + strlen(schname) + strlen(tblname) - 6;
         char *update = (char *) palloc (update_s + 1);
         sprintf(update, string2, earg, schname, tblname);
         char *ret = return_val(update); res = "UPDATED";
    }
    else {
         string2 = "INSERT INTO pg51g.alternatives VALUES ('%s', '%s', '%s', NULL);";
         int insert_s = strlen(string2) + strlen(earg) + strlen(schname) + strlen(tblname) - 6;
         char *insert = (char *) palloc (insert_s + 1);
         sprintf(insert, string2, schname, tblname, earg);
         char *ret = return_val(insert); res = "INSERTED";
    }

    int res_s = strlen(res);
    int32 new_size = res_s + VARHDRSZ;
    VarChar *new_varchar = (VarChar *) palloc(new_size);
    SET_VARSIZE(new_varchar, new_size);
    memcpy(VARDATA(new_varchar), res, res_s); pfree(buf);
    PG_RETURN_VARCHAR_P(new_varchar);
}

PG_FUNCTION_INFO_V1(define_val);
Datum define_val(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); } if (PG_ARGISNULL(1)) { PG_RETURN_NULL(); } 

    VarChar *name = PG_GETARG_VARCHAR_P(0); unsigned int size = VARSIZE(name) - VARHDRSZ;
    char *buf = (char *) palloc(size+1); strncpy(buf,VARDATA(name),size); buf[size] = '\0';

    VarChar *val = PG_GETARG_VARCHAR_P(1); unsigned int val_s = VARSIZE(val) - VARHDRSZ;
    char *arg = (char *) palloc(val_s+1); strncpy(arg,VARDATA(val),val_s); arg[val_s] = '\0';

    int ret; char *res;

    char *schname = get_schema(buf);
    char *tblname = get_table(buf);

    char *string1 = "SELECT COUNT(*) FROM pg51g.alternatives WHERE t_schema = '%s' AND t_table = '%s';";
    int sql_s = strlen(string1) + strlen(schname) + strlen(tblname);
    char *sql = (char *) palloc (sql_s + 1);
    sprintf(sql, string1, schname, tblname);
    char *returned = return_val(sql); int rows = 0; sscanf(returned,"%d",&rows);
    char *string2;

    if ( rows > 0 ) {
         string2 = "UPDATE pg51g.alternatives SET t_val_sql = '%s' WHERE t_schema = '%s' AND t_table = '%s';";
         int update_s = strlen(string2) + strlen(arg) + strlen(schname) + strlen(tblname) - 6;
         char *update = (char *) palloc (update_s + 1);
         sprintf(update, string2, arg, schname, tblname);
         char *ret = return_val(update); res = "UPDATED";
    }
    else {
         string2 = "INSERT INTO pg51g.alternatives VALUES ('%s', '%s', NULL, '%s');";
         int insert_s = strlen(string2) + strlen(arg) + strlen(schname) + strlen(tblname) - 6;
         char *insert = (char *) palloc (insert_s + 1);
         sprintf(insert, string2, schname, tblname, arg);
         char *ret = return_val(insert); res = "INSERTED";
    }

    int res_s = strlen(res);
    int32 new_size = res_s + VARHDRSZ;
    VarChar *new_varchar = (VarChar *) palloc(new_size);
    SET_VARSIZE(new_varchar, new_size);
    memcpy(VARDATA(new_varchar), res, res_s); pfree(buf);
    PG_RETURN_VARCHAR_P(new_varchar);
}

PG_FUNCTION_INFO_V1(undef_pkey);
Datum undef_pkey(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }

    VarChar *name = PG_GETARG_VARCHAR_P(0); unsigned int size = VARSIZE(name) - VARHDRSZ;
    char *buf = (char *) palloc(size+1); strncpy(buf,VARDATA(name),size); buf[size] = '\0';

    int ret; char *res;

    char *schname = get_schema(buf);
    char *tblname = get_table(buf);

    char *string1 = "SELECT t_val_sql FROM pg51g.alternatives WHERE t_schema = '%s' AND t_table = '%s';";
    int sql_s = strlen(string1) + strlen(schname) + strlen(tblname);
    char *sql = (char *) palloc (sql_s + 1);
    sprintf(sql, string1, schname, tblname);
    char *returned = return_val(sql); char *string2;

    if ( abs(strcmp(returned,"")) ) {
         string2 = "UPDATE pg51g.alternatives SET t_key_sql = NULL WHERE t_schema = '%s' AND t_table = '%s';";
         int update_s = strlen(string2) + strlen(schname) + strlen(tblname) - 4;
         char *update = (char *) palloc (update_s + 1);
         sprintf(update, string2, schname, tblname);
         char *ret = return_val(update); res = "UPDATED";
    }
    else {
         string2 = "DELETE FROM pg51g.alternatives WHERE t_schema = '%s' AND t_table = '%s';";
         int delete_s = strlen(string2) + strlen(schname) + strlen(tblname) - 4;
         char *delete = (char *) palloc (delete_s + 1);
         sprintf(delete, string2, schname, tblname);
         char *ret = return_val(delete); res = "DELETED";
    }

    int res_s = strlen(res);
    int32 new_size = res_s + VARHDRSZ;
    VarChar *new_varchar = (VarChar *) palloc(new_size);
    SET_VARSIZE(new_varchar, new_size);
    memcpy(VARDATA(new_varchar), res, res_s); pfree(buf);
    PG_RETURN_VARCHAR_P(new_varchar);
}

PG_FUNCTION_INFO_V1(undef_val);
Datum undef_val(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) { PG_RETURN_NULL(); }

    VarChar *name = PG_GETARG_VARCHAR_P(0); unsigned int size = VARSIZE(name) - VARHDRSZ;
    char *buf = (char *) palloc(size+1); strncpy(buf,VARDATA(name),size); buf[size] = '\0';

    int ret; char *res;

    char *schname = get_schema(buf);
    char *tblname = get_table(buf);

    char *string1 = "SELECT t_key_sql FROM pg51g.alternatives WHERE t_schema = '%s' AND t_table = '%s';";
    int sql_s = strlen(string1) + strlen(schname) + strlen(tblname);
    char *sql = (char *) palloc (sql_s + 1);
    sprintf(sql, string1, schname, tblname);
    char *returned = return_val(sql); char *string2;

    if ( abs(strcmp(returned,"")) ) {
         string2 = "UPDATE pg51g.alternatives SET t_val_sql = NULL WHERE t_schema = '%s' AND t_table = '%s';";
         int update_s = strlen(string2) + strlen(schname) + strlen(tblname) - 4;
         char *update = (char *) palloc (update_s + 1);
         sprintf(update, string2, schname, tblname);
         char *ret = return_val(update); res = "UPDATED";
    }
    else {
         string2 = "DELETE FROM pg51g.alternatives WHERE t_schema = '%s' AND t_table = '%s';";
         int delete_s = strlen(string2) + strlen(schname) + strlen(tblname) - 4;
         char *delete = (char *) palloc (delete_s + 1);
         sprintf(delete, string2, schname, tblname);
         char *ret = return_val(delete); res = "DELETED";
    }

    int res_s = strlen(res);
    int32 new_size = res_s + VARHDRSZ;
    VarChar *new_varchar = (VarChar *) palloc(new_size);
    SET_VARSIZE(new_varchar, new_size);
    memcpy(VARDATA(new_varchar), res, res_s); pfree(buf);
    PG_RETURN_VARCHAR_P(new_varchar);
}

