#!/usr/bin/perl

# Test suite for pg51g - data diff tool for Postgresql databases
# author: Michael Nacos (m.nacos AT gmail.com)
# license: same as pg51g -- please see COPYRIGHT

use Test::Unit::Procedural;
use DBI; use DBD::Pg;

# Interesting way of running your tests: (if you see any output, some runs have definitely failed :-(
# for i in $(seq 10); do ./test-pg51g.pl 2>>/dev/null; done | grep 'Run'

$dbhost = 'localhost';
$dbport = '5432';
$dbname = 'pg51gTests';
$dbuser = 'update51g';
$dbpass = 'update51g';
$pgpass = '';
$psql = "psql -U postgres $dbname";

# Functions that help with random generation of data for each type

$types = {
    'int' => 'int,2147483648',
    'smallint' => 'int,32768',
    'bigint' => 'int,9223372036854775808',
    'real' => 'float,24',
    'double' => 'float,53',
    'numeric' => 'float,64',
    'char16' => 'chars,16',
    'char32' => 'chars,32',
    'char256' => 'chars,256',
    'varchar16' => 'chars,16',
    'varchar' => 'chars,128',
    'text' => 'chars,1024',
    'bytea' => 'bytea,256',
    'timestamp' => 'timestamp',
    'date' => 'timestamp,date',
    'time' => 'timestamp,time',
    'interval' => 'interval',
    'mood' => 'enum,mood',
    'point' => 'point,100',
    'bit32' => 'bit,32',
    'varbit64' => 'bit,64',
    'mycomp' => 'mycomp',
    'integer_array' => 'intarray',
    'boolean' => 'bool',
    'nullable_text' => 'textornull,0',
};

# ip/mac addresses

sub int {
    my $arg = shift;
    return sprintf("%d",rand($arg));
}

sub float {
    my $arg = shift; my $i;
    my $lp = abs(&int($arg))+1;
    my $left = 10**$lp; my $right = 10**($arg-$lp);
    return sprintf("%u",&int($left)).".".sprintf("%u",&int($right));
}

sub bool {
    my $val = rand();
    if ($val < 0.50) { return "false"; } else { return "true"; }
}

sub chars {
    my $arg = shift; my $result = ""; my $c;
    for($i=0; $i<$arg; $i++) { $c = int(rand(126)); if ($c<32) { $c += 32; } $result .= chr($c); }
    $result =~ s/'/''/g;
    $result =~ s/\\/'\|\|E'\\\\'\|\|'/g;
    return "'".$result."'";
}

sub bytea {
    my $arg = shift; my $result = ""; my $c;
    for($i=0; $i<$arg; $i++) { $c = int(rand(256)); $result .= "E'".'\\\\'.sprintf("%03o",$c)."'||"; }
    if($i) { chop $result; chop $result; $result = "(".$result.")::bytea"; }
    return $result;
}

sub textornull {
    my $arg = shift; my $val = rand();
    if ($val < 0.50) { return "NULL"; } else { return &chars($arg); }
}

sub timestamp {
    my $arg = shift; my $result = "";
    my $y = sprintf("%04d",int(rand(21)+2000));
    my $m = sprintf("%02d",int(rand(12)+1));
    my $d = sprintf("%02d",int(rand(27)+1));
    my $H = sprintf("%02d",int(rand(24)));
    my $M = sprintf("%02d",int(rand(60)));
    my $S = sprintf("%02d",int(rand(60)));
    if ($arg eq "date") { $result = $y.'-'.$m.'-'.$d; }
    elsif ($arg eq "time") { $result = $H.':'.$M.':'.$S; }
    else { $result = $y.'-'.$m.'-'.$d.' '.$H.':'.$M.':'.$S; }
    return "'".$result."'";
}

sub interval {
    my $direction = ""; $direction = " ago" if &bool eq "false";
    my $time = &timestamp("time"); $time =~ s/'//g;
    return "'@ ".int(rand(100))." ".$time.$direction."'";
}

sub enum {
    my $arg = shift; my $c;
    if ($arg eq "mood") {
         $c = int(rand(3)); if ($c == 2) { return "'happy'"; }
         elsif ($c == 1) { return "'ok'"; } else { return "'sad'"; }
    }
}

sub point {
    my $arg = shift;
    return "'(".int(rand($arg)).",".int(rand($arg)).")'";
}

sub bit {
    my $arg = shift;
    return "B'".sprintf("%0".$arg."b",int(rand(2**$arg)))."'";
}

sub mycomp {
    return "ROW(".&chars(16).", ".int(rand(256)).")";
}

sub intarray {
    my $c = int(rand(10))+1; my $i;
    my $result = "ARRAY[";
    for($i=0; $i<$c; $i++) { $result .= "[".int(rand(64)).",".int(rand(64))."],"; }
    chop $result; $result .= "]";
    return $result;
}

sub generate {
    my $s = shift; my ($fun,$arg) = split ',', $s; my $val = undef;
    $val = &{$fun}($arg) if length($fun) > 0 and defined(&{$fun});
    return $val;
}

sub populate {
    my $hash = shift; my $target = shift;
    my $rows = shift; my $i; my $insert1; my $insert2; my $tinsert; my $retval;
    for($i=0; $i<$rows; $i++) {
        $insert1 = "INSERT INTO ".$target." ("; $insert2 = ") VALUES (";
        foreach $k (keys %{$hash}) { $insert1 .= $k.", "; $insert2 .= &generate($$types{$hash->{$k}}).", "; }
        chop $insert1; chop $insert1; chop $insert2; chop $insert2;
        $tinsert = $insert1.$insert2.");";
        $retval = $user->do($tinsert);
        unless($retval) { print "ERROR while populating $target... Perhaps we're violating a UNIQUE constraint! (HINT: Run more tests)\n"; }
    }
}

sub not_in {
    my $val = shift; my $ref = shift;
    my $size = scalar(@$ref); my $ret = 1; my $i;
    for ($i; $i<$size; $i++) { if ($$ref[$i] == $val) { $ret = 0; last; } }
    return $ret;
}

sub update {
    my $hash = shift; my $target = shift; my $pri = shift;
    my $keys = shift; my $num = shift; # num is how many fields we shall try to change
    my $i; my $insert1; my $insert2; my $tinsert;
    my $rows = scalar(@$keys); my @fields = keys %{$hash};
    my @chosen; my $pick; my $field; my $val;
    for($i=0; $i<$rows; $i++) {
        @chosen = ();
        while (scalar(@chosen) < $num) {
            $pick = int(rand(scalar(@fields)));
            push @chosen, $pick if &not_in($pick, \@chosen);
        }
        foreach $pick (@chosen) {
            $field = @fields[$pick]; if ($pri eq $field) { $i--; next; }
            $val = &generate($$types{$hash->{$field}});
            $update = "UPDATE ".$target." SET ".$field." = ".$val." WHERE ".$pri." = '".$$keys[$i]."';";
            $user->do($update);
        }
    }
}

# Loading all necessary data into the database (schema creation, data import etc.)

$admin = DBI->connect("dbi:Pg:dbname=postgres;host=$dbhost;port=$dbport", "postgres", $pgpass, {AutoCommit => 1});
$admin->do("DROP DATABASE IF EXISTS \"".$dbname."\";");
$admin->do("DROP USER IF EXISTS \"".$dbuser."\";");
$admin->do("CREATE USER \"".$dbuser."\" PASSWORD '".$dbpass."';");
$admin->do("CREATE DATABASE \"".$dbname."\" OWNER \"".$dbuser."\";");
$admin->disconnect();

$vanilla_table = {
    'mysmallint' => 'smallint',
    'mybigint' => 'bigint',
    'myreal' => 'real',
    'mydouble' => 'double',
    'mynumeric' => 'numeric',
    'myvarchar' => 'varchar',
    'myvarchar16' => 'varchar16',
    'mychar' => 'char256' ,
    'mytext' => 'text' ,
    'mybytea' => 'bytea' ,
    'mytimestamp' => 'timestamp' ,
    'myinterval' => 'interval' ,
    'mydate' => 'date' ,
    'mytime' => 'time' ,
    'myboolean' => 'boolean' ,
    'myenum' => 'mood' ,
    'mypoint' => 'point' ,
    'mybit' => 'bit32' ,
    'myvarbit' => 'varbit64' ,
    'myarray' => 'integer_array' ,
    'mycomposite' => 'mycomp' ,
    'mynull' => 'nullable_text'
};

$choc_table = {
    'mytag' => 'char32',
    'mysmallint' => 'smallint',
    'mybigint' => 'bigint',
    'myreal' => 'real',
    'mydouble' => 'double',
    'mynumeric' => 'numeric',
    'myvarchar' => 'varchar',
    'myvarchar16' => 'varchar16',
    'mychar' => 'char256' ,
    'mytext' => 'text' ,
    'mybytea' => 'bytea' ,
    'mytimestamp' => 'timestamp' ,
    'myinterval' => 'interval' ,
    'mydate' => 'date' ,
    'mytime' => 'time' ,
    'myboolean' => 'boolean' ,
    'myenum' => 'mood' ,
    'mypoint' => 'point' ,
    'mybit' => 'bit32' ,
    'myvarbit' => 'varbit64' ,
    'myarray' => 'integer_array' ,
    'mycomposite' => 'mycomp' ,
    'mynull' => 'nullable_text'
};

$strawberry_table = {
    'mytag1' => 'char16',
    'mytag2' => 'char16',
    'mytag3' => 'char16',
    'mysmallint' => 'smallint',
    'mybigint' => 'bigint',
    'myreal' => 'real',
    'mydouble' => 'double',
    'mynumeric' => 'numeric',
    'myvarchar' => 'varchar',
    'myvarchar16' => 'varchar16',
    'mychar' => 'char256' ,
    'mytext' => 'text' ,
    'mybytea' => 'bytea' ,
    'mytimestamp' => 'timestamp' ,
    'myinterval' => 'interval' ,
    'mydate' => 'date' ,
    'mytime' => 'time' ,
    'myboolean' => 'boolean' ,
    'myenum' => 'mood' ,
    'mypoint' => 'point' ,
    'mybit' => 'bit32' ,
    'myvarbit' => 'varbit64' ,
    'myarray' => 'integer_array' ,
    'mycomposite' => 'mycomp' ,
    'mynull' => 'nullable_text'
};

$fudge_table = {
    'mysmallint' => 'smallint',
    'mybigint' => 'bigint',
    'myreal' => 'real',
    'mydouble' => 'double',
    'mynumeric' => 'numeric',
    'myvarchar' => 'varchar',
    'myvarchar16' => 'varchar16',
    'mychar' => 'char256' ,
    'mytext' => 'text' ,
    'mybytea' => 'bytea' ,
    'mytimestamp' => 'timestamp' ,
    'myinterval' => 'interval' ,
    'mydate' => 'date' ,
    'mytime' => 'time' ,
    'myboolean' => 'boolean' ,
    'myenum' => 'mood' ,
    'mypoint' => 'point' ,
    'mybit' => 'bit32' ,
    'myvarbit' => 'varbit64' ,
    'myarray' => 'integer_array' ,
    'mycomposite' => 'mycomp' ,
    'mynull' => 'nullable_text'
};

$SQL_create_prereqs = <<SQL_CREATE_PREREQS;

CREATE TYPE MOOD AS ENUM ('sad', 'ok', 'happy');
CREATE TYPE MYCOMP AS ( name TEXT, value INTEGER );
CREATE SEQUENCE testdata_vanilla_table_seq;
CREATE SEQUENCE testdata_choc_table_seq;
CREATE SEQUENCE testdata_strawberry_table_seq;
CREATE SEQUENCE testdata_fudge_table_seq_1;
CREATE SEQUENCE testdata_fudge_table_seq_2 START 1001;
CREATE SEQUENCE testdata_fudge_table_seq_3 START 2001;
SQL_CREATE_PREREQS

$SQL_create_vanilla_table = <<SQL_CREATE_ALLTYPES;

CREATE TABLE testdata.vanilla_table (
   myserial INTEGER NOT NULL DEFAULT nextval('testdata_vanilla_table_seq') PRIMARY KEY, 
   mysmallint SMALLINT NOT NULL,
   mybigint BIGINT NOT NULL,
   mynumeric NUMERIC NOT NULL,
   myreal REAL NOT NULL,
   mydouble DOUBLE PRECISION NOT NULL,
   myvarchar VARCHAR NOT NULL,
   myvarchar16 VARCHAR(16) NOT NULL,
   mychar CHAR(256) NOT NULL,
   mytext TEXT NOT NULL,
   mybytea BYTEA NOT NULL,
   mytimestamp TIMESTAMP NOT NULL,
   myinterval INTERVAL NOT NULL,
   mydate DATE NOT NULL,
   mytime TIME NOT NULL,
   myboolean BOOLEAN NOT NULL,
   myenum MOOD NOT NULL,
   mypoint POINT NOT NULL,
   mybit BIT(32) NOT NULL,
   myvarbit BIT VARYING(64) NOT NULL,
   myarray INTEGER[] NOT NULL,
   mycomposite MYCOMP NOT NULL,
   mynull TEXT
);

SQL_CREATE_ALLTYPES

$SQL_create_choc_table = <<SQL_CREATE_CHOCTYPES;

CREATE TABLE testdata.choc_table (
   myserial INTEGER NOT NULL DEFAULT nextval('testdata_choc_table_seq'), 
   mytag TEXT NOT NULL UNIQUE,
   mysmallint SMALLINT NOT NULL,
   mybigint BIGINT NOT NULL,
   mynumeric NUMERIC NOT NULL,
   myreal REAL NOT NULL,
   mydouble DOUBLE PRECISION NOT NULL,
   myvarchar VARCHAR NOT NULL,
   myvarchar16 VARCHAR(16) NOT NULL,
   mychar CHAR(256) NOT NULL,
   mytext TEXT NOT NULL,
   mybytea BYTEA NOT NULL,
   mytimestamp TIMESTAMP NOT NULL,
   myinterval INTERVAL NOT NULL,
   mydate DATE NOT NULL,
   mytime TIME NOT NULL,
   myboolean BOOLEAN NOT NULL,
   myenum MOOD NOT NULL,
   mypoint POINT NOT NULL,
   mybit BIT(32) NOT NULL,
   myvarbit BIT VARYING(64) NOT NULL,
   myarray INTEGER[] NOT NULL,
   mycomposite MYCOMP NOT NULL,
   mynull TEXT
);

SQL_CREATE_CHOCTYPES

$SQL_create_strawberry_table = <<SQL_CREATE_STRAWTYPES;

CREATE TABLE testdata.strawberry_table (
   myserial INTEGER NOT NULL DEFAULT nextval('testdata_strawberry_table_seq'), 
   mytag1 TEXT NOT NULL UNIQUE,
   mytag2 TEXT NOT NULL UNIQUE,
   mytag3 TEXT NOT NULL UNIQUE,
   mysmallint SMALLINT NOT NULL,
   mybigint BIGINT NOT NULL,
   mynumeric NUMERIC NOT NULL,
   myreal REAL NOT NULL,
   mydouble DOUBLE PRECISION NOT NULL,
   myvarchar VARCHAR NOT NULL,
   myvarchar16 VARCHAR(16) NOT NULL,
   mychar CHAR(256) NOT NULL,
   mytext TEXT NOT NULL,
   mybytea BYTEA NOT NULL,
   mytimestamp TIMESTAMP NOT NULL,
   myinterval INTERVAL NOT NULL,
   mydate DATE NOT NULL,
   mytime TIME NOT NULL,
   myboolean BOOLEAN NOT NULL,
   myenum MOOD NOT NULL,
   mypoint POINT NOT NULL,
   mybit BIT(32) NOT NULL,
   myvarbit BIT VARYING(64) NOT NULL,
   myarray INTEGER[] NOT NULL,
   mycomposite MYCOMP NOT NULL,
   mynull TEXT
);

SQL_CREATE_STRAWTYPES

$SQL_create_fudge_table = <<SQL_CREATE_FUDGETYPES;

CREATE TABLE testdata.fudge_table (
   myserial1 INTEGER NOT NULL DEFAULT nextval('testdata_fudge_table_seq_1'), 
   myserial2 INTEGER NOT NULL DEFAULT nextval('testdata_fudge_table_seq_2'), 
   myserial3 INTEGER NOT NULL DEFAULT nextval('testdata_fudge_table_seq_3'), 
   mysmallint SMALLINT NOT NULL,
   mybigint BIGINT NOT NULL,
   mynumeric NUMERIC NOT NULL,
   myreal REAL NOT NULL,
   mydouble DOUBLE PRECISION NOT NULL,
   myvarchar VARCHAR NOT NULL,
   myvarchar16 VARCHAR(16) NOT NULL,
   mychar CHAR(256) NOT NULL,
   mytext TEXT NOT NULL,
   mybytea BYTEA NOT NULL,
   mytimestamp TIMESTAMP NOT NULL,
   myinterval INTERVAL NOT NULL,
   mydate DATE NOT NULL,
   mytime TIME NOT NULL,
   myboolean BOOLEAN NOT NULL,
   myenum MOOD NOT NULL,
   mypoint POINT NOT NULL,
   mybit BIT(32) NOT NULL,
   myvarbit BIT VARYING(64) NOT NULL,
   myarray INTEGER[] NOT NULL,
   mycomposite MYCOMP NOT NULL,
   mynull TEXT,
   PRIMARY KEY (myserial1, myserial2, myserial3)
);

SQL_CREATE_FUDGETYPES

# ----------------------------------------------------------------------------------

# CONNECTION for tests
$user = DBI->connect("dbi:Pg:dbname=$dbname;host=$dbhost;port=$dbport", $dbuser, $dbpass, {AutoCommit => 0});

# installation of the pg51g system before testing our use cases
`cat pg51g.sql | $psql`;

# reconnecting admin user to the test database
$admin = DBI->connect("dbi:Pg:dbname=".$dbname.";host=$dbhost;port=$dbport", 'postgres', $pgpass, {AutoCommit => 0});

# granting privileges for the update user
$admin->do("GRANT USAGE ON SCHEMA pg51g TO \"".$dbuser."\";"); $admin->commit;
$admin->do("GRANT SELECT, UPDATE ON pg51g.metadata TO \"".$dbuser."\";"); $admin->commit;

# generating test data

$user->do("DROP SCHEMA IF EXISTS \"testdata\" CASCADE;"); $user->commit;

$user->do("CREATE SCHEMA \"testdata\";");
$user->do($SQL_create_prereqs);
$user->commit;

$user->do($SQL_create_vanilla_table);
&populate($vanilla_table,'testdata.vanilla_table',5000);
$user->commit;

$user->do($SQL_create_choc_table);
&populate($choc_table,'testdata.choc_table',2500);
$user->commit;

$user->do($SQL_create_strawberry_table);
&populate($strawberry_table,'testdata.strawberry_table',2500);
$user->commit;

$user->do($SQL_create_fudge_table);
&populate($fudge_table,'testdata.fudge_table',5000);
$user->commit;

$user->do("CREATE VIEW testdata.vanilla_view AS SELECT * FROM testdata.vanilla_table WHERE myserial > 3000;");
$user->commit;

$admin->do("SELECT pg51g.key('testdata.vanilla_view','myserial');");
$admin->do("SELECT pg51g.add('testdata.vanilla_view');");
$admin->do("SELECT pg51g.snap('testdata.vanilla_view');");
$admin->commit;

$user->do("CREATE VIEW testdata.vanilla_view_concat1 AS SELECT * FROM testdata.vanilla_table WHERE myserial > 3000;");
$user->commit;

$admin->do("SELECT pg51g.key('testdata.vanilla_view_concat1','(myserial::TEXT||myserial::TEXT||myserial::TEXT)');");
$admin->do("SELECT pg51g.add('testdata.vanilla_view_concat1');");
$admin->do("SELECT pg51g.snap('testdata.vanilla_view_concat1');");
$admin->commit;

$user->do("CREATE VIEW testdata.vanilla_view_concat2 AS SELECT * FROM testdata.vanilla_table WHERE myserial > 3000;");
$user->commit;

$admin->do("SELECT pg51g.key('testdata.vanilla_view_concat2','(myserial::TEXT||''.''||myserial::TEXT||''.''||myserial::TEXT)');");
$admin->do("SELECT pg51g.add('testdata.vanilla_view_concat2');");
$admin->do("SELECT pg51g.snap('testdata.vanilla_view_concat2');");
$admin->commit;

# In theory, we could also test pg51g scenaria for temporary views. This is not an option, however
# because the DBI system has no notion of sessions, so temp views disappear and lookups fail

#$admin->do("CREATE TEMP VIEW vanilla_view_temp AS SELECT * FROM testdata.vanilla_table WHERE myserial <= 2000;");
#$admin->do("SELECT pg51g.key('vanilla_view_temp','myserial');");
#$admin->do("SELECT pg51g.add('vanilla_view_temp');");
#$admin->do("SELECT pg51g.snap('vanilla_view_temp');");
#$admin->commit;

$admin->do("SELECT pg51g.add('testdata.vanilla_table');");
$admin->do("GRANT SELECT, UPDATE, INSERT, DELETE ON pg51g.testdata_vanilla_table TO \"".$dbuser."\";");
$admin->commit;

$admin->do("SELECT pg51g.key('testdata.choc_table', 'mytag');");
$admin->do("SELECT pg51g.add('testdata.choc_table');");
$admin->do("GRANT SELECT, UPDATE, INSERT, DELETE ON pg51g.testdata_choc_table TO \"".$dbuser."\";");
$admin->commit;

$admin->do("SELECT pg51g.key('testdata.strawberry_table', 'mytag1||''----''||mytag2||''----''||mytag3');");
$admin->do("SELECT pg51g.add('testdata.strawberry_table');");
$admin->do("GRANT SELECT, UPDATE, INSERT, DELETE ON pg51g.testdata_strawberry_table TO \"".$dbuser."\";");
$admin->commit;

$admin->do("SELECT pg51g.add('testdata.fudge_table');");
$admin->do("GRANT SELECT, UPDATE, INSERT, DELETE ON pg51g.testdata_fudge_table TO \"".$dbuser."\";");
$admin->commit;

# preparation for USE CASES

# vanilla_table: normal table, single-column normal primary key with trigger
# choc_table: normal table, no primary key, with alternative key def and trigger
# strawberry_table: normal table, no primary key, alternative multi-col key concat
# vanilla_view: normal view 
# vanilla_view_concat1: normal view, key def includes field concatenation
# vanilla_view_concat2: normal view, key def includes field and literal concatenation
# fudge_table: normal table, multi-column normal primary key with trigger

# data change scenario: snap, 3 insertions, 3 deletions, 3 updates (5 random attributes), diff

$admin->do("SELECT pg51g.snap('testdata.vanilla_table');"); $admin->commit; # sleep 3;
&populate($vanilla_table,'testdata.vanilla_table',3); $user->commit;
$user->do("DELETE FROM testdata.vanilla_table WHERE myserial IN (1001, 2002, 3003);"); $user->commit;
&update($vanilla_table,'testdata.vanilla_table','myserial',['1011','2999','3500'],5); $user->commit;

# to showcase Java client only
$admin->do("GRANT SELECT ON pg51g.saved_testdata_vanilla_table TO \"".$dbuser."\";");

$admin->do("SELECT pg51g.snap('testdata.choc_table');"); $admin->commit; # sleep 3;
$user->do("DELETE FROM testdata.choc_table WHERE myserial IN (1001, 2002, 2404);"); $user->commit;
&update($vanilla_table,'testdata.choc_table','myserial',['1011','1999','2003'],5); $user->commit;
# using $vanilla_table here, this is on purporse -- we don't want mytag to be modified!

$admin->do("SELECT pg51g.snap('testdata.strawberry_table');"); $admin->commit; # sleep 3;
$user->do("DELETE FROM testdata.strawberry_table WHERE myserial IN (1001, 2002, 2404);"); $user->commit;
&update($vanilla_table,'testdata.strawberry_table','myserial',['1011','1999','2003'],5); $user->commit;
# using $vanilla_table here, this is on purporse -- we don't want mytag to be modified!

$admin->do("SELECT pg51g.snap('testdata.fudge_table');"); $admin->commit; # sleep 3;
&populate($fudge_table,'testdata.fudge_table',3); $user->commit;
$user->do("DELETE FROM testdata.fudge_table WHERE myserial1 IN (1001, 2002, 3003);"); $user->commit;
&update($fudge_table,'testdata.fudge_table',"myserial1||'.'||myserial2||'.'||myserial3",['1011.2011.3011','2999.3999.4999','3500.4500.5500'],5);
$user->commit;

sub test_01_SnapDoDiff_5000_normal_table_normal_key {
    $admin->disconnect();
    $admin = DBI->connect("dbi:Pg:dbname=".$dbname.";host=$dbhost;port=$dbport", 'postgres', $pgpass, {AutoCommit => 0});
    print "..........................................................\n";
    print "Use-case: SnapDoDiff_5000_normal_table_normal_key\n";
    my $ans = [
                ['1001','DELETE'], ['2002','DELETE'], ['3003','DELETE'],
                ['5001','INSERT'], ['5002','INSERT'], ['5003','INSERT'],
                ['1011','UPDATE'], ['2999','UPDATE'], ['3500','UPDATE']
              ];
    assert( &diff_and_compare('testdata.vanilla_table', $ans), "Problem with SnapDoDiff_5000_normal_table_normal_key");
    assert( &session_active, "SEVERE! SnapDoDiff_5000_normal_table_normal_key may have killed your server");
    print "\n";
}

sub test_02_SnapDoDiff_2000_normal_view {
    $admin->disconnect();
    $admin = DBI->connect("dbi:Pg:dbname=".$dbname.";host=$dbhost;port=$dbport", 'postgres', $pgpass, {AutoCommit => 0});
    print "..........................................................\n";
    print "Use-case: SnapDoDiff_2000_normal_view\n";
    my $ans = [
                ['3003','DELETE'],
                ['5001','INSERT'], ['5002','INSERT'], ['5003','INSERT'],
                ['3500','UPDATE']
              ];
    $admin->do("SELECT pg51g.do('testdata.vanilla_view');"); $admin->commit;
    assert( &diff_and_compare('testdata.vanilla_view', $ans), "Problem with SnapDoDiff_2000_normal_view");
    assert( &session_active, "SEVERE! SnapDoDiff_2000_normal_view may have killed your server");
    print "\n";
}

sub test_03_SnapDoDiff_2000_normal_view_concat1 {
    $admin->disconnect();
    $admin = DBI->connect("dbi:Pg:dbname=".$dbname.";host=$dbhost;port=$dbport", 'postgres', $pgpass, {AutoCommit => 0});
    print "..........................................................\n";
    print "Use-case: SnapDoDiff_2000_normal_view_concat1\n";
    my $ans = [
                ['300330033003','DELETE'],
                ['500150015001','INSERT'], ['500250025002','INSERT'], ['500350035003','INSERT'],
                ['350035003500','UPDATE']
              ];
    $admin->do("SELECT pg51g.do('testdata.vanilla_view_concat1');"); $admin->commit;
    assert( &diff_and_compare('testdata.vanilla_view_concat1', $ans), "Problem with SnapDoDiff_2000_normal_view_concat1");
    assert( &session_active, "SEVERE! SnapDoDiff_2000_normal_view_concat1 may have killed your server");
    print "\n";
}

sub test_04_SnapDoDiff_2000_normal_view_concat2 {
    $admin->disconnect();
    $admin = DBI->connect("dbi:Pg:dbname=".$dbname.";host=$dbhost;port=$dbport", 'postgres', $pgpass, {AutoCommit => 0});
    print "..........................................................\n";
    print "Use-case: SnapDoDiff_2000_normal_view_concat2\n";
    my $ans = [
                ['3003.3003.3003','DELETE'],
                ['5001.5001.5001','INSERT'], ['5002.5002.5002','INSERT'], ['5003.5003.5003','INSERT'],
                ['3500.3500.3500','UPDATE']
              ];
    $admin->do("SELECT pg51g.do('testdata.vanilla_view_concat2');"); $admin->commit;
    assert( &diff_and_compare('testdata.vanilla_view_concat2', $ans), "Problem with SnapDoDiff_2000_normal_view_concat2");
    assert( &session_active, "SEVERE! SnapDoDiff_2000_normal_view_concat2 may have killed your server");
    print "\n";
}

sub test_05_SnapDoDiff_2500_normal_table_alternative_key_single {
    $admin->disconnect();
    $admin = DBI->connect("dbi:Pg:dbname=".$dbname.";host=$dbhost;port=$dbport", 'postgres', $pgpass, {AutoCommit => 0});
    print "..........................................................\n";
    print "Use-case: SnapDoDiff_2500_normal_table_alternative_key_single\n";
    my $ans = [
                ['1001','DELETE'], ['2002','DELETE'], ['2404','DELETE'],
                ['1011','UPDATE'], ['1999','UPDATE'], ['2003','UPDATE']
              ];
    assert( &diff_and_compare_light('testdata.choc_table', $ans), "Problem with SnapDoDiff_2500_normal_table_alternative_key_single");
    assert( &session_active, "SEVERE! SnapDoDiff_2500_normal_table_alternative_key_single may have killed your server");
    print "\n";
}

sub test_06_SnapDoDiff_2500_normal_table_alternative_key_multi {
    $admin->disconnect();
    $admin = DBI->connect("dbi:Pg:dbname=".$dbname.";host=$dbhost;port=$dbport", 'postgres', $pgpass, {AutoCommit => 0});
    print "..........................................................\n";
    print "Use-case: SnapDoDiff_2500_normal_table_alternative_key_multi\n";
    my $ans = [
                ['1001','DELETE'], ['2002','DELETE'], ['2404','DELETE'],
                ['1011','UPDATE'], ['1999','UPDATE'], ['2003','UPDATE']
              ];
    assert( &diff_and_compare_light('testdata.strawberry_table', $ans), "Problem with SnapDoDiff_2500_normal_table_alternative_key_multi");
    assert( &session_active, "SEVERE! SnapDoDiff_2500_normal_table_alternative_key_multi may have killed your server");
    print "\n";
}

sub test_07_SnapDoDiff_5000_normal_table_normal_key_multi {
    $admin->disconnect();
    $admin = DBI->connect("dbi:Pg:dbname=".$dbname.";host=$dbhost;port=$dbport", 'postgres', $pgpass, {AutoCommit => 0});
    print "..........................................................\n";
    print "Use-case: SnapDoDiff_5000_normal_table_normal_key_multi\n";
    my $ans = [
                ['1001-2001-3001','DELETE'], ['2002-3002-4002','DELETE'], ['3003-4003-5003','DELETE'],
                ['5001-6001-7001','INSERT'], ['5002-6002-7002','INSERT'], ['5003-6003-7003','INSERT'],
                ['1011-2011-3011','UPDATE'], ['2999-3999-4999','UPDATE'], ['3500-4500-5500','UPDATE']
              ];
    assert( &diff_and_compare('testdata.fudge_table', $ans), "Problem with SnapDoDiff_5000_normal_table_normal_key_multi");
    assert( &session_active, "SEVERE! SnapDoDiff_5000_normal_table_normal_key_multi may have killed your server");
    print "\n";
}

# ----------------------------------------------------------------------------------

# METADATA lookups ################################################################# 

# returns if the connection is still active, 0 otherwise

sub session_active { return $user->ping; }

sub diff_and_compare {
    my $target = shift; my $sol = shift;
    # this is necessary for temp views -- doesn't do anything for tables, anyway
    # $admin->do("SELECT pg51g.do('".$target."');"); $admin->commit;
    # on to the diffing...
    my $sql = "SELECT * FROM ( SELECT (pg51g.diff('".$target."')).* ) AS mydiff ORDER BY op, key;";
    my $ans = $admin->selectall_arrayref($sql); my $diffs = scalar(@$ans);
    print "   _diff_and_compare_: Number of differences found: ".scalar(@$ans), "\n";
    my $i; my $size = scalar(@$sol); my $match = 1;
    for($i=0; $i<$size; $i++) {
        $match = 0 unless $$sol[$i]->[0] eq $$ans[$i]->[0] and $$sol[$i]->[1] eq $$ans[$i]->[1];
        last unless $match;
    }
    if ($match) { print "....................................................... PASS\n"; }
    else { print "....................................................... FAIL\n";
        for($i=0; $i<$diffs; $i++) { print $$ans[$i]->[0].",".$$ans[$i]->[1]."\n"; }
    }
    return $match;
}

sub diff_and_compare_light {
    my $target = shift; my $sol = shift;
    # this is necessary for temp views -- doesn't do anything for tables, anyway
    # $admin->do("SELECT pg51g.do('".$target."');"); $admin->commit;
    # on to the diffing...
    my $sql = "SELECT * FROM ( SELECT (pg51g.diff('".$target."')).* ) AS mydiff ORDER BY op, key;";
    my $ans = $admin->selectall_arrayref($sql); my $diffs = scalar(@$ans);
    print "   _diff_and_compare_: Number of differences found: ".scalar(@$ans), "\n";
    my $i; my $size = scalar(@$sol); my $match = 1;
    for($i=0; $i<$size; $i++) {
        $match = 0 unless $$sol[$i]->[1] eq $$ans[$i]->[1];
        last unless $match;
    }
    if ($match) { print "....................................................... PASS\n"; }
    else { print "....................................................... FAIL\n";
        for($i=0; $i<$diffs; $i++) { print $$ans[$i]->[0].",".$$ans[$i]->[1]."\n"; }
    }
    return $match;
}

# ----------------------------------------------------------------------------------

# METADATA lookups ################################################################# 

# returns number of entries for target table in the metadata table
sub metadata_count_4target {
    my $schname = shift; my $tblname = shift;
    return 0;
}

# returns hash of contents for a particular metatdata entry
sub metadata_contents {
    my $schname = shift; my $tblname = shift;
    return 0;
}

# ----------------------------------------------------------------------------------

# ALTERNATIVES lookups ############################################################# 

# returns number of entries for target table in the alternatives table
sub alternatives_count_4target {
    my $schname = shift; my $tblname = shift;
    return 0;
}

# returns hash of contents for a particular entry in the alternatives table
sub alternatives_contents {
    my $schname = shift; my $tblname = shift;
    return 0;
}

# ----------------------------------------------------------------------------------

# SIGTBL lookups ###################################################################

# returns number corresponding to total number of rows for the sigtbl
sub sigtbl_count {
    my $sigtbl = shift;
}

# returns number of folding levels in the sigtbl
sub sigtbl_levels {
}

# returns number corresponding to number of rows per folding level
sub sigtbl_count_4level {
    my $sigtbl = shift; my $level = shift;
}

# returns the global checksum for the table
sub sigtbl_root {
    my $sigtbl = shift;
}
# ----------------------------------------------------------------------------------

#sub set_up    { print "hello world\n"; }
#sub tear_down { print "leaving world again\n"; }

# run your test
create_suite();
run_suite();

#$admin->disconnect();

#$user->do("DROP SCHEMA IF EXISTS \"testdata\" CASCADE;");
#$user->disconnect();

# cleanup

#$admin = DBI->connect("dbi:Pg:dbname=".$dbname, 'postgres', $pgpass, {AutoCommit => 1});
#$admin->do("DROP DATABASE IF EXISTS \"".$dbname."\";");
#$admin->do("DROP USER IF EXISTS \"".$dbuser."\";");
#$admin->disconnect();

