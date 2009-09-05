import java.util.*;
import java.sql.*;

/* sample java client for pg51g-based cross-database row-level comparisons
   -----------------------------------------------------------------------
   author: Michael Nacos (m.nacos AT gmail.com)
   license: same as pg51g -- please see COPYRIGHT
                                                                            */

public class JComparator {

    public static void main(String[] argv) {
       // JComparator myself;
       if (argv.length > 7) { new JComparator(argv[0],argv[1],argv[2],argv[3],argv[4],argv[5],argv[6],argv[7]); }
       else {
     System.out.println("USAGE: java ... JComparator con1 usr1 pass1 tbl1 con2 usr2 pass2 tbl2");
     System.out.println("e.g. : java ... JComparator jdbc:postgresql://localhost:5432/pg51gTests pg51g.testdata_vanilla_table ...");
       }
    }

    public JComparator(String str1, String usr1, String pas1, String tbl1, String str2, String usr2, String pas2, String tbl2) {
        table1 = tbl1; table2 = tbl2;
        // Database Connectivity
        try {
           Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException cnfe) {
           System.err.println("*** Couldn't find the Postgresql JDBC driver!");
           System.exit(1);
        }
        // Some output
        System.out.println("---------------------------------------------------------------------------------------------------------------------------");
        System.out.println("Connection 1:\t"+str1);
        System.out.println("Table 1:\t\t"+tbl1);
        System.out.println("---------------------------------------------------------------------------------------------------------------------------");
        System.out.println("Connection 2:\t"+str2);
        System.out.println("Table 2:\t\t"+tbl2);
        System.out.println("---------------------------------------------------------------------------------------------------------------------------");
        // Attempting to connect
        connection1 = null; connection2 = null;
        try { connection1 = DriverManager.getConnection(str1,usr1,pas1); }
        catch (SQLException se) { System.err.println("*** Couldn't connect to Postgresql server using: "+str1); System.exit(1); }
        try { connection2 = DriverManager.getConnection(str2,usr2,pas2); }
        catch (SQLException se) { System.err.println("*** Couldn't connect to Postgresql server using: "+str2); System.exit(1); }

        if (connection1 != null) {
           try { connection1.createStatement(); System.out.println("Database connection 1: OK"); }
           catch (Exception e) { System.err.println("*** Couldn't create a JDBC statement: (connection 1)"); System.exit(1); }
           try { connection2.createStatement(); System.out.println("Database connection 2: OK"); }
           catch (Exception e) { System.err.println("*** Couldn't create a JDBC statement: (connection 1)"); System.exit(1); }
        }
        System.out.println("---------------------------------------------------------------------------------------------------------------------------");

        int size = 0;
        try { size = goCompare(); } catch (SQLException e) {
            System.err.print("*** goCompare() has failed! "); System.err.println(e.getMessage());
        }
        System.out.print("Number of differences: "); System.out.println(size);
    }

    private int goCompare() throws SQLException {
        int depth1 = getLevels(connection1, table1); int depth2 = getLevels(connection2, table2);
        if (depth1 != depth2) { System.err.println("Different folding levels: aborting..."); System.exit(1); }
        String mask, pri; int i, level, mySize, counter; Pg51gArray sec; ArrayList<String> pris = new ArrayList<String>();
        counter = 0; pris.add("00000000000000000000000000000000"); // For top level

        // make sure autocommit is off
        connection1.setAutoCommit(false);

        for(level=depth1; level>0; level--) {
           counter = 0; mySize = pris.size(); mask = getMask(connection1, depth1, level);
           ArrayList<String> tmp = new ArrayList<String>();
           System.out.print("--> Level: "); System.out.print(level-1);
           System.out.println("                                                                                                               ");
           System.out.println("          -----------------------------------------------------------------------------------------------------------------");
            for(i=0; i<mySize; i++) {
              pri = pris.get(i); sec = null;
              sec = getRecordsForGroup(connection2, table2, (level-1), mask, pri);
              Statement st1 = connection1.createStatement();
              String query1 = "SELECT * FROM "+table1+" WHERE pg51g.group_md5('"+mask+"',key) = '"+pri+"' AND level = "+(level-1)+" ORDER BY pri, key, val;";
              String pri1, key1, val1; Pg51gStruct temp; int k;
              // Turn on use of the cursor
              st1.setFetchSize(500);
              ResultSet rs1 = st1.executeQuery(query1);
             while(rs1.next()) {
                 pri1 = rs1.getString(2); key1 = rs1.getString(3); val1 = rs1.getString(4);
                 temp = new Pg51gStruct( (level-1), pri1, key1, val1);
                 if (!sec.hasPri(temp)) { // there's an INSERT difference
                    System.out.print("INSERT: "); tmp.add(temp.getPri()); temp.print(); counter++;
                 }
                 else {
                    if (!sec.hasA(temp)) { // there's an UPDATE difference
                       System.out.print("UPDATE: "); tmp.add(temp.getPri()); temp.print(); counter++;
                    }
                 }
              }
              rs1.close();
              st1.close();

              Pg51gArray dels = sec.getUnmatched(); int size = dels.getList().size(); sec = null;
              for(k=0; k<size; k++) { counter++; System.out.print("DELETE: "); tmp.add(dels.getList().get(k).getPri()); dels.getList().get(k).print(); }
           }
           pris = null; pris = tmp;
           System.out.println("---------------------------------------------------------------------------------------------------------------------------");
        }
        return counter;
    }
   
    private int getLevels(Connection con, String tbl) throws SQLException {
        PreparedStatement getTuples = con.prepareStatement( "SELECT DISTINCT level FROM "+tbl+" ORDER BY level DESC;" );
        ResultSet rs = getTuples.executeQuery();
		// Pg51gArray results = new Pg51gArray();
		int levels = 0;
        if(rs.next()) { levels = rs.getInt(1); }
        rs.close(); getTuples.close();
        return levels;
    }

    private String getMask(Connection con, int depth, int level) throws SQLException {
        PreparedStatement getTuples = con.prepareStatement( "SELECT pg51g.mask4level( ? , ? );" );
        getTuples.setInt(1,depth); getTuples.setInt(2,level);
        ResultSet rs = getTuples.executeQuery();
		// Pg51gArray results = new Pg51gArray();
		String mask = "";
        if(rs.next()) { mask = rs.getString(1); }
        rs.close(); getTuples.close();
        return mask;
    }


    private Pg51gArray getRecordsForGroup(Connection con, String tbl, int level, String mask, String pri) throws SQLException {
        PreparedStatement getTuples = con.prepareStatement(
                    "SELECT * FROM "+tbl+" WHERE pg51g.group_md5(?,key) = ? AND level = ? ORDER BY pri, key, val;"
        );
        getTuples.setString(1,mask); getTuples.setString(2,pri); getTuples.setInt(3,level);
        ResultSet rs = getTuples.executeQuery(); Pg51gArray results = new Pg51gArray();
        while (rs.next()) {
            Pg51gStruct tmp = new Pg51gStruct( rs.getInt(1),rs.getString(2),rs.getString(3),rs.getString(4) );
            results.add(tmp);
        }
        rs.close(); getTuples.close();
        return results;
    }

    private Connection connection1;
    private String table1;
    private Connection connection2;
    private String table2;

    private class Pg51gStruct {
        public Pg51gStruct(int mylevel, String mypri, String mykey, String myval) { level = mylevel; pri = mypri; key = mykey; val = myval; m = false; }
        public int getLevel() { return this.level; }
        public String getPri() { return this.pri; }
        public void setKey(String newkey) { this.key = newkey; }
        public String getKey() { return this.key; }
        public void setVal(String newval) { this.val = newval; }
        public String getVal() { return this.val; }
        public void setMatched(Boolean newval) { this.m = newval; }
        public Boolean getMatched() { return this.m; }
        public Pg51gStruct duplicate() { Pg51gStruct clone = new Pg51gStruct(level, pri, key, val); return clone; }
        public void print() { System.out.println("  Pri: "+pri+" Key: "+key+" Val: "+val); }
        private int level;
        private String pri;
        private String key;
        private String val;
        private Boolean m; // matched
    }

    private class Pg51gArray {
        public Pg51gArray() { records = new ArrayList<Pg51gStruct>(); }
	public void add(Pg51gStruct mystruct) { records.add(mystruct); }
        public ArrayList<Pg51gStruct> getList() { return this.records; }
        public Pg51gArray getUnmatched() {
            int size = records.size(); int i; Pg51gArray nova = new Pg51gArray();
            for(i=0; i<size; i++) { if (!records.get(i).getMatched()) { nova.getList().add(records.get(i)); } }
            return nova;
        }
        private ArrayList<Pg51gStruct> records;
        public Boolean hasA(Pg51gStruct item) { 
            int size = records.size(); int i;
            for(i=0; i<size; i++) {
               if (item.getLevel() == records.get(i).getLevel() &&
                      item.getPri().equals(records.get(i).getPri()) &&
                        item.getKey().equals(records.get(i).getKey()) &&
                           item.getVal().equals(records.get(i).getVal()) ) { return true; }
            }
            return false;
        }
        public Boolean hasPri(Pg51gStruct item) { 
            int size = records.size(); int i;
            for(i=0; i<size; i++) {
               if (item.getLevel() == records.get(i).getLevel() && item.getPri().equals(records.get(i).getPri())) { records.get(i).setMatched(true); return true; }
            }
            return false;
        }
     }

}

