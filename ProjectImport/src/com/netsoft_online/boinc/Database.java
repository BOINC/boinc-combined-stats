package com.netsoft_online.boinc;
// BOINC Combined Statistics
// http://boinc.netsoft-online.com/
// For use with Berkeley Open Infrastructure for Network Computing (BOINC)
// Source: http://boinc.berkeley.edu/trac/browser/trunk/boinc_stats
// Copyright(C) 2006-2007 James E. Drews
//
// This is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation;
// either version 2.1 of the License, or (at your option) any later version.
//
// This software is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
// See the GNU Lesser General Public License for more details.
//
// To view the GNU Lesser General Public License visit
// http://www.gnu.org/copyleft/lesser.html
// or write to the Free Software Foundation, Inc.,
// 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//
//
//
// Database.java
//
// MySQL DB Routines
//
//

import java.math.BigInteger;
import java.security.MessageDigest;
import java.sql.*;
import java.util.*;
import org.apache.log4j.Logger;

public class Database {

    String szConnectString = null;
    String szDBDriver = null;
    String szLogin = null;
    String szPassword = null;
    boolean bConnected = false;
    static Logger log = Logger.getLogger(Database.class.getName());  // log4j stuff
    
    private Connection cDBConnection;
    private PreparedStatement psUserAddEntry=null;
    private String sUserAddEntryTable=null;
    private PreparedStatement psHostAddEntry=null;
    private String sHostAddEntryTable=null;
    private PreparedStatement psTeamAddEntry=null;
    private String sTeamAddEntryTable=null;
    private PreparedStatement psCTeamAddEntry=null;
    private PreparedStatement psCPidHistory=null;
    private PreparedStatement psTeamHistory=null;
    
    public Database()
    {
      bConnected = false;
    }
    
    public void SetDatabaseInfo(String dbDriver, String connectString, String login, String password)
    {
        szDBDriver = dbDriver;
        szConnectString = connectString;
        szLogin = login;
        szPassword = password;        
    }
    
    public void LoginToDatabase() throws java.sql.SQLException {
        if (bConnected == true) return;
        try {
            // Load the jdbc driver
            Driver d = (Driver)Class.forName(szDBDriver).newInstance();
            if (d == null) throw new SQLException("Failed to get database driver");
            cDBConnection = DriverManager.getConnection(szConnectString, szLogin, szPassword);
        } 
        
        catch (Exception exc) 
        {
            //log.error(exc);
            cDBConnection = null;
            bConnected = false;
            throw new SQLException(exc.toString());
        }
        
        //cDBConnection.setAutoCommit(false);
        bConnected = true;        
    }
    
    public void ReadPModelTable (Hashtable<String, Integer> myPM) throws SQLException{
        
        String query = "select pmodel_id,p_model from pmodel";
        myPM.clear();

        Statement statement = null;
        ResultSet resultSet = null;
        
        try {            
            statement = cDBConnection.createStatement();
            
            resultSet = statement.executeQuery( query );

            if ( resultSet == null ) {
                throw new SQLException();
            }
            
            while( resultSet.next() )
            {
                Integer pmodel_id = new Integer(( (int) resultSet.getInt("pmodel_id")));
                String p_model = ( (String) resultSet.getString( "p_model" ) );  
                
                myPM.put(p_model,pmodel_id);
            }       
            resultSet.close();
            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("ReadPModelTable - " + s.toString());
        }
    }
    
 
    
    public void ReadPVendorTable (Hashtable<String, Integer> myPV) throws SQLException{
        
        String query = "select pvendor_id,p_vendor from pvendor";
        myPV.clear();

        Statement statement = null;
        ResultSet resultSet = null;
        
        try {            
            statement = cDBConnection.createStatement();
            
            resultSet = statement.executeQuery( query );

            if ( resultSet == null ) {
                throw new SQLException();
            }
            
            while( resultSet.next() )
            {
                Integer pvendor_id = new Integer(( (int) resultSet.getInt("pvendor_id")));
                String p_vendor = ( (String) resultSet.getString( "p_vendor" ) );  
                
                myPV.put(p_vendor,pvendor_id);
            }    
            resultSet.close();
            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("ReadPVendorTable - " + s.toString());
        }
    }

    public void ReadOSNameTable (Hashtable<String, Integer> myOS) throws SQLException{
        
        String query = "select osname_id,os_name from osname";
        myOS.clear();

        Statement statement = null;
        ResultSet resultSet = null;
        
        try {            
            statement = cDBConnection.createStatement();
            
            resultSet = statement.executeQuery( query );

            if ( resultSet == null ) {
                throw new SQLException();
            }
            
            while( resultSet.next() )
            {
                Integer osname_id = new Integer(( (int) resultSet.getInt("osname_id")));
                String os_name = ( (String) resultSet.getString( "os_name" ) );  
                
                myOS.put(os_name,osname_id);
            }   
            resultSet.close();
            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("ReadOSNameTable", s);
        }
    }

    public void ReadCountryTable (Hashtable<String, Country> myCountries) throws SQLException{
        
        String query = "select country_id, country, remap_id from country";
        myCountries.clear();

        Statement statement = null;
        ResultSet resultSet = null;
        
        try {            
            statement = cDBConnection.createStatement();
            
            resultSet = statement.executeQuery( query );

            if ( resultSet == null ) {
                throw new SQLException();
            }
            
            while( resultSet.next() )
            {
            	Country c = new Country();
                c.country_id = resultSet.getInt("country_id");
                c.country = resultSet.getString( "country" ).toLowerCase();  
                c.remap_id = resultSet.getInt("remap_id");
                myCountries.put(c.country,c);
            }   
            resultSet.close();
            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("ReadCountryTable", s);
        }
    }
    
   public int AddPModel(String p_model) throws SQLException {
        
        Statement statement = null;
        int table_id=0;
        
        String query = "insert into pmodel (p_model) values ('"+p_model+"')";
        
        
        try {           
            statement = cDBConnection.createStatement();
            int res = statement.executeUpdate(query);
            if (res != 1) {
                log.debug("Number of rows updated is wrong: "+res);
            }

            ResultSet rs;
            rs = statement.getGeneratedKeys();
            while (rs.next())
                table_id = rs.getInt(1);
            
            // clean up
            rs.close();
            statement.close();
            statement = null;
            return table_id;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("AddPModel - " + s.toString());
        }   
    }    
   
    public int AddPVendor( String p_vendor) throws SQLException {
        
        Statement statement = null;
        int table_id=0;
        String query = "insert into pvendor (p_vendor) values ('"+p_vendor+"')";
        
        try {           
            statement = cDBConnection.createStatement();
            int res = statement.executeUpdate(query);
            if (res != 1) {
                //RollbackTransaction(); // problems, should only be 1 row
                log.debug("Number of rows updated is wrong: "+res);
            }

            ResultSet rs;
            rs = statement.getGeneratedKeys();
            while (rs.next())
                table_id = rs.getInt(1);
            
            // clean up
            rs.close();
            statement.close();
            statement = null;
            return table_id;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("AddPVendor - " + s.toString());
        }
        
    }
        
    public int AddOSName(String os_name) throws SQLException {
        
        Statement statement = null;
        int table_id=0;
        
        String query = "insert into osname (os_name) values ('"+os_name+"')";
        
        
        try {           
            statement = cDBConnection.createStatement();
            int res = statement.executeUpdate(query);
            if (res != 1) {
                //RollbackTransaction(); // problems, should only be 1 row
                log.debug("Number of rows updated is wrong: "+res);
            }

            ResultSet rs;
            rs = statement.getGeneratedKeys();
            while (rs.next())
                table_id = rs.getInt(1);
            
            // clean up
            rs.close();
            statement.close();
            statement = null;
            return table_id;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("AddOSName - " + s.toString());
        }
        
    }
    
    public int AddCountry(String country) throws SQLException {
        
        Statement statement = null;
        int table_id = 0;
        
        String ccountry = country.replaceAll("'","\\\\'");
        String query = "insert into country (country) values ('"+ccountry+"')";
        
        
        try {           
            statement = cDBConnection.createStatement();
            int res = statement.executeUpdate(query);
            if (res != 1) {
                log.debug("Number of rows updated is wrong: "+res);
            }

            ResultSet rs;
            rs = statement.getGeneratedKeys();
            while (rs.next())
                table_id = rs.getInt(1);
            
            // clean up
            rs.close();
            statement.close();
            statement = null;          
            return table_id;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("AddCountry - " + s.toString());
        }
        
    }
    
    public int UserAddEntry(String tablename, int user_id, String name, int country_id, long create_time, String user_cpid, String url,
            int teamid, int b_cteam_id, long total_credit, long rac, long rac_time, long cpid_id) throws SQLException {
        
        int table_id=0;
        if (name.length() > 255) name = name.substring(0,254);
        if (url.length() > 200) url = url.substring(0,199);
        
        if (psUserAddEntry == null || sUserAddEntryTable.compareToIgnoreCase(tablename) != 0) {
        	if (psUserAddEntry != null) psUserAddEntry.close();
        	psUserAddEntry = cDBConnection.prepareStatement("insert ignore into "+tablename+" (user_id,name,country_id,create_time,user_cpid,url,teamid,b_cteam_id,total_credit,rac,rac_time,b_cpid_id) values (?,?,?,?,?,?,?,?,?,?,?,?)");
        	sUserAddEntryTable = tablename;
        }
        
        psUserAddEntry.setLong(1,user_id);
        psUserAddEntry.setString(2,name);
        psUserAddEntry.setInt(3,country_id);
        psUserAddEntry.setLong(4, create_time);
        psUserAddEntry.setString(5,user_cpid);
        psUserAddEntry.setString(6,url);
        psUserAddEntry.setInt(7,teamid);
        psUserAddEntry.setInt(8,b_cteam_id);
        psUserAddEntry.setLong(9,total_credit);
        psUserAddEntry.setLong(10,rac);
        psUserAddEntry.setLong(11, rac_time);
        psUserAddEntry.setLong(12,cpid_id);
        
                
        try {           
        	int res = psUserAddEntry.executeUpdate();
            if (res != 1) {
                log.debug("Number of rows updated is wrong: "+res);
            }

            ResultSet rs;
            rs = psUserAddEntry.getGeneratedKeys();
            while (rs.next())
                table_id = rs.getInt(1);
            rs.close();
            return table_id;
        } catch (SQLException s) {
            throw new SQLException("AddUserEntry - " + s.toString());
        }
    }
    
    public void UserRead(Hashtable<String, User> myUsers, String table) throws SQLException {
        
        myUsers.clear();
        try { Thread.sleep(1000); }
        catch (Exception e) {}
        String query = "select user_id,b_cpid_id,user_cpid,teamid from "+table;
        Statement statement = null;
        ResultSet resultSet = null;
        
        try {            
            statement = cDBConnection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,java.sql.ResultSet.CONCUR_READ_ONLY);
            statement.setFetchSize(Integer.MIN_VALUE);
            
            resultSet = statement.executeQuery( query );

            if ( resultSet == null ) {
                throw new SQLException();
            }
            
            while( resultSet.next() )
            {
                User u = new User();                
                u.user_id = resultSet.getLong("user_id");
                u.b_cpid_id = resultSet.getLong("b_cpid_id");
                u.cpid = resultSet.getString("user_cpid");
                u.team_id = resultSet.getLong("teamid");
                String t = (String) new Long(u.user_id).toString();
                myUsers.put(t,u);
            } 
            resultSet.close();
            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("ReadUsers() - " + s.toString());
        }        
    }
    
    
    public int HostAddEntry(String tablename, int host_id, int user_id, int p_vendor_id, int p_model_id,
            int os_name_id, String os_version, long create_time, long rpc_time, int timezone, int ncpus, double p_fpops,
            double p_iops, double p_membw, double m_nbytes, double m_cache, double m_swap, double d_total, double d_free,
            double n_bwup, double n_bwdown, double avg_turnaround, String host_cpid, long total_credit, long rac, long rac_time, 
            long b_cpid_id, double credit_per_cpu_sec) 
        throws SQLException {
               
        if (psHostAddEntry == null || sHostAddEntryTable.compareToIgnoreCase(tablename) != 0) {
        	if (psHostAddEntry != null) psHostAddEntry.close();
        	psHostAddEntry = cDBConnection.prepareStatement("insert ignore into "+tablename+" (host_id,user_id,p_vendor,p_model,os_name,os_version,create_time,rpc_time,timezone,ncpus,p_fpops,p_iops,p_membw,m_nbytes,m_cache,m_swap,d_total,d_free,n_bwup,n_bwdown,avg_turnaround,host_cpid,total_credit,rac,rac_time,b_cpid_id,credit_per_cpu_sec) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        	sHostAddEntryTable = tablename;
        }
        
        int table_id=0;
                
        try {
        	psHostAddEntry.setLong(1,host_id);
        	psHostAddEntry.setLong(2,user_id);
        	psHostAddEntry.setInt(3,p_vendor_id);
        	psHostAddEntry.setInt(4,p_model_id);
        	psHostAddEntry.setInt(5,os_name_id);
        	psHostAddEntry.setString(6,os_version);
        	psHostAddEntry.setLong(7,create_time);
        	psHostAddEntry.setLong(8,rpc_time);
        	psHostAddEntry.setInt(9,timezone);
        	psHostAddEntry.setInt(10,ncpus);
        	psHostAddEntry.setDouble(11,p_fpops);
        	psHostAddEntry.setDouble(12,p_iops);
        	psHostAddEntry.setDouble(13,p_membw);
        	psHostAddEntry.setDouble(14,m_nbytes);
        	psHostAddEntry.setDouble(15,m_cache);
        	psHostAddEntry.setDouble(16,m_swap);
        	psHostAddEntry.setDouble(17,d_total);
        	psHostAddEntry.setDouble(18,d_free);
        	psHostAddEntry.setDouble(19,n_bwup);
        	psHostAddEntry.setDouble(20,n_bwdown);
        	psHostAddEntry.setDouble(21,avg_turnaround);
        	psHostAddEntry.setString(22,host_cpid);
        	psHostAddEntry.setLong(23,total_credit);
        	psHostAddEntry.setLong(24,rac);
        	psHostAddEntry.setLong(25,rac_time);
        	psHostAddEntry.setLong(26,b_cpid_id);
        	psHostAddEntry.setDouble(27,credit_per_cpu_sec);
        	
            
            int res = psHostAddEntry.executeUpdate();
            if (res != 1) {
                log.debug("Number of rows updated is wrong: "+res);
            }

            return table_id;
        } catch (SQLException s) {
            throw new SQLException("AddHostEntry - " + s.toString());
        }
    }

    public void HostRead(Hashtable<String, Host> myHosts, int project_id) throws SQLException {
        
        myHosts.clear();
        try { Thread.sleep(1000); }
        catch (Exception e) {}
        String query = "select table_id,host_id from b_hosts where project_id="+project_id;
        Statement statement = null;
        ResultSet resultSet = null;
        
        try {            
            statement = cDBConnection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,java.sql.ResultSet.CONCUR_READ_ONLY);
            statement.setFetchSize(Integer.MIN_VALUE);

            
            resultSet = statement.executeQuery( query );

            if ( resultSet == null ) {
                throw new SQLException();
            }
            
            while( resultSet.next() )
            {
                Host h = new Host();
                h.table_id =  resultSet.getInt("table_id");
                h.host_id = resultSet.getInt("host_id");
                String t = (String) new Integer(h.host_id).toString();
                myHosts.put(t,h);
            } 
            resultSet.close();
            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("HostRead() - " + s.toString());
        }        
    }

   
    
    public int TeamAddEntry( String table, int team_id, int cteam_table_id, int type, String name, String teamCPID, int founder_id,
            long total_credit, long rac, long rac_time, int nusers, String founder_name, long create_time, String url, String description,
            int country_id) throws SQLException {
        
        if (psTeamAddEntry == null || sTeamAddEntryTable.compareToIgnoreCase(table) != 0) {
        	if (psTeamAddEntry != null) psTeamAddEntry.close();
        	psTeamAddEntry = cDBConnection.prepareStatement("insert ignore into "+table+" (team_id,b_cteam_id,type,name,team_cpid,founder_id,total_credit,rac,rac_time,nusers,founder_name,create_time,url,description,country_id) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        	sTeamAddEntryTable = table;
        }

        if (name.length() > 255) name = name.substring(0,254);
        if (founder_name.length() > 100) founder_name = founder_name.substring(0,99);
        if (url.length() > 200) url = url.substring(0,199);
        if (description.length() > 200) description = description.substring(0,199);

        int table_id=0;
                
        try {
        	psTeamAddEntry.setLong(1,team_id);
        	psTeamAddEntry.setLong(2, cteam_table_id);
        	psTeamAddEntry.setInt(3, type);
        	psTeamAddEntry.setString(4, name);
        	psTeamAddEntry.setString(5, teamCPID);
        	psTeamAddEntry.setLong(6, founder_id);
        	psTeamAddEntry.setLong(7,total_credit);
        	psTeamAddEntry.setLong(8,rac);
        	psTeamAddEntry.setLong(9,rac_time);
        	psTeamAddEntry.setInt(10,nusers);
        	psTeamAddEntry.setString(11,founder_name);
        	psTeamAddEntry.setLong(12,create_time);
        	psTeamAddEntry.setString(13, url);
        	psTeamAddEntry.setString(14,description);
        	psTeamAddEntry.setInt(15,country_id);
            
            int res = psTeamAddEntry.executeUpdate();
            if (res != 1) {
                log.debug("Number of rows updated is wrong: "+res);
            }

            return table_id;

        } catch (SQLException s) {            
            throw new SQLException("TeamAddEntry",s);
        }
    }
    
    
    public void TeamRead(Hashtable<String, Team> myTeams, String table) throws SQLException {
        
        myTeams.clear();
        System.gc();
        try { Thread.sleep(1000); }
        catch (Exception e) {}
        
        String query = "select team_id,b_cteam_id from "+table;
        Statement statement = null;
        ResultSet resultSet = null;
        
        try {            
            statement = cDBConnection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,java.sql.ResultSet.CONCUR_READ_ONLY);
            statement.setFetchSize(Integer.MIN_VALUE);

            
            resultSet = statement.executeQuery( query );

            if ( resultSet == null ) {
                throw new SQLException();
            }
            
            while( resultSet.next() )
            {
                Team t = new Team();                
                t.team_id = resultSet.getInt("team_id");
                t.b_cteam_id = resultSet.getInt("b_cteam_id");
                String i = (String) new Integer(t.team_id).toString();
                myTeams.put(i,t);
            }   
            resultSet.close();
            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("TeamRead() - " + s.toString());
        }        
    }    

    public void CTeamRead(Hashtable<String, CTeam> myCTeams) throws SQLException {
        
        myCTeams.clear();
        String query = "select table_id,name,team_cpid from cteams";
        Statement statement = null;
        ResultSet resultSet = null;
        
        try {            
            statement = cDBConnection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,java.sql.ResultSet.CONCUR_READ_ONLY);
            statement.setFetchSize(Integer.MIN_VALUE);

            
            resultSet = statement.executeQuery( query );

            if ( resultSet == null ) {
                throw new SQLException();
            }
            
            while( resultSet.next() )
            {
                CTeam t = new CTeam();
                t.table_id =  resultSet.getInt("table_id");
                t.name = resultSet.getString("name");
                t.team_cpid = resultSet.getString("team_cpid");
                
                if (t.team_cpid == null || t.team_cpid.length()==0) {
                	try {
  	                  MessageDigest m = MessageDigest.getInstance("MD5");
  	                  m.update(t.name.toLowerCase().getBytes(),0,t.name.length());
  	                  BigInteger bi = new BigInteger(1,m.digest());
  	                  t.team_cpid = bi.toString(16); 
                    }
                    catch (Exception e) {
                  	  log.error("Error Creating MD5 of team name: "+t.name,e);
                    }
                }
                myCTeams.put(t.team_cpid,t);
            }   
            resultSet.close();
            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("CTeamRead() - " + s.toString());
        }        
    }
    
    public CTeam CTeamGetTeamIDFromDB(String name) throws SQLException {
        
       
        Statement statement = null;
        ResultSet resultSet = null;
        CTeam ct = new CTeam();
        ct.table_id = 0;
        ct.team_cpid = "";
        ct.name = "";

        
        if (name.length() > 255) name = name.substring(0,254);
        name = name.replaceAll("\\\\","\\\\\\\\");
        name = name.replaceAll("'", "\\\\'");
        
        String query = "select table_id, name, team_cpid from cteams where name='"+name+"'";
        
        try {            
            statement = cDBConnection.createStatement();
            
            resultSet = statement.executeQuery( query );

            if ( resultSet == null ) {
                throw new SQLException();
            }
            
            while( resultSet.next() )
            {
                ct.table_id = resultSet.getInt("table_id");
                ct.name = resultSet.getString("name");
                ct.team_cpid = resultSet.getString("team_cpid");
            }     
            resultSet.close();
            statement.close();
            return ct;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("CTeamGetTeamIDFromDB() - " + s.toString());
        }        
    }
    
    public int CTeamAddEntry( String name, String team_cpid) throws SQLException {
        
        int table_id=0;
        if (name.length() > 255) name = name.substring(0,254);
        
        if (psCTeamAddEntry == null) {
        	psCTeamAddEntry = cDBConnection.prepareStatement("insert into cteams (name, team_cpid) values (?,?)");
        }
         
        try {
        	psCTeamAddEntry.setString(1,name);
        	psCTeamAddEntry.setString(2,team_cpid);

            int res = psCTeamAddEntry.executeUpdate();
            if (res != 1) {
                log.debug("Number of rows inserted is wrong: "+res);
            }

            ResultSet rs;
            rs = psCTeamAddEntry.getGeneratedKeys();
            while (rs.next())
                table_id = rs.getInt(1);
            rs.close();
            return table_id;
        } catch (SQLException s) {
            throw new SQLException("CTeamAddEntry()",s);
        }
    }
    

    public void CPIDRead(Hashtable<String, CPID> myCPIDs) throws SQLException {
        
        myCPIDs.clear();
        String query = "select b_cpid_id,user_cpid from cpid";
        Statement statement = null;
        ResultSet resultSet = null;
        
        try {            
            statement = cDBConnection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,java.sql.ResultSet.CONCUR_READ_ONLY);
            statement.setFetchSize(Integer.MIN_VALUE);

            
            resultSet = statement.executeQuery( query );

            if ( resultSet == null ) {
                throw new SQLException();
            }
            
            while( resultSet.next() )
            {
                CPID c = new CPID();
                c.b_cpid_id =  resultSet.getInt("b_cpid_id");                
                c.user_cpid = resultSet.getString("user_cpid");  
                myCPIDs.put(c.user_cpid,c);
            }     
            resultSet.close();
            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("CPIDRead() - " + s.toString());
        }        
    }
    
    public long CPIDGetIDFromDB(String cpid) throws SQLException {
        
        
        Statement statement = null;
        ResultSet resultSet = null;
        long b_cpid_id=0;
       
        String query = "select table_id from cpid where user_cpid='"+cpid+"'";
        
        try {            
            statement = cDBConnection.createStatement();
            
            resultSet = statement.executeQuery( query );

            if ( resultSet == null ) {
                throw new SQLException();
            }
            
            while( resultSet.next() )
            {
                b_cpid_id = resultSet.getLong("table_id");
            }     
            resultSet.close();
            statement.close();
            return b_cpid_id;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("CPIDGetIDFromDB() - " + s.toString());
        }        
    }
    
    public int CPIDAddEntry(String cpid, long total_credit, long rac, long rac_time) throws SQLException {
        
        Statement statement = null;
        int table_id=0;
        
        String query = "insert into cpid (user_cpid,total_credit,rac,rac_time,project_count) values ('"+
            cpid+"',"+total_credit+","+rac+","+rac_time+",1)";       
        
        try {           
            statement = cDBConnection.createStatement();
            int res = statement.executeUpdate(query);
            if (res != 1) {
                //RollbackTransaction(); // problems, should only be 1 row
                log.debug("Number of rows updated is wrong: "+res);
            }

            ResultSet rs;
            rs = statement.getGeneratedKeys();
            while (rs.next())
                table_id = rs.getInt(1);
            rs.close();
            // clean up
            statement.close();
            statement = null;

            //CommitTransaction();            
            return table_id;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("CPIDAddEntry - " + s.toString());
        }
    }
    
    
    public int CPIDChangeCPID(int table_id, String new_cpid) 
    throws SQLException {
    
        Statement statement = null;
        int updated_rows=0;
        
        String query = "update cpid set user_cpid='"+new_cpid+"' where table_id="+table_id;
        
        try {           
            statement = cDBConnection.createStatement();
            updated_rows = statement.executeUpdate(query);
            if (updated_rows != 1) {
                //RollbackTransaction(); // problems, should only be 1 row
                log.debug("Number of rows updated is wrong: "+updated_rows);
            }
    
            // clean up
            statement.close();
            statement = null;
    
            //CommitTransaction();            
            return updated_rows;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("CPIDChangeCPID - " + s.toString());
        }
    }
    
    
    public void AddToCPIDHistory(int project_id,int user_id,String old_cpid, String new_cpid) throws SQLException {
        
        if (psCPidHistory == null) {
        	psCPidHistory = cDBConnection.prepareStatement("insert into cpid_change_history (project_id,user_id,old_cpid,new_cpid,changetime) values (?,?,?,?,?)");
        }
         
        java.sql.Date now = new java.sql.Date(System.currentTimeMillis());
        
        try {
        	psCPidHistory.setLong(1,project_id);
        	psCPidHistory.setLong(2,user_id);
        	psCPidHistory.setString(3,old_cpid);
        	psCPidHistory.setString(4,new_cpid);
        	psCPidHistory.setDate(5,now);
        	
            int res = psCPidHistory.executeUpdate();
            if (res != 1) {
                log.debug("Number of rows updated is wrong: "+res);
            }
            return;
        } catch (SQLException s) {
            log.error("SQL Error",s);
            
            throw new SQLException("AddToCPIDHistory",s);
        }
    }
    
    public void AddToTeamHistory(int project_id,int user_id,long old_team, long new_team) throws SQLException {
        
        if (psTeamHistory == null) {
        	psTeamHistory = cDBConnection.prepareStatement("insert into team_change_history (project_id,user_id,old_team,new_team,changetime) values (?,?,?,?,?)");
        }
         
        java.sql.Date now = new java.sql.Date(System.currentTimeMillis());
        
        try {
        	psTeamHistory.setLong(1,project_id);
        	psTeamHistory.setLong(2,user_id);
        	psTeamHistory.setLong(3,old_team);
        	psTeamHistory.setLong(4,new_team);
        	psTeamHistory.setDate(5,now);
        	
            int res = psTeamHistory.executeUpdate();
            if (res != 1) {
                log.debug("Number of rows updated is wrong: "+res);
            }
            return;
        } catch (SQLException s) {
            log.error("SQL Error",s);
            
            throw new SQLException("AddToTeamHistory",s);
        }
    }

    public int ProjectUpdateUsersTime(int project_id, String updateTime) 
    throws SQLException {
    
        Statement statement = null;
        int updated_rows;
        
        String query = "update projects set last_update_users='"+updateTime+"' where project_id="+project_id;
        
        try {           
            statement = cDBConnection.createStatement();
            updated_rows = statement.executeUpdate(query);
            if (updated_rows != 1) {
                //RollbackTransaction(); // problems, should only be 1 row
                log.debug("Number of rows updated is wrong: "+updated_rows);
            }
    
            // clean up
            statement.close();
            statement = null;
    
            //CommitTransaction();            
            return updated_rows;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("ProjectUpdateTime() - " + s.toString());
        }
    }

    public int ProjectUpdateHostsTime(int project_id, String updateTime) 
    throws SQLException {
    
        Statement statement = null;
        int updated_rows;
        
        String query = "update projects set last_update_hosts='"+updateTime+"' where project_id="+project_id;
        
        try {           
            statement = cDBConnection.createStatement();
            updated_rows = statement.executeUpdate(query);
            if (updated_rows != 1) {
                //RollbackTransaction(); // problems, should only be 1 row
                log.debug("Number of rows updated is wrong: "+updated_rows);
            }
    
            // clean up
            statement.close();
            statement = null;
    
            //CommitTransaction();            
            return updated_rows;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("ProjectUpdateHostsTime() - " + s.toString());
        }
    }
    
    public int ProjectUpdateTeamsTime(int project_id, String updateTime) 
    throws SQLException {
    
        Statement statement = null;
        int updated_rows;
        
        String query = "update projects set last_update_teams='"+updateTime+"' where project_id="+project_id;
        
        try {           
            statement = cDBConnection.createStatement();
            updated_rows = statement.executeUpdate(query);
            if (updated_rows != 1) {
                //RollbackTransaction(); // problems, should only be 1 row
                log.debug("Number of rows updated is wrong: "+updated_rows);
            }
    
            // clean up
            statement.close();
            statement = null;
    
            //CommitTransaction();            
            return updated_rows;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("ProjectUpdateTeamsTime() - " + s.toString());
        }
    }    
    
    public String ProjectGetUserUpdateTime(int projectID) throws SQLException {
        
        
        Statement statement = null;
        ResultSet resultSet = null;
        String utime="";
       
        String query = "select last_update_users from projects where project_id="+projectID;
        
        try {            
            statement = cDBConnection.createStatement();
            
            resultSet = statement.executeQuery( query );

            if ( resultSet == null ) {
                throw new SQLException();
            }
            
            while( resultSet.next() )
            {
            	
            	Object o = resultSet.getObject("last_update_users");
            	if (o == null)          	
            		utime = "0000-00-00 00:00:00";
            	else
            		utime = resultSet.getString("last_update_users");
            }     
            resultSet.close();
            statement.close();
            return utime;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("ProjectGetUserUpdateTime()", s);
        }        
    }
    
    public String ProjectGetHostUpdateTime(int projectID) throws SQLException {
        
        
        Statement statement = null;
        ResultSet resultSet = null;
        String utime="";
       
        String query = "select last_update_hosts from projects where project_id="+projectID;
        
        try {            
            statement = cDBConnection.createStatement();
            
            resultSet = statement.executeQuery( query );

            if ( resultSet == null ) {
                throw new SQLException();
            }
            
            while( resultSet.next() )
            {
            	Object o = resultSet.getObject("last_update_hosts");
            	if (o == null)        	
            		utime = "0000-00-00 00:00:00";
            	else
            		utime = resultSet.getString("last_update_hosts");
            }     
            resultSet.close();
            statement.close();
            return utime;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("CPIDGetIDFromDB() - " + s.toString());
        }        
    }
    
    public String ProjectGetTeamUpdateTime(int projectID) throws SQLException {
        
        
        Statement statement = null;
        ResultSet resultSet = null;
        String utime="";
       
        String query = "select last_update_teams from projects where project_id="+projectID;
        
        try {            
            statement = cDBConnection.createStatement();
            
            resultSet = statement.executeQuery( query );

            if ( resultSet == null ) {
                throw new SQLException();
            }
            
            while( resultSet.next() )
            {
            	Object o = resultSet.getObject("last_update_teams");
            	if (o == null)
            		utime = "0000-00-00 00:00:00";
            	else
            		utime = resultSet.getString("last_update_teams");
            }     
            resultSet.close();
            statement.close();
            return utime;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("CPIDGetIDFromDB() - " + s.toString());
        }        
    }
    
    public void ProjectsRead(Vector<Project> myProjects) throws SQLException {
        
        myProjects.clear();
        String query = "select project_id,name,host_file,user_file,team_file,shortname,data_dir,active,retired,gr from projects where active='Y' || retired='Y'";
        
        Statement statement = null;
        ResultSet resultSet = null;
        
        try {            
            statement = cDBConnection.createStatement();
            
            resultSet = statement.executeQuery( query );

            if ( resultSet == null ) {
                throw new SQLException();
            }
            
            while( resultSet.next() )
            {
                Project p = new Project();
                p.project_id =  resultSet.getInt("project_id");
                p.name = resultSet.getString("name");
                p.host_file = resultSet.getString("host_file");
                p.user_file = resultSet.getString("user_file");
                p.team_file = resultSet.getString("team_file");
                p.shortname = resultSet.getString("shortname");
                p.data_dir = resultSet.getString("data_dir");
                String t;
                t = resultSet.getString("active");
                if (t.compareToIgnoreCase("Y")==0)
                    p.active = true;
                else
                    p.active = false;
                t = resultSet.getString("retired");
                if (t.compareToIgnoreCase("Y")==0)
                    p.retired = true;
                else
                    p.retired = false;
                t = resultSet.getString("gr");
                if (t.compareToIgnoreCase("Y")==0)
                    p.gr = true;
                else
                    p.gr = false;
                myProjects.add(p);

            }
            resultSet.close();
            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("ReadProjects() - " + s.toString());
        }        
    }    
    
    public void DoProjectUserRankings (String tablename) throws SQLException {
        
        String query = "set @rank=0";
        String query2 ="update "+tablename+" set project_rank_credit = @rank := @rank+1 order by total_credit desc;";
        String query3 ="update "+tablename+" set project_rank_rac = @rank := @rank+1 order by rac desc;";
        String query4 ="set @rank := 0, @pos := 0, @teamid := null, @tc := null;";
        String query5 = "update "+tablename+" set team_rank_credit = greatest("+
		      "@rank := if(@teamid = teamid and @tc = total_credit, @rank, if(@teamid <> teamid, 1, @rank + 1)),"+
		      "least(0, @pos   := if(@teamid = teamid, @pos + 1, 1)),"+
		      "least(0, @tc := total_credit), least(0, @teamid  := teamid)) "+
		      "where teamid <> 0 order by teamid desc, total_credit desc;";
        String query6 = "update "+tablename+" set team_rank_rac = greatest("+
	      "@rank := if(@teamid = teamid and @tc = rac, @rank, if(@teamid <> teamid, 1, @rank + 1)),"+
	      "least(0, @pos   := if(@teamid = teamid, @pos + 1, 1)),"+
	      "least(0, @tc := rac), least(0, @teamid  := teamid)) "+
	      "where teamid <> 0 order by teamid desc, rac desc;";
        
        Statement statement = null;

        
        try {            
            statement = cDBConnection.createStatement();
            
            statement.execute(query);
            statement.execute(query2);
            statement.execute(query);
            statement.execute(query3);
            statement.execute(query4);
            statement.execute(query5);
            statement.execute(query4);
            statement.execute(query6);
            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("DoProjectUserRankings() - " + s.toString());
        }                
    }
    
    public void DoProjectHostRankings (String tablename) throws SQLException {
        
        String query = "set @rank=0";
        String query2 ="update "+tablename+" set project_rank_credit = @rank := @rank+1 order by total_credit desc;";
        String query3 ="update "+tablename+" set project_rank_rac = @rank := @rank+1 order by rac desc;";
        Statement statement = null;
        
        try {            
            statement = cDBConnection.createStatement();
            
            statement.execute(query);
            statement.execute(query2);
            statement.execute(query);
            statement.execute(query3);
            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("DoProjectHostRankings() - " + s.toString());
        }                
    }
    
    public void DoProjectTeamRankings (String tablename) throws SQLException {
        
        String query = "set @rank=0";
        String query2 ="update "+tablename+" set project_rank_credit = @rank := @rank+1 order by total_credit desc;";
        String query3 ="update "+tablename+" set project_rank_rac = @rank := @rank+1 order by rac desc;";
        Statement statement = null;
        
        try {            
            statement = cDBConnection.createStatement();
            
            statement.execute(query);
            statement.execute(query2);
            statement.execute(query);
            statement.execute(query3);
            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("DoProjectTeamRankings() - " + s.toString());
        }                
    }
    
    public void DoProjectStats (int project_id, String usertable, String hosttable, String teamtable) throws SQLException {
        
        String query1 = "select count(*) as cnt from "+usertable;
        String query2 = "select count(*) as cnt from "+usertable+" where rac > 0";
        String query3 = "select count(*) as cnt from "+hosttable;
        String query4 = "select count(*) as cnt from "+hosttable+" where rac > 0";
        String query5 = "select count(*) as cnt from "+teamtable;
        String query6 = "select count(*) as cnt from "+teamtable+" where rac > 0";
        String query7 = "select count(*) as cnt from (select distinct country_id from "+usertable+") a";
        String query8 = "select sum(total_credit) as tc from "+usertable;
        String query9 = "select sum(rac) as rac from "+usertable;
        String query10 = "update projects set user_count=?, host_count=?, team_count=?, active_users=?,active_hosts=?,active_teams=?,country_count=?,total_credit=?,rac=? where project_id=?";
        
     
        PreparedStatement ps = null;

        try {            

        	log.debug("Getting user counts");
        	long usercount = RunCountQuery(query1, "cnt");
        	long activeusercount = RunCountQuery(query2, "cnt");
        	log.debug("Getting host counts");
        	long hostcount = RunCountQuery(query3, "cnt");
        	long activehostcount = RunCountQuery(query4, "cnt");
        	log.debug("Getting team counts");
        	long teamcount = RunCountQuery(query5, "cnt");
        	long activeteamcount = RunCountQuery(query6, "cnt");
        	long country_count = RunCountQuery(query7,"cnt");
        	long total_credit = RunCountQuery(query8,"tc");
        	long rac = RunCountQuery(query9,"rac");
        	
        	log.debug("Updating projects table");
        	ps = cDBConnection.prepareStatement(query10);
        	ps.setLong(1, usercount);
        	ps.setLong(2, hostcount);
        	ps.setLong(3, teamcount);
        	ps.setLong(4, activeusercount);
        	ps.setLong(5, activehostcount);
        	ps.setLong(6, activeteamcount);
        	ps.setLong(7, country_count);
        	ps.setLong(8, total_credit);
        	ps.setLong(9, rac);
        	ps.setInt(10, project_id);
        	
        	ps.executeUpdate();
        	return;
        } catch (SQLException s) {
            throw new SQLException("DoProjectStats()",s);
        }                
    }
    
    
    public void DoUpdateComputerCounts (String usersTable, String hostsTable) throws SQLException {
        
        String query1 ="update "+usersTable+",(select user_id,count(*) as cnt from "+hostsTable+" where user_id > 0 group by user_id) cc set "+usersTable+".computer_count = cc.cnt where "+usersTable+".user_id=cc.user_id";
        String query2 ="update "+usersTable+",(select user_id,count(*) as cnt from "+hostsTable+" where user_id > 0 and rac > 0 group by user_id) cc set "+usersTable+".active_computer_count = cc.cnt where "+usersTable+".user_id=cc.user_id";
        
        Statement statement = null;

        
        try {            
            statement = cDBConnection.createStatement();

            statement.execute(query1);
            statement.execute(query2);
            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("DoUpdateComputerCounts() - " + s.toString());
        }                
    }
    

    public void DoUpdateTeamUserCounts (String userTable, String teamTable) throws SQLException {
        
        String query ="update "+teamTable+",(select teamid,count(*) as cnt from "+userTable+" where  teamid > 0 group by teamid) cc set "+teamTable+".nusers = cc.cnt where "+teamTable+".team_id=cc.teamid";
        
        Statement statement = null;

        
        try {            
            statement = cDBConnection.createStatement();

            statement.execute(query);
            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("DoUpdateTeamUserCounts() - " + s.toString());
        }                
    }


    
    public int  GetCurrentDay(String key) throws SQLException {
        
        int day_of_update=0;
        String query = "select value from b_currentday where key_item='"+key+"'";
        
        Statement statement = null;
        ResultSet resultSet = null;
        
        try {            
            statement = cDBConnection.createStatement();
            
            resultSet = statement.executeQuery( query );

            if ( resultSet == null ) {
                throw new SQLException();
            }
            
            while( resultSet.next() )
            {
                day_of_update =  resultSet.getInt("value");
            }     
            resultSet.close();
            statement.close();
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("GetCurrentDay() - " + s.toString());
        }      
        return day_of_update;
    }
    
    public int SetCurrentDay(String key, int day_of_update) throws SQLException {
        Statement statement = null;
        int updated_rows=0;
        
        try {            
                       
            String query = "update b_currentday set value="+day_of_update+" where key_item='"+key+"'";
        
            statement = cDBConnection.createStatement();
            updated_rows = statement.executeUpdate(query);
            if (updated_rows != 1) {
                //RollbackTransaction(); // problems, should only be 1 row
                log.debug("Number of rows updated is wrong: "+updated_rows);
            }

            // clean up
            statement.close();
            statement = null;         
            return updated_rows;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("SetCurrentDay() - " + s.toString());
        }        
    }    
    
    public String EscapeXMLChars(String text) {
    	String test = text;
    	    	
    	test = test.replaceAll("&quot;", "\"");
    	test = test.replaceAll("&lt;","<");
    	test = test.replaceAll("&gt;",">");
    	test = test.replaceAll("&apos;","'");
    	test = test.replaceAll("&amp;", "&");
    	
    	test = test.replaceAll("&", "&amp;");
    	test = test.replaceAll("<","&lt;");
    	test = test.replaceAll(">", "&gt;");
    	test = test.replaceAll("'", "&apos;");
    	test = test.replaceAll("\"", "&quot;");
    	return test;    	
    }
    
    
    public String MakeSafe(String text) {
    	String test = text;
    	    	
    	test = test.replaceAll("\\\\","\\\\\\\\");
    	test = test.replaceAll("'", "\\\\'");
    	test = test.replaceAll("\"", "\\\"");
    	test = test.replaceAll("\r", "\\\\r");
    	test = test.replaceAll("\n","\\\\n");
    	test = test.replaceAll("\t", "\\\\t");

    	return test;    	
    }
    
    public int RunQuery (String query) throws SQLException {
 
        Statement statement = null;
        int updated_rows = 0;
        try {            
            statement = cDBConnection.createStatement();
            
            log.debug("Running generic query ["+query+"]");
            //statement.execute(query);
            statement.executeUpdate(query);
            updated_rows = statement.getUpdateCount();
            log.debug("Query updated/added "+updated_rows+" rows");
            // clean up
            statement.close();
            statement = null;
    
            //CommitTransaction();            
            return updated_rows;
            
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("RunQuery() - " + s.toString());
        }                
    }
    
    public long RunCountQuery (String query, String columnName) throws SQLException {
    	 
        Statement statement = null;
        ResultSet resultSet = null;
        long count = 0;
        try {            
            statement = cDBConnection.createStatement();
            
            log.debug("Running generic query ["+query+"]");
            //statement.execute(query);
            resultSet = statement.executeQuery(query);
            if ( resultSet == null ) {
                throw new SQLException();
            }
            
            while( resultSet.next() )
            {
                count =  resultSet.getLong(columnName);
            }
            // clean up
            statement.close();
            statement = null;
    
                        
            return count;
            
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("RunQuery() - " + s.toString());
        }                
    }
    
    public void CreateUsersTable (String projectShortName) throws SQLException {
    
    	String query = "CREATE TABLE users_"+projectShortName;
    	query += " (`user_id` bigint(20) NOT NULL default '0',`b_cpid_id` bigint(20) NOT NULL default '0',"+
    	  "`name` varchar(255) default NULL,`country_id` smallint(6) default NULL,"+
    	  "`create_time` int(11) NOT NULL default '0',`user_cpid` varchar(50) default NULL,"+
    	  "`url` varchar(200) default NULL,`teamid` int(11) default NULL,"+
    	  "`b_cteam_id` bigint(20) NOT NULL default '0',"+
    	  "`total_credit` bigint(20) NOT NULL default '0',`rac` bigint(20) NOT NULL default '0',"+
    	  "`rac_time` bigint(20) default NULL,`project_rank_credit` int(11) NOT NULL default '0',"+
    	  "`project_rank_rac` int(11) NOT NULL default '0',`team_rank_credit` int(11) NOT NULL default '0',"+
    	  "`team_rank_rac` int(11) NOT NULL default '0',`computer_count` int(11) NOT NULL default '0',"+
    	  "`active_computer_count` int(11) NOT NULL default '0', PRIMARY KEY  (`user_id`),"+
    	  "KEY `teamid`(`teamid`)) ENGINE=MyISAM DEFAULT CHARSET=utf8;";

    	Statement statement = null;
    	try {   
    		
            statement = cDBConnection.createStatement();
            
            log.debug("Creating table users_"+projectShortName);
            //statement.execute(query);
            statement.execute(query);
            
            // clean up
            statement.close();
            statement = null;
    
            //CommitTransaction();            
            return;            
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("RunQuery() - " + s.toString());
        }
    }
    
    public void CreateHostsTable (String projectShortName) throws SQLException {
        
    	String query = "CREATE TABLE hosts_"+projectShortName;
    	query += " (`host_id` bigint(20) NOT NULL default '0',`user_id` bigint(20) default '0',"+
		  "`b_cpid_id` bigint(20) NOT NULL default '0',"+
		  "`p_vendor` smallint(6) NOT NULL default '0',`p_model` smallint(6) NOT NULL default '0',"+
		  "`os_name` smallint(6) NOT NULL default '0',`os_version` varchar(100) default NULL,"+
		  "`create_time` int(11) NOT NULL default '0',`rpc_time` int(11) NOT NULL default '0',"+
		  "`timezone` mediumint(8) NOT NULL default '0',`ncpus` smallint(6) NOT NULL default '0',"+
		  "`p_fpops` double NOT NULL default '0',`p_iops` double NOT NULL default '0',"+
		  "`p_membw` double NOT NULL default '0',`m_nbytes` double NOT NULL default '0',"+
		  "`m_cache` double NOT NULL default '0',`m_swap` double NOT NULL default '0',"+
		  "`d_total` double NOT NULL default '0',`d_free` double NOT NULL default '0',"+
		  "`n_bwup` double NOT NULL default '0',`n_bwdown` double NOT NULL default '0',"+
		  "`avg_turnaround` double NOT NULL default '0',`host_cpid` varchar(32) default NULL,"+
		  "`total_credit` bigint(20) NOT NULL default '0',`rac` bigint(20) NOT NULL default '0',"+
		  "`rac_time` bigint(20) NOT NULL default '0',`credit_per_cpu_sec` double NOT NULL default '0',"+
		  "`project_rank_credit` bigint(20) NOT NULL default '0',`project_rank_rac` bigint(20) NOT NULL default '0',"+
		  "PRIMARY KEY  (`host_id`),"+
		  "KEY `user_id`(`user_id`), KEY `b_cpid_id`(`b_cpid_id`)"+
		  ") ENGINE=MyISAM DEFAULT CHARSET=utf8;";
  	
    	
    	Statement statement = null;
    	try {   
    		
            statement = cDBConnection.createStatement();
            
            log.debug("Creating table hosts_"+projectShortName);
            //statement.execute(query);
            statement.execute(query);
            
            // clean up
            statement.close();
            statement = null;         
            return;            
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("RunQuery() - " + s.toString());
        }
    }
    
    public void CreateTeamsTable (String projectShortName) throws SQLException {
        
    	String query = "CREATE TABLE teams_"+projectShortName;
    	query += " (`team_id` bigint(20) NOT NULL default '0',`b_cteam_id` bigint(20) NOT NULL default '0',"+
		  "`type` smallint(6) NOT NULL default '0',`name` varchar(255) NOT NULL default '',"+
		  "`team_cpid` varchar(50), `founder_id` bigint(20) NOT NULL default '0',"+
		  "`total_credit` bigint(20) NOT NULL default '0',`rac` bigint(20) NOT NULL default '0',"+
		  "`rac_time` bigint(20) NOT NULL default '0',`nusers` int(11) NOT NULL default '0',"+
		  "`founder_name` varchar(255) NOT NULL default '',`create_time` int(11) NOT NULL default '0',"+
		  "`url` varchar(250) NOT NULL default '',`description` text NOT NULL,"+
		  "`country_id` smallint(6) NOT NULL default '0',`project_rank_credit` int(11) NOT NULL default '0',"+
		  "`project_rank_rac` int(11) NOT NULL default '0',PRIMARY KEY  (`team_id`),"+
		  "KEY `name` (`name`)) DEFAULT CHARSET=utf8;";
    	
    	Statement statement = null;
    	try {   
    		
            statement = cDBConnection.createStatement();
            
            log.debug("Creating table teams_"+projectShortName);
            //statement.execute(query);
            statement.execute(query);
            
            // clean up
            statement.close();
            statement = null;
    
            //CommitTransaction();            
            return;            
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("RunQuery() - " + s.toString());
        }
    }


    public void RenameTable (String oldName, String newName) throws SQLException {
        
    	String query = "ALTER TABLE `"+oldName+"` RENAME TO `"+newName+"`;";
    	
    	Statement statement = null;
    	try {   
    		
            statement = cDBConnection.createStatement();
            
            log.debug("Renaming table from "+oldName+" to "+newName);
            //statement.execute(query);
            statement.execute(query);
            
            // clean up
            statement.close();
            statement = null;
    
            //CommitTransaction();            
            return;            
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("RenameTable()",s);
        }
    }
    
    public void DropTable (String table) throws SQLException {
        
    	String query = "DROP TABLE `"+table;
    	
    	Statement statement = null;
    	try {   
    		
            statement = cDBConnection.createStatement();
            
            log.debug("dropping table "+table);
            //statement.execute(query);
            statement.execute(query);
            
            // clean up
            statement.close();
            statement = null;
    
            //CommitTransaction();            
            return;            
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("DropTable()",s);
        }
    }    
}
