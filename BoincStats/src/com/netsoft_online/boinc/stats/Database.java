package com.netsoft_online.boinc.stats;
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

import java.security.MessageDigest;
import java.sql.*;
import java.util.*;
import java.util.Date;

import org.apache.log4j.Logger;
import java.util.zip.*;
import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;

public class Database {

    String szConnectString = null;
    String szDBDriver = null;
    String szLogin = null;
    String szPassword = null;
    boolean bConnected = false;
    static Logger log = Logger.getLogger(Database.class.getName());  // log4j stuff
    
    private Connection cDBConnection;
    private PreparedStatement psCPIDAddEntry=null;
    private String sCPIDAddEntryTable=null;
    private PreparedStatement psCTeamAddEntry=null;
    private String sCTeamAddEntryTable=null;

    
    public Database()
    {
      bConnected = false;
    }
    
    private double DecayRac(double current_rac, double rac_time, double racComputeTime) {
        double M_LN2 = 0.693147180559945309417;
        double credit_half_life = 86400 * 7;
        
        double avg_time = rac_time;
        double avg = current_rac;
        
        if (racComputeTime == 0) {
            // get current time
            racComputeTime = System.currentTimeMillis() / 1000;
        }

        double diff = racComputeTime - avg_time;
        double weight = java.lang.Math.exp(-diff * M_LN2/credit_half_life);
        double avgr = avg * weight;
        
        if (avgr < 0.009) avgr = 0.0;
        return avgr;      
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
    

    public void ReadCountryTable (Hashtable<String, Integer> myCountries) throws SQLException{
        
        String query = "select country_id, country from b_country";
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
                Integer country_id = new Integer(( (int) resultSet.getInt("country_id")));
                String country = ( (String) resultSet.getString( "country" ) );  
                
                myCountries.put(country.toLowerCase(),country_id);
            }   
            resultSet.close();
            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("ReadCountryTable",s);
        }
    }
    
    public long ProjectUserRead(String tablename, Hashtable<String, CPID> hCpid, double newRacTime) throws SQLException {
        
    	long count=0;
        String query = "select user_cpid,b_cpid_id,name,country_id,create_time,total_credit,rac,rac_time,computer_count from "+tablename;
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
            	count++;
            	if (count%100==0) log.debug("Read "+count+" entries so far");
            	long b_cpid_id = 0;
            	String user_cpid = resultSet.getString("user_cpid");
            	b_cpid_id = resultSet.getLong("b_cpid_id");
            	String name = resultSet.getString("name");
            	int country_id = resultSet.getInt("country_id");
            	int create_time = resultSet.getInt("create_time");
                double total_credit = resultSet.getDouble("total_credit");
                double rac = resultSet.getDouble("rac");
                double rac_time = resultSet.getDouble("rac_time"); 
                int computer_count = resultSet.getInt("computer_count");
                
                double newRac = DecayRac(rac,rac_time,newRacTime);
                Double d;
                d = new Double(newRacTime);
                
                // find entry in hash
                CPID myEntry = hCpid.get(user_cpid);
                if (myEntry == null) {
                	log.warn("Failed to locate cpid "+user_cpid+" in hash");
                	myEntry = new CPID();
                	if (b_cpid_id != 0)
                		myEntry.b_cpid_id = b_cpid_id;
                	else
                		myEntry.b_cpid_id = 0;
                	myEntry.hosts_visible = "Y";
                	myEntry.user_cpid = user_cpid;
                	hCpid.put(user_cpid, myEntry);
                } 
            	myEntry.name = name; 
            	myEntry.project_count++;
            	myEntry.total_credit += total_credit;
            	myEntry.rac_time = d.longValue();
            	if (newRac > 0) {
            		myEntry.active_project_count++;
            		myEntry.rac += newRac;
            	}
            	if (myEntry.create_time == 0)
            		myEntry.create_time = create_time;
            	
            	if (create_time < myEntry.create_time && create_time != 0) {
            		myEntry.create_time = create_time;
            	}
            	if (country_id != 0) {
            		if(myEntry.country_id == 0) {
            			myEntry.country_id = country_id;
            		} else if (myEntry.country_id != -1 && myEntry.country_id != country_id) {
            			// mark as "international"
            			myEntry.country_id = -1;
            		}
            	}
            	if (computer_count == 0)
            		myEntry.hosts_visible = "N";
            } 
            
            resultSet.close();
            statement.close();
            return count;
        } catch (SQLException s) {
        	log.error("SQL Exception",s);
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("ProjectUserRead()",s);
        }       
        catch (Exception e) {
        	log.error("Exception",e);
        	if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("ProjectUserRead()",e);
        }
    }
    
    public long ProjectTeamRead(String tablename, Hashtable<String, CTeam> hCTeam, double newRacTime) throws SQLException {
        
    	long count=0;
        String query = "select name,b_cteam_id,team_cpid,country_id,create_time,total_credit,rac,rac_time from "+tablename;
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
            	count++;
            	long b_cteam_id=0;
            	String name = resultSet.getString("name");
            	b_cteam_id = resultSet.getLong("b_cteam_id");
            	String team_cpid = resultSet.getString("team_cpid");            	
            	int country_id = resultSet.getInt("country_id");
            	int create_time = resultSet.getInt("create_time");
                double total_credit = resultSet.getDouble("total_credit");
                double rac = resultSet.getDouble("rac");
                double rac_time = resultSet.getDouble("rac_time"); 
                
                double newRac = DecayRac(rac,rac_time,newRacTime);
                Double d;
                d = new Double(newRacTime);
                
                // find entry in hash
                CTeam myEntry = hCTeam.get(team_cpid);
                if (myEntry == null) {
                	log.warn("Failed to locate team_cpid "+team_cpid+"("+name+") in hash");
                	myEntry = new CTeam();
                	myEntry.team_cpid = team_cpid;
                	if (b_cteam_id != 0)
                		myEntry.b_cteam_id = b_cteam_id;
                	else 
                		myEntry.b_cteam_id = 0;
                	hCTeam.put(team_cpid,myEntry);
                } 
            	myEntry.name = name; 
            	myEntry.project_count++;
            	myEntry.total_credit += total_credit;
            	myEntry.rac_time = d.longValue();
            	if (newRac > 0) {
            		myEntry.rac += newRac;
            		myEntry.active_project_count++;
            	}
            	if (country_id != 0) {
            		if(myEntry.country_id == 0) {
            			myEntry.country_id = country_id;
            		} else if (myEntry.country_id != -1 && myEntry.country_id != country_id) {
            			// mark as "international"
            			myEntry.country_id = -1;
            		}
                }
            	if (myEntry.create_time == 0 || create_time < myEntry.create_time ) {
            		myEntry.create_time = create_time;
            	}
            } 
            
            resultSet.close();
            statement.close();
            return count;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("ProjectTeamRead()",s);
        }     
	    catch (Exception e) {
	        if (statement != null) {
	            statement.close();
	            statement = null;
	        }
	        throw new SQLException("ProjectTeamRead()",e);
	    }
    }
    
    public int MapUserTableInsert(String tablename, String projectname, int project_id) 
    throws SQLException {
    
        Statement statement = null;
        int updated_rows=0;
        
        String query = "insert ignore into "+tablename+" (project_id,user_id, b_cpid_id, user_cpid, team_id, b_cteam_id,user_name,total_credit) (select "+project_id+",user_id, b_cpid_id, user_cpid, teamid, b_cteam_id,name,total_credit from users_"+projectname+")";
        
        try {           
            statement = cDBConnection.createStatement();
            updated_rows = statement.executeUpdate(query);
            statement.close();
            statement = null;
          
            return updated_rows;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("MapUserTableInsert()", s);
        }
    }

    public int MapHostTableInsert(String tablename, String projectname, int project_id) 
    throws SQLException {
    
        Statement statement = null;
        int updated_rows=0;
        
        String query = "insert ignore into "+tablename+" (project_id,host_id,host_cpid,user_id, b_cpid_id, credit_per_cpu_sec) (select "+project_id+",host_id, host_cpid,user_id, b_cpid_id, credit_per_cpu_sec from hosts_"+projectname+" where rac > 0 and host_cpid <> '')";
        
        try {           
            statement = cDBConnection.createStatement();
            updated_rows = statement.executeUpdate(query);
            statement.close();
            statement = null;
          
            return updated_rows;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("MapHostTableInsert()", s);
        }
    }
    
    public long CTeamRead(Hashtable<String, CTeam> myCTeams) throws SQLException {
        
        myCTeams.clear();
        String query = "select table_id,name,team_cpid from cteams";
        Statement statement = null;
        ResultSet resultSet = null;
        long maxid=0;
        
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
                t.b_cteam_id =  resultSet.getInt("table_id");
                if (t.b_cteam_id > maxid)
                	maxid = t.b_cteam_id;
                t.name = resultSet.getString("name");
                t.project_count = 0;
                t.active_project_count = 0;
                t.rac = 0;
                t.rac_time = 0;
                t.team_cpid = resultSet.getString("team_cpid");;
                t.total_credit = 0;
                t.country_id = 0;
                
                if (t.team_cpid == null || t.team_cpid.length() == 0 || t.team_cpid.compareTo("")==0) {
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
            return maxid;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("CTeamRead()",s);
        } 
        catch (Exception e) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("CTeamRead()",e);
        } 
    }
    
    public int CTeamGetTeamIDFromDB(String name) throws SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        int teamid=0;
        
        if (name.length() > 255) name = name.substring(0,254);
        name = MakeSafe(name);
        
        String query = "select table_id from cteams where name='"+name+"'";
        
        try {            
            statement = cDBConnection.createStatement();
            
            resultSet = statement.executeQuery( query );

            if ( resultSet == null ) {
                throw new SQLException();
            }
            
            while( resultSet.next() )
            {
                teamid = resultSet.getInt("table_id");
            }     
            resultSet.close();
            statement.close();
            return teamid;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("CTeamGetTeamIDFromDB()",s);
        }
        catch (Exception e) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("CTeamGetTeamIDFromDB()",e);
        }        
    }
    
    public void CTeamAddEntry(String tablename, long table_id, String cteam_id, String name, int nusers, long total_credit, long rac, long rac_time, int project_count, int active_project_count, int country_id, int create_time) throws SQLException {
        
    	if (psCTeamAddEntry == null || sCTeamAddEntryTable.compareToIgnoreCase(tablename) != 0) {
        	if (psCTeamAddEntry != null) psCTeamAddEntry.close();
        	psCTeamAddEntry = cDBConnection.prepareStatement("insert ignore into "+tablename+" (table_id, team_cpid, name, nusers, total_credit, rac, rac_time, project_count, active_project_count,country_id, create_time) values (?,?,?,?,?,?,?,?,?,?,?)");
        	sCTeamAddEntryTable = tablename;
        }
        
        try {
        	psCTeamAddEntry.setLong(1,table_id);
        	psCTeamAddEntry.setString(2, cteam_id);
        	psCTeamAddEntry.setString(3, name);
        	psCTeamAddEntry.setInt(4, nusers);
        	psCTeamAddEntry.setLong(5,total_credit);
        	psCTeamAddEntry.setLong(6,rac);
        	psCTeamAddEntry.setLong(7,rac_time);
        	psCTeamAddEntry.setInt(8, project_count);
        	psCTeamAddEntry.setInt(9, active_project_count);
        	psCTeamAddEntry.setInt(10, country_id);
        	psCTeamAddEntry.setInt(11,create_time);
        	
        	int res = psCTeamAddEntry.executeUpdate();
            if (res != 1) {
                log.debug("Number of rows inserted is wrong: "+res);
            }
            return;
        } catch (SQLException s) {
        	psCTeamAddEntry.close();
        	psCTeamAddEntry = null;
        	sCTeamAddEntryTable="";
            throw new SQLException("CTeamAddEntry",s);
        }
        catch (Exception e) {
        	psCTeamAddEntry.close();
        	psCTeamAddEntry = null;
        	sCTeamAddEntryTable="";
            throw new SQLException("CTeamAddEntry",e);
        }
    }

    public long CPIDRead(Hashtable<String, CPID> myCPIDs) throws SQLException {
        
    	long maxid=0;
        myCPIDs.clear();
        String query = "select b_cpid_id,user_cpid,user_name from cpid";
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
                if (c.b_cpid_id > maxid)
                	maxid = c.b_cpid_id;
                c.user_cpid = resultSet.getString("user_cpid");  
                c.name = resultSet.getString("user_name");
                c.active_project_count = 0;
                c.create_time = 0;
                c.project_count = 0;
                c.rac = 0;
                c.rac_time = 0;
                c.total_credit = 0;
                c.country_id = 0;
                c.hosts_visible = "Y";
                myCPIDs.put(c.user_cpid,c);
            }     
            resultSet.close();
            statement.close();
            return maxid;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("CPIDRead()",s);
        }
        catch (Exception e) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("CPIDRead()",e);
        } 
    }
    
    public void CPIDAddEntry(String tablename, long b_cpid_id, String user_cpid, String user_name, int create_time, int country_id, int project_count, int active_project_count, int active_computer_count, long total_credit, long rac, long rac_time, String hostsShown) throws SQLException {
        
        if (psCPIDAddEntry == null || sCPIDAddEntryTable.compareToIgnoreCase(tablename) != 0) {
        	if (psCPIDAddEntry != null) psCPIDAddEntry.close();
        	psCPIDAddEntry = cDBConnection.prepareStatement("insert ignore into "+tablename+" (b_cpid_id, user_cpid, user_name, join_date, country_id, project_count, active_project_count, active_computer_count, total_credit, rac, rac_time,hosts_visible) values (?,?,?,?,?,?,?,?,?,?,?,?)");
        	sCPIDAddEntryTable = tablename;
        }
        
        String join_date;
        if (create_time > 0) {
	        long timestamp = (long)create_time * (long)1000; 
	        Date when = new Date(timestamp);
	        java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
	        sdf.setTimeZone(TimeZone.getDefault());
	        join_date = sdf.format(when);
        } else {
        	join_date = "0000-00-00";
        }
        try {
        	psCPIDAddEntry.setLong(1,b_cpid_id);
        	psCPIDAddEntry.setString(2, user_cpid);
        	psCPIDAddEntry.setString(3, user_name);
        	psCPIDAddEntry.setString(4, join_date);
        	psCPIDAddEntry.setInt(5, country_id);
        	psCPIDAddEntry.setInt(6, project_count);
        	psCPIDAddEntry.setInt(7, active_project_count);
        	psCPIDAddEntry.setInt(8, active_computer_count);
        	psCPIDAddEntry.setLong(9,total_credit);
        	psCPIDAddEntry.setLong(10,rac);
        	psCPIDAddEntry.setLong(11,rac_time);
        	psCPIDAddEntry.setString(12,hostsShown);
        	
        	int res = psCPIDAddEntry.executeUpdate();
            if (res != 1) {
                log.debug("Number of rows inserted is wrong: "+res);
            }

            return;
        } catch (SQLException s) {
        	psCPIDAddEntry.close();
        	psCPIDAddEntry = null;
        	sCPIDAddEntryTable = "";
            throw new SQLException("CPIDAddEntry()",s);
        }
        catch (Exception e) {
        	psCPIDAddEntry.close();
        	psCPIDAddEntry = null;
        	sCPIDAddEntryTable = "";
            throw new SQLException("CPIDAddEntry()",e);
        }
    }
    
    public int CPIDUpdateHostCounts(String cpidTable, String hostTable) 
    throws SQLException {
    
        Statement statement = null;
        int updated_rows=0;
        
        String query = "update "+cpidTable+" a,(select b_cpid_id, count(*) as cnt from (select distinct b_cpid_id, host_cpid from "+hostTable+" where b_cpid_id > 0) c group by b_cpid_id) b "+
        	"set a.active_computer_count = b.cnt where a.b_cpid_id=b.b_cpid_id and a.hosts_visible='Y'";

        
        try {           
            statement = cDBConnection.createStatement();
            updated_rows = statement.executeUpdate(query);
    
            // clean up
            statement.close();
            statement = null;
            return updated_rows;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("CPIDUpdateHostCounts()",s);
        }
        catch (Exception e) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("CPIDUpdateHostCounts()",e);
        }
    }
    
    public int CTeamUpdateUserCounts(String cteamTable, String cpidTable) 
    throws SQLException {
    
        Statement statement = null;
        int updated_rows=0;
        
        String query = "update "+cteamTable+" a, (select b_cteam_id, count(*) as cnt from (select distinct b_cteam_id, b_cpid_id from "+cpidTable+" where b_cteam_id <> 0) a group by b_cteam_id) b "+
        	"set a.nusers=b.cnt where a.table_id=b.b_cteam_id";

        
        try {           
            statement = cDBConnection.createStatement();
            updated_rows = statement.executeUpdate(query);
    
            // clean up
            statement.close();
            statement = null;
            return updated_rows;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("CPIDUpdateHostCounts()",s);
        }
        catch (Exception e) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("CPIDUpdateHostCounts()",e);
        }
    }
    
   
    public int ProjectUpdateTime(int project_id, String updateTime) 
    throws SQLException {
    
        Statement statement = null;
        int updated_rows;
        
        String query = "update projects set last_update_users='"+updateTime+"' where project_id="+project_id;
        
        try {           
            statement = cDBConnection.createStatement();
            updated_rows = statement.executeUpdate(query);
            if (updated_rows != 1) {
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
            throw new SQLException("ProjectUpdateTime()", s);
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
            throw new SQLException("ReadProjects()",s);
        }    
        catch (Exception e) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("ReadProjects()",e);
        } 
    }    
    
    public void CalculateCPCSTable(String hostMapTable, String cpcsTable) throws SQLException {
      
        String query;
        Statement statement = null;
        ResultSet resultSet = null;
        int pmax=0;
        
        log.info("Starting credit_per_cpu_sec analysis");
        try {            
        
            statement = cDBConnection.createStatement();

            // get max project_id
            query = "select max(project_id) as pmax from projects";
            statement = cDBConnection.createStatement();
            resultSet = statement.executeQuery( query );
    
            if ( resultSet == null ) 
                throw new SQLException();
            
            while( resultSet.next() )
                pmax = resultSet.getInt("pmax");
            
            resultSet.close();
            statement.close();
    
            // build an in-memory matrix
            BigDecimal [][] sum1 = new BigDecimal[pmax+1][pmax+1];
            BigDecimal [][] sum2 = new BigDecimal[pmax+1][pmax+1];
            long [][] counts = new long [pmax+1][pmax+1];
            for (int i=0;i<=pmax;i++)
                for (int j=0;j<=pmax;j++) {
                    sum1[i][j] = new BigDecimal("0.0");
                    sum2[i][j] = new BigDecimal("0.0");
                    counts[i][j] = 0;
                }
            
            // read in the hosts_map, ordered by host_cpid
            // for each host_cpid/project pair, update the matrix
            Vector <Integer> projectIDs = new Vector<Integer>();
            Vector <String> vcpcs = new Vector<String>();
            String last_cpid="";
            
            query = "select project_id,host_cpid,credit_per_cpu_sec from "+hostMapTable+" where credit_per_cpu_sec > 0.0 order by host_cpid";
            statement = cDBConnection.createStatement();
            resultSet = statement.executeQuery( query );
    
            if ( resultSet == null ) 
                throw new SQLException();
            boolean validHost=true;
            
            while( resultSet.next() )
            {
                String host_cpid;
                int project_id;
                String cpcs;
                
                project_id = resultSet.getInt("project_id");
                host_cpid = resultSet.getString("host_cpid");
                cpcs = resultSet.getString("credit_per_cpu_sec");
                double tval = resultSet.getDouble("credit_per_cpu_sec");
                
                if (tval > 1.0) {
                    validHost = false;
                    log.info("Host "+host_cpid+" has a bad credit_per_cpu_sec ("+cpcs+")");
                }
                
                if (last_cpid == "") 
                    last_cpid = host_cpid;
                
                if (host_cpid.compareToIgnoreCase(last_cpid)!= 0) {
                    // we now have a new set to put into the matrix
                    // only do if we have more than one item
                    if (projectIDs.size() > 1 && validHost==true) {
                        for (int i=0;i<projectIDs.size();i++) {
                            for (int j = i+1; j<projectIDs.size();j++) {
                                int p1 = projectIDs.elementAt(i);
                                int p2 = projectIDs.elementAt(j);
                                String v1 = vcpcs.elementAt(i);
                                String v2 = vcpcs.elementAt(j);

                                if (p1 != p2) {
                                    counts[p1][p2]++;
                                    counts[p2][p1]++;
                                    sum1[p1][p2]=sum1[p1][p2].add(new BigDecimal(v1));
                                    sum2[p1][p2]=sum2[p1][p2].add(new BigDecimal(v2));
                                    sum1[p2][p1]=sum1[p2][p1].add(new BigDecimal(v2));
                                    sum2[p2][p1]=sum2[p2][p1].add(new BigDecimal(v1));
                                }
                            }
                        }
                    }
                    
                    projectIDs.clear();
                    vcpcs.clear();
                    validHost = true;
                }
                last_cpid = host_cpid;
                projectIDs.add(project_id);
                vcpcs.add(cpcs);
            }
            
            // have to do last set as well
            if (projectIDs.size() > 1 && validHost==true) {
                for (int i=0;i<projectIDs.size();i++) {
                    for (int j = i+1; j<projectIDs.size();j++) {
                        int p1 = projectIDs.elementAt(i);
                        int p2 = projectIDs.elementAt(j);
                        String v1 = vcpcs.elementAt(i);
                        String v2 = vcpcs.elementAt(j);
                        
                        if (p1 != p2) {
                            counts[p1][p2]++;
                            counts[p2][p1]++;
                            sum1[p1][p2]=sum1[p1][p2].add(new BigDecimal(v1));
                            sum2[p1][p2]=sum2[p1][p2].add(new BigDecimal(v2));
                            sum1[p2][p1]=sum1[p2][p1].add(new BigDecimal(v2));
                            sum2[p2][p1]=sum2[p2][p1].add(new BigDecimal(v1));
                        }
                    }
                }
            }
            
            resultSet.close();
            statement.close();            
    
            // zero out hidden projects
            query = "select project_id from projects where shown='N'";
            statement = cDBConnection.createStatement();
            resultSet = statement.executeQuery( query );
            
            if ( resultSet == null ) 
                throw new SQLException();
           
            
            while( resultSet.next() ) {                
                int project_id = resultSet.getInt("project_id");
                for (int i=0; i<=pmax;i++)
                {
                    counts[project_id][i] = 0;
                    counts [i][project_id] = 0;
                }
            }
            
            // truncate table b_cpcs
            query = "truncate table "+cpcsTable;
            statement = cDBConnection.createStatement();
            int updated_rows = statement.executeUpdate(query);
            // clean up
            statement.close();
            statement = null;
    
            
            // save out the matix to b_cpcs         
            for (int i=1; i<=pmax;i++) {
                for (int j=1; j<=pmax;j++) {
                    // save info into db
                    double v1,v2,res;
                    if (counts[i][j] > 0) {
                        v1 = sum1[i][j].doubleValue() / (double)counts[i][j];
                        v2 = sum2[i][j].doubleValue() / (double)counts[i][j];
                        res = v1 / v2;
                        
                        query = "insert into "+cpcsTable+" (project_id1,project_id2,sum1,sum2,count,r_over_c) "+
                                "values ("+i+","+j+","+sum1[i][j].toString()+","+sum2[i][j].toString()+","+counts[i][j]+","+res+")";
                        
                        statement = cDBConnection.createStatement();
                        updated_rows = statement.executeUpdate(query);
                        if (updated_rows != 1) {
                            log.debug("Number of rows updated is wrong in CalculateCPCSTable: "+updated_rows);
                        }
                
                        // clean up
                        statement.close();
                        statement = null;
                        
                    }
                }
            }
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("CalculateCPCSTable()" ,s);        
        }
        catch (Exception e) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("CalculateCPCSTable()" ,e);        
        }
        
        log.info("CPCS Analysis Complete");
           
    } 
    
    public void DoGlobalUserRankings (String cpidTable, String tcTable, String racTable) throws SQLException {
        
        String query = "set @rank=0;";
        String query2 ="set @rank := 0, @country := 0";
        String query3 ="set @rank := 0, @year := 0;";
        
        Statement statement = null;

        String days30;
        String days90;
        String days365;
                
        long timestamp = System.currentTimeMillis() - (long)30*24*60*60*1000; 
        Date when = new Date(timestamp);
        java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
        sdf.setTimeZone(TimeZone.getDefault());
        days30 = sdf.format(when);
        timestamp = System.currentTimeMillis() - (long)90*24*60*60*1000; 
        when = new Date(timestamp);
        sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
        sdf.setTimeZone(TimeZone.getDefault());
        days90 = sdf.format(when);
        timestamp = System.currentTimeMillis() - (long)365*24*60*60*1000; 
        when = new Date(timestamp);
        sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
        sdf.setTimeZone(TimeZone.getDefault());
        days365 = sdf.format(when);
        
        try {            
            statement = cDBConnection.createStatement();

            log.info("Doing global ranking by credit");
            statement.execute(query);
            statement.execute("update "+cpidTable+" set global_credit = @rank := @rank+1 order by total_credit desc;");
            
            log.info("Doing global ranking by rac");
            statement.execute(query);
            statement.execute("update "+cpidTable+" set global_rac = @rank := @rank+1 where rac > 0 order by rac desc;");

            //log.info("Adding helper indexes");            
            //statement.execute("alter table "+cpidTable+" add index `active_project_count` (`active_project_count`),add index `join_date` (`join_date`),add index `country_id` (`country_id`),add index `active_computer_count` (`active_computer_count`);");
            
            log.info("Doing global ranking by join in last 30 days");            
            statement.execute(query);
            statement.execute("update "+cpidTable+" set global_new30days_credit = @rank := @rank+1 where join_date >='"+days30+"' and active_project_count >=1 order by total_credit desc;");
            statement.execute(query);
            statement.execute("update "+cpidTable+" set global_new30days_rac = @rank := @rank+1 where rac > 0 and join_date >='"+days30+"' and active_project_count >=1 order by rac desc;");

            log.info("Doing global ranking by join in last 90 days");            
            statement.execute(query);
            statement.execute("update "+cpidTable+" set global_new90days_credit = @rank := @rank+1 where join_date >='"+days90+"' and active_project_count >=1 order by total_credit desc;");
            statement.execute(query);
            statement.execute("update "+cpidTable+" set global_new90days_rac = @rank := @rank+1 where rac > 0 and join_date >='"+days90+"' and active_project_count >=1 order by rac desc;");

            log.info("Doing global ranking by join in last 365 days");            
            statement.execute(query);
            statement.execute("update "+cpidTable+" set global_new365days_credit = @rank := @rank+1 where join_date >='"+days365+"' and active_project_count >=1 order by total_credit desc;");
            statement.execute(query);
            statement.execute("update "+cpidTable+" set global_new365days_rac = @rank := @rank+1 where rac > 0 and join_date >='"+days365+"' and active_project_count >=1 order by rac desc;");

            log.info("Doing global ranking by >= 1 active project");            
            statement.execute(query);
            statement.execute("update "+cpidTable+" set global_1project_credit = @rank := @rank+1 where active_project_count >=1 order by total_credit desc;");
            statement.execute(query);
            statement.execute("update "+cpidTable+" set global_1project_rac = @rank := @rank+1 where rac > 0 and active_project_count >=1 order by rac desc;");

            log.info("Doing global ranking by >= 5 active projects");            
            statement.execute(query);
            statement.execute("update "+cpidTable+" set global_5project_credit = @rank := @rank+1 where active_project_count >= 5 order by total_credit desc;");
            statement.execute(query);
            statement.execute("update "+cpidTable+" set global_5project_rac = @rank := @rank+1 where rac > 0 and active_project_count >= 5 order by rac desc;");

            log.info("Doing global ranking by >= 10 active projects");            
            statement.execute(query);
            statement.execute("update "+cpidTable+" set global_10project_credit = @rank := @rank+1 where active_project_count >= 10 order by total_credit desc;");
            statement.execute(query);
            statement.execute("update "+cpidTable+" set global_10project_rac = @rank := @rank+1 where rac > 0 and active_project_count >= 10 order by rac desc;");

            log.info("Doing global ranking by >= 20 active projects");            
            statement.execute(query);
            statement.execute("update "+cpidTable+" set global_20project_credit = @rank := @rank+1 where active_project_count >= 20 order by total_credit desc;");
            statement.execute(query);
            statement.execute("update "+cpidTable+" set global_20project_rac = @rank := @rank+1 where rac > 0 and active_project_count >= 20 order by rac desc;");
            
            log.info("Doing global ranking by country");
            statement.execute(query2);
            statement.execute("update "+cpidTable+" t1, (select b_cpid_id, @rank := IF (@country = country_id,"+
            		"@rank := @rank + 1, @rank := 1) as rank, @country := country_id as country, total_credit from "+cpidTable+
            		" where country_id > 0 order by country_id, total_credit desc) t2 "+
            		"set global_country_credit = t2.rank where t2.b_cpid_id=t1.b_cpid_id;");
            statement.execute(query2);
            statement.execute("update "+cpidTable+" t1, (select b_cpid_id, @rank := IF (@country = country_id,"+
            		"@rank := @rank + 1, @rank := 1) as rank, @country := country_id as country, total_credit from "+cpidTable+
            		" where country_id > 0 and rac > 0 order by country_id, total_credit desc) t2 "+
            		"set global_country_credit = t2.rank where t2.b_cpid_id=t1.b_cpid_id;");
            
            log.info("Doing global ranking by join year");
            statement.execute(query3);
            statement.execute("update "+cpidTable+" t1, (select b_cpid_id, @rank := IF (@year = join_year,"+
            		"@rank := @rank + 1, @rank := 1) as rank, @year := join_year as year, total_credit from "+cpidTable+
            		" where join_year >= 1999 order by join_year, total_credit desc) t2 "+
            		"set global_joinyear_credit = t2.rank where t2.b_cpid_id=t1.b_cpid_id;");
            statement.execute(query3);
            statement.execute("update "+cpidTable+" t1, (select b_cpid_id, @rank := IF (@year = join_year,"+
            		"@rank := @rank + 1, @rank := 1) as rank, @year := join_year as year, rac from "+cpidTable+
            		" where join_year >= 1999 and rac > 0 order by join_year, rac desc) t2"+
            		"set global_joinyear_rac = t2.rank where t2.b_cpid_id=t1.b_cpid_id;");

            if (true == false) {
	            log.info("Doing global ranking by 1 computer and >= 1 active projects");            
	            statement.execute(query);
	            statement.execute("update "+cpidTable+" set computer1_1project_credit = @rank := @rank+1 where active_computer_count=1 and active_project_count >=1 order by total_credit desc;");
	            statement.execute(query);
	            statement.execute("update "+cpidTable+" set computer1_1project_rac = @rank := @rank+1 where rac > 0 and active_computer_count=1 and active_project_count >=1 order by rac desc;");
	            
	            log.info("Doing global ranking by 1 computer and >= 5 active projects");            
	            statement.execute(query);
	            statement.execute("update "+cpidTable+" set computer1_5project_credit = @rank := @rank+1 where active_computer_count = 1 and active_project_count >=5 order by total_credit desc;");
	            statement.execute(query);
	            statement.execute("update "+cpidTable+" set computer1_5project_rac = @rank := @rank+1 where rac > 0 and active_computer_count = 1 and active_project_count >=5 order by rac desc;");
	
	            log.info("Doing global ranking by 1 computer and >= 10 active projects");            
	            statement.execute(query);
	            statement.execute("update "+cpidTable+" set computer1_10project_credit = @rank := @rank+1 where active_computer_count = 1 and active_project_count >= 10 order by total_credit desc;");
	            statement.execute(query);
	            statement.execute("update "+cpidTable+" set computer1_10project_rac = @rank := @rank+1 where rac > 0 and active_computer_count = 1 and active_project_count >= 10 order by rac desc;");
	            
	            log.info("Doing global ranking by 1 computer and >= 20 active projects");            
	            statement.execute(query);
	            statement.execute("update "+cpidTable+" set computer1_20project_credit = @rank := @rank+1 where active_computer_count = 1 and active_project_count >= 20 order by total_credit desc;");
	            statement.execute(query);
	            statement.execute("update "+cpidTable+" set computer1_20project_rac = @rank := @rank+1 where rac > 0 and active_computer_count = 1 and active_project_count >= 20 order by rac desc;");
	            
	            log.info("Doing global ranking by <= 5 computers and >= 1 active projects");            
	            statement.execute(query);
	            statement.execute("update "+cpidTable+" set computer5_1project_credit = @rank := @rank+1 where active_computer_count > 0 and active_computer_count <= 5 and active_project_count >=1 order by total_credit desc;");
	            statement.execute(query);
	            statement.execute("update "+cpidTable+" set computer5_1project_rac = @rank := @rank+1 where rac > 0 and active_computer_count > 0 and active_computer_count <= 5 and active_project_count >=1 order by rac desc;");
	            
	            log.info("Doing global ranking by <= 5 computers and >= 5 active projects");            
	            statement.execute(query);
	            statement.execute("update "+cpidTable+" set computer5_5project_credit = @rank := @rank+1 where active_computer_count > 0 and active_computer_count <= 5 and active_project_count >=5 order by total_credit desc;");
	            statement.execute(query);
	            statement.execute("update "+cpidTable+" set computer5_5project_rac = @rank := @rank+1 where rac > 0 and active_computer_count > 0 and active_computer_count <= 5 and active_project_count >=5 order by rac desc;");
	            
	            log.info("Doing global ranking by <= 5 computers and >= 10 active projects");            
	            statement.execute(query);
	            statement.execute("update "+cpidTable+" set computer5_10project_credit = @rank := @rank+1 where active_computer_count > 0 and active_computer_count <= 5 and active_project_count >= 10 order by total_credit desc;");
	            statement.execute(query);
	            statement.execute("update "+cpidTable+" set computer5_10project_rac = @rank := @rank+1 where rac > 0 and active_computer_count > 0 and active_computer_count <= 5 and active_project_count >= 10 order by rac desc;");
	
	            log.info("Doing global ranking by <= 5 computers and >= 20 active projects");            
	            statement.execute(query);
	            statement.execute("update "+cpidTable+" set computer5_20project_credit = @rank := @rank+1 where active_computer_count > 0 and active_computer_count <= 5 and active_project_count >= 20 order by total_credit desc;");
	            statement.execute(query);
	            statement.execute("update "+cpidTable+" set computer5_20project_rac = @rank := @rank+1 where rac > 0 and active_computer_count > 0 and active_computer_count <= 5 and active_project_count >= 20 order by rac desc;");
	            
	            log.info("Doing global ranking by <= 10 computers and >= 1 active projects");            
	            statement.execute(query);
	            statement.execute("update "+cpidTable+" set computer10_1project_credit = @rank := @rank+1 where active_computer_count > 0 and active_computer_count <= 10 and active_project_count >=1 order by total_credit desc;");
	            statement.execute(query);
	            statement.execute("update "+cpidTable+" set computer10_1project_rac = @rank := @rank+1 where rac > 0 and active_computer_count > 0 and active_computer_count <= 10 and active_project_count >=1 order by rac desc;");
	            
	            log.info("Doing global ranking by <= 10 computers and >= 5 active projects");            
	            statement.execute(query);
	            statement.execute("update "+cpidTable+" set computer10_5project_credit = @rank := @rank+1 where active_computer_count > 0 and active_computer_count <= 10 and active_project_count >=5 order by total_credit desc;");
	            statement.execute(query);
	            statement.execute("update "+cpidTable+" set computer10_5project_rac = @rank := @rank+1 where rac > 0 and active_computer_count > 0 and active_computer_count <= 10 and active_project_count >=5 order by rac desc;");
	            
	            log.info("Doing global ranking by <= 10 computers and >= 10 active projects");            
	            statement.execute(query);
	            statement.execute("update "+cpidTable+" set computer10_10project_credit = @rank := @rank+1 where active_computer_count > 0 and active_computer_count <= 10 and active_project_count >= 10 order by total_credit desc;");
	            statement.execute(query);
	            statement.execute("update "+cpidTable+" set computer10_10project_rac = @rank := @rank+1 where rac > 0 and active_computer_count > 0 and active_computer_count <= 10 and active_project_count >= 10 order by rac desc;");
	            
	            log.info("Doing global ranking by <= 10 computers and >= 20 active projects");            
	            statement.execute(query);
	            statement.execute("update "+cpidTable+" set computer10_20project_credit = @rank := @rank+1 where active_computer_count > 0 and active_computer_count <= 10 and active_project_count >= 20 order by total_credit desc;");
	            statement.execute(query);
	            statement.execute("update "+cpidTable+" set computer10_20project_rac = @rank := @rank+1 where rac > 0 and active_computer_count > 0 and active_computer_count <= 10 and active_project_count >= 20 order by rac desc;");
            }
            
            log.info("Adding entries to user tc rankings table");
            statement.execute("alter table "+tcTable+" DISABLE KEYS;");
            log.info("starting insert");
            //statement.execute("insert into "+tcTable+" (b_cpid_id, user_cpid, name, country_id,active_computer_count,project_count,active_project_count, join_date, total_credit, rac, rac_time, global_credit, global_new30days_credit, global_new90days_credit, global_new365days_credit, global_1project_credit, global_5project_credit, global_10project_credit, global_20project_credit, global_country_credit, global_joindate_credit,computer1_1project_credit, computer1_5project_credit, computer1_10project_credit, computer1_20project_credit, computer5_1project_credit, computer5_5project_credit, computer5_10project_credit, computer5_20project_credit, computer10_1project_credit, computer10_5project_credit, computer10_10project_credit, computer10_20project_credit ) (select b_cpid_id, user_cpid, user_name, country_id, active_computer_count, project_count, active_project_count, join_date, total_credit, rac, rac_time, global_credit, global_new30days_credit, global_new90days_credit, global_new365days_credit, global_1project_credit, global_5project_credit, global_10project_credit, global_20project_credit, global_country_credit, global_joindate_credit,computer1_1project_credit, computer1_5project_credit, computer1_10project_credit, computer1_20project_credit, computer5_1project_credit, computer5_5project_credit, computer5_10project_credit, computer5_20project_credit, computer10_1project_credit, computer10_5project_credit, computer10_10project_credit, computer10_20project_credit from "+cpidTable+")");
            statement.execute("insert into "+tcTable+" (b_cpid_id, user_cpid, name, country_id,active_computer_count,project_count,active_project_count, join_date, join_year, total_credit, rac, rac_time, global_credit, global_new30days_credit, global_new90days_credit, global_new365days_credit, global_1project_credit, global_5project_credit, global_10project_credit, global_20project_credit, global_country_credit, global_joinyear_credit ) (select b_cpid_id, user_cpid, user_name, country_id, active_computer_count, project_count, active_project_count, join_date, join_year, total_credit, rac, rac_time, global_credit, global_new30days_credit, global_new90days_credit, global_new365days_credit, global_1project_credit, global_5project_credit, global_10project_credit, global_20project_credit, global_country_credit, global_joinyear_credit from "+cpidTable+")");            
            log.info("enabling keys");
            statement.execute("alter table "+tcTable+" ENABLE KEYS;");
            
            log.info("Adding entries to user rac rankings table");
            //statement.execute("alter table "+racTable+" DISABLE KEYS;");
            log.info("starting insert");
            //statement.execute("insert into "+racTable+" (b_cpid_id, user_cpid, name, country_id,active_computer_count,project_count,active_project_count, join_date, total_credit, rac, rac_time, global_rac, global_new30days_rac, global_new90days_rac, global_new365days_rac, global_1project_rac, global_5project_rac, global_10project_rac, global_20project_rac, global_country_rac, global_joindate_rac,computer1_1project_rac, computer1_5project_rac, computer1_10project_rac, computer1_20project_rac, computer5_1project_rac, computer5_5project_rac, computer5_10project_rac, computer5_20project_rac, computer10_1project_rac, computer10_5project_rac, computer10_10project_rac, computer10_20project_rac ) (select b_cpid_id, user_cpid, user_name, country_id, active_computer_count, project_count, active_project_count, join_date, total_credit, rac, rac_time, global_rac, global_new30days_rac, global_new90days_rac, global_new365days_rac, global_1project_rac, global_5project_rac, global_10project_rac, global_20project_rac, global_country_rac, global_joindate_rac,computer1_1project_rac, computer1_5project_rac, computer1_10project_rac, computer1_20project_rac, computer5_1project_rac, computer5_5project_rac, computer5_10project_rac, computer5_20project_rac, computer10_1project_rac, computer10_5project_rac, computer10_10project_rac, computer10_20project_rac from "+cpidTable+")");
            statement.execute("insert into "+racTable+" (b_cpid_id, user_cpid, name, country_id,active_computer_count,project_count,active_project_count, join_date,join_year, total_credit, rac, rac_time, global_rac, global_new30days_rac, global_new90days_rac, global_new365days_rac, global_1project_rac, global_5project_rac, global_10project_rac, global_20project_rac, global_country_rac, global_joinyear_rac) (select b_cpid_id, user_cpid, user_name, country_id, active_computer_count, project_count, active_project_count, join_date, join_year, total_credit, rac, rac_time, global_rac, global_new30days_rac, global_new90days_rac, global_new365days_rac, global_1project_rac, global_5project_rac, global_10project_rac, global_20project_rac, global_country_rac, global_joinyear_rac from "+cpidTable+")");
            //log.info("enabling keys");
            //statement.execute("alter table "+racTable+" ENABLE KEYS;");

            
            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("DoGlobalUserRankings()",s);
        } 
        catch (Exception e) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("DoGlobalUserRankings()",e);
        }
    }
    
    public void DoGlobalCTeamRankings (String cteamTable, String tcTable, String racTable) throws SQLException {
        
        String query = "set @rank=0;";
        String query2 ="set @rank := 0, @pos := 0, @country_id := null, @tc := null;";
        
        Statement statement = null;
        
        
        try {            
            statement = cDBConnection.createStatement();

            log.info("Doing team global ranking by credit");
            statement.execute(query);
            statement.execute("update "+cteamTable+" set global_credit = @rank := @rank+1 order by total_credit desc;");
            
            log.info("Doing team global ranking by rac");
            statement.execute(query);
            statement.execute("update "+cteamTable+" set global_rac = @rank := @rank+1 where rac > 0 order by rac desc;");

            log.info("Adding team helper indexes");            
            statement.execute("alter table "+cteamTable+" add index `active_project_count` (`active_project_count`),add index `country_id` (`country_id`);");
            
            log.info("Doing team global ranking by >= 1 active project");            
            statement.execute(query);
            statement.execute("update "+cteamTable+" set global_1project_credit = @rank := @rank+1 where active_project_count >=1 order by total_credit desc;");
            statement.execute(query);
            statement.execute("update "+cteamTable+" set global_1project_rac = @rank := @rank+1 where rac > 0 and active_project_count >=1 order by rac desc;");

            log.info("Doing team global ranking by >= 5 active projects");            
            statement.execute(query);
            statement.execute("update "+cteamTable+" set global_5project_credit = @rank := @rank+1 where active_project_count >= 5 order by total_credit desc;");
            statement.execute(query);
            statement.execute("update "+cteamTable+" set global_5project_rac = @rank := @rank+1 where rac > 0 and active_project_count >= 5 order by rac desc;");

            log.info("Doing team global ranking by >= 10 active projects");            
            statement.execute(query);
            statement.execute("update "+cteamTable+" set global_10project_credit = @rank := @rank+1 where active_project_count >= 10 order by total_credit desc;");
            statement.execute(query);
            statement.execute("update "+cteamTable+" set global_10project_rac = @rank := @rank+1 where rac > 0 and active_project_count >= 10 order by rac desc;");

            log.info("Doing team global ranking by >= 20 active projects");            
            statement.execute(query);
            statement.execute("update "+cteamTable+" set global_20project_credit = @rank := @rank+1 where active_project_count >= 20 order by total_credit desc;");
            statement.execute(query);
            statement.execute("update "+cteamTable+" set global_20project_rac = @rank := @rank+1 where rac > 0 and active_project_count >= 20 order by rac desc;");
            
            log.info("Doing team global ranking by country");
            statement.execute(query2);
            statement.execute("update "+cteamTable+" set global_country_credit = greatest("+
          	      "@rank := if(@country_id = country_id and @tc = total_credit, @rank, if(@country_id <> country_id, 1, @rank + 1)),"+
        	      "least(0, @pos   := if(@country_id = country_id, @pos + 1, 1)),"+
        	      "least(0, @tc := total_credit), least(0, @country_id  := country_id)) "+
        	      "where country_id > 0 order by country_id desc, total_credit desc;");
            statement.execute(query2);
            statement.execute("update "+cteamTable+" set global_country_rac = greatest("+
          	      "@rank := if(@country_id = country_id and @tc = rac, @rank, if(@country_id <> country_id, 1, @rank + 1)),"+
        	      "least(0, @pos   := if(@country_id = country_id, @pos + 1, 1)),"+
        	      "least(0, @tc := rac), least(0, @country_id  := country_id)) "+
        	      "where country_id > 0 and rac > 0 order by country_id desc, rac desc;");
            
            
            log.info("Adding entries to team tc rankings table");
            //statement.execute("alter table "+tcTable+" DISABLE KEYS;");
            log.info("starting insert");
            statement.execute("insert into "+tcTable+" (table_id, team_cpid, name, nusers, total_credit, rac, rac_time, project_count, active_project_count, create_time, country_id, global_credit, global_country_credit, global_1project_credit, global_5project_credit, global_10project_credit, global_20project_credit ) (select table_id, team_cpid, name, nusers, total_credit, rac, rac_time, project_count, active_project_count, create_time, country_id, global_credit, global_country_credit, global_1project_credit, global_5project_credit, global_10project_credit, global_20project_credit from "+cteamTable+")");
            //log.info("enabling keys");
            //statement.execute("alter table "+tcTable+" ENABLE KEYS;");
            
            log.info("Adding entries to team rac rankings table");
            //statement.execute("alter table "+racTable+" DISABLE KEYS;");
            log.info("starting insert");
            statement.execute("insert into "+racTable+" (table_id, team_cpid, name, nusers, total_credit, rac, rac_time, project_count, active_project_count, create_time, country_id, global_rac, global_country_rac, global_1project_rac, global_5project_rac, global_10project_rac, global_20project_rac ) (select table_id, team_cpid, name, nusers, total_credit, rac, rac_time, project_count, active_project_count, create_time, country_id, global_rac, global_country_rac, global_1project_rac, global_5project_rac, global_10project_rac, global_20project_rac from "+cteamTable+")");
            //log.info("enabling keys");
            //statement.execute("alter table "+racTable+" ENABLE KEYS;");

            
            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("DoGlobalCTeamRankings()",s);
        }
        catch (Exception e) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("DoGlobalCTeamRankings()",e);
        }
    }

    
    public void DoGlobalCountryRankings (String cpidTable, String countryTable) throws SQLException {
        
        String query = "set @rank=0;";
        Statement statement = null;
        
        try {            
            statement = cDBConnection.createStatement();

            log.info("Creating Country totals");
            statement.execute(query);
            statement.execute("insert into "+countryTable+" (country_id, total_credit, rac,user_count) (select country_id, sum(total_credit),sum(rac),count(*) from "+cpidTable+" group by country_id);");

            log.info("Doing country global ranking by total credit");
            statement.execute(query);
            statement.execute("update "+countryTable+" set rank_tc = @rank := @rank+1 where country_id > 0 order by total_credit desc;");

            log.info("Doing country global ranking by rac");
            statement.execute(query);
            statement.execute("update "+countryTable+" set rank_rac = @rank := @rank+1 where country_id > 0 and rac > 0 order by rac desc;");

            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("DoGlobalCountryRankings()",s);
        }
        catch (Exception e) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("DoGlobalCTeamRankings()",e);
        }
    }

    public void DoCombinedStatsProjectUpdates (String cpidTable, String teamTable) throws SQLException {
        
    
        // update user count, total credit, rac
        String query1 = "update projects, (select count(*) as cnt, sum(total_credit) as tc, sum(rac) as rc from "+cpidTable+" where project_count > 0) cc set projects.user_count=cc.cnt, projects.total_credit=cc.tc,projects.rac=cc.rc where projects.project_id=1";
        String query2 = "update projects, (select count(*) as cnt from "+cpidTable+" where project_count > 0 and rac > 0) cc set projects.active_users=cc.cnt where projects.project_id=1";
        
        // update country count
        String query3 = "update projects, (select count(*) as cnt from (select distinct country_id from "+cpidTable+") uc) cc set projects.country_count=cc.cnt where projects.project_id=1";
        
        // update computer count
        String query4 = "update projects, (select sum(active_computer_count) as cnt from "+cpidTable+") cc set projects.host_count=0,projects.active_hosts=cc.cnt where projects.project_id=1";
                        
        // update team counts
        String query5 = "update projects, (select count(*) as cnt from "+teamTable+" where project_count > 0) cc set projects.team_count=cc.cnt where projects.project_id=1";
        String query6 = "update projects, (select count(*) as cnt from "+teamTable+" where project_count > 0 and rac > 0) cc set projects.active_teams=cc.cnt where projects.project_id=1";
        

        Statement statement = null;

        
        try {            
            statement = cDBConnection.createStatement();

            log.info("updating project 1 user count, total credit, rac");
            statement.execute(query1);
            log.info("updating project 1 active user count");
            statement.execute(query2);
            log.info("updating project 1 country count");
            statement.execute(query3);
            log.info("Updating project 1 active computer count");
            statement.execute(query4);
            log.info("Updating project 1 team count");
            statement.execute(query5);
            log.info("Updating project 1 active team count");
            statement.execute(query6);
            statement.close();

            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("DoCombinedStatsProjectUpdates()",s);
        }                
    }


    public int  GetCurrentDay(String key) throws SQLException {
        
        int day_of_update=0;
        String query = "select value from currentday where key_item='"+key+"'";
        
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
            return day_of_update;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("GetCurrentDay()",s);
        } 
        catch (Exception e) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("GetCurrentDay()",e);
        }
    }
    
    public int SetCurrentDay(String key, int day_of_update) throws SQLException {
        Statement statement = null;
        int updated_rows=0;
        
        try {            
                       
            String query = "replace currentday (key_item, value) values ('"+key+"',"+day_of_update+")";
        
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
            throw new SQLException("SetCurrentDay()",s);
        }        
    }    
    
  
    public void ExportUserTable(String exportFile) throws SQLException {
        
        String psquery = "select b_cpid.user_cpid, b_cpid.total_credit, b_cpid.rac, b_cpid.rac_time, b_cpid.computer_count, b_users.name,b_users.user_id, "+
            "b_country.country,b_users.create_time,b_users.url,b_users.teamid,b_teams.name as team_name, b_users.total_credit as ptc, b_users.rac as prac, "+
            "b_users.rac_time as prt, projects.name as pname, projects.url as purl, b_users.computer_count as pcc, b_rank_user_tc_p0c0.rank as wr "+
            "from b_cpid join (b_users, projects, b_rank_user_tc_p0c0) "+
            "on (b_cpid.table_id=b_users.b_cpid_id and b_users.project_id=projects.project_id and b_rank_user_tc_p0c0.b_cpid_id=b_cpid.table_id) "+
            "left join (b_country) on (b_users.country_id=b_country.country_id) "+
            "left join (b_teams) on (b_users.b_team_id=b_teams.table_id) "+
            "where b_cpid.user_cpid=? and b_cpid.project_count > 0 and b_cpid.total_credit > 0 ";//+
            //"order by b_cpid.rac desc,b_cpid.table_id";

        PreparedStatement ps = null;
        String query = "select b_cpid.user_cpid from b_cpid order by b_cpid.rac desc";
        
        String countQuery = "select count(*)as cnt, sum(total_credit) as tc, sum(rac) as rt from b_cpid where project_count > 0 and total_credit > 0";
        
        Statement statement = null;
        ResultSet resultSet = null;
        
        long totalCredit = 0;
        long totalRac = 0;
        long totalUsers = 0;
        
        // get the totals
        try {            
            statement = cDBConnection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,java.sql.ResultSet.CONCUR_READ_ONLY);
            //statement.setFetchSize(Integer.MIN_VALUE);
            statement.setFetchSize(100);

            resultSet = statement.executeQuery( countQuery );

            if ( resultSet == null ) {
                throw new SQLException();
            }
            
            while( resultSet.next() )
            {
                totalUsers =  resultSet.getLong("cnt");
                totalCredit = resultSet.getLong("tc");
                totalRac = resultSet.getLong("rt");
            }        
            resultSet.close();
            statement.close();
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("ExportUserTable() - " + s.toString());
        }     
        
        log.info("Expecting "+totalUsers);
        int count=0;
        // now open the file
        try {
            GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(exportFile));
            String temp;
            
            temp = "<users>\n"+
                   "  <nusers>"+totalUsers+"</nusers>\n"+
                   "  <total_credit>"+totalCredit+".000000</total_credit>\n"+
                   "  <expavg_credit>"+totalRac+".000000</expavg_credit>\n";
            out.write(temp.getBytes());
            
            temp = "";
            
            // do SQL Query to get data
            try {            
                statement = cDBConnection.createStatement();
                resultSet = statement.executeQuery( query );

                if ( resultSet == null ) {
                    throw new SQLException();
                }
                
                String lastCpid="";
                
                ps = cDBConnection.prepareStatement(psquery);
                while( resultSet.next() )
                {
                    String user_cpid =  resultSet.getString("user_cpid");
                    
                    ps.setString(1, user_cpid);
                    
                    ResultSet rs2 = ps.executeQuery();
                    while (rs2.next()) {
                        user_cpid =  rs2.getString("user_cpid");
                        totalCredit = rs2.getInt("total_credit");
                        totalRac = rs2.getInt("rac");
                        int rac_time = rs2.getInt("rac_time");
                        int computer_count = rs2.getInt("computer_count");
                        String user_name = rs2.getString("name");
                        int user_id = rs2.getInt("user_id");
                        String country = rs2.getString("country");
                        int create_time = rs2.getInt("create_time");
                        String url = rs2.getString("url");
                        int teamid = rs2.getInt("teamid");
                        String team_name = rs2.getString("team_name");
                        int ptc = rs2.getInt("ptc");
                        int prac = rs2.getInt("prac");
                        int prt = rs2.getInt("prt");
                        String pname = rs2.getString("pname");
                        String purl = rs2.getString("purl");
                        int pcc = rs2.getInt("pcc");
                        int world_rank = rs2.getInt("wr");
                        
                        if (lastCpid.compareToIgnoreCase(user_cpid)!= 0) {
                            
                            // starting a new entry
                            if (count > 0) temp = "  </user>\n";
                            count++;
                            temp += "  <user>\n"+
                                   "     <total_credit>"+totalCredit+"</total_credit>\n"+
                                   "     <expavg_credit>"+totalRac+"</expavg_credit>\n"+
                                   "     <expavg_time>"+rac_time+"</expavg_time>\n"+
                                   "     <world_rank>"+world_rank+"</world_rank>\n"+
                                   "     <name>"+user_name+"</name>\n"+
                                   "     <user_cpid>"+user_cpid+"</user_cpid>\n"+
                                   "     <computer_count>"+computer_count+"</computer_count>\n";
                            out.write(temp.getBytes());
                        }          
                        
                        temp =     "     <project>\n"+
                                   "         <name>"+EscapeXMLChars(pname)+"</name>\n";
                        if (purl != null && purl.length() > 0) 
                            temp +="         <url>"+purl+"</url>\n";
                        
                        temp +=    "         <id>"+user_id+"</id>\n"+
                                   "         <user_name>"+EscapeXMLChars(user_name)+"</user_name>\n"+
                                   "         <create_time>"+create_time+"</create_time>\n"+
                                   "         <total_credit>"+ptc+"</total_credit>\n"+
                                   "         <expavg_credit>"+prac+"</expavg_credit>\n"+
                                   "         <expavg_time>"+prt+"</expavg_time>\n";
                        if (country != null && country.length() > 0) {
                            temp +="         <country>"+EscapeXMLChars(country)+"</country>\n";
                        }
                        if (url != null && url.length() > 0)
                            temp +="         <user_url>http://"+url+"</user_url>\n";
                        if (team_name != null && team_name.length() > 0) {
                            temp +="         <team_id>"+teamid+"</team_id>\n"+
                                   "         <team_name>"+EscapeXMLChars(team_name)+"</team_name>\n";
                                   
                        }
                        temp +=    "         <computer_count>"+pcc+"</computer_count>\n"+
                                   "     </project>\n";
                        
                        out.write(temp.getBytes());
                        
                        lastCpid = user_cpid;
                    }
                    rs2.close();
                }  
                resultSet.close();
                statement.close();
            } catch (SQLException s) {
            	s.printStackTrace();
                if (statement != null) {
                    statement.close();
                    statement = null;
                }
                throw new SQLException("ExportUserTable() - " + s.toString());
            }
            // write out a user entry
            
            // write closing bit
            temp = "  </user>\n</users>\n";
            out.write(temp.getBytes());
            out.close();
            
        }
        catch (Exception e) {
            log.error(e);
        }
        
        log.info("Wrote "+count+" entries");
    }
        
    public void ExportHostTable(String exportFile) throws SQLException {
        
        String query = "select b_cpid.user_cpid, b_cpid.total_credit, b_cpid.rac, b_cpid.rac_time, b_cpid.computer_count, b_users.name,b_users.user_id, "+
            "b_country.country,b_users.create_time,b_users.url,b_teams.name as team_name, b_users.total_credit as ptc, b_users.rac as prac, "+
            "b_users.rac_time as prt, projects.name as pname, projects.url as purl, b_users.computer_count as pcc, b_rank_world_tc.rank as wr "+
            "from b_cpid join (b_users, projects, b_rank_world_tc) "+
            "on (b_cpid.table_id=b_users.b_cpid_id and b_users.project_id=projects.project_id and b_rank_world_tc.b_cpid_id=b_cpid.table_id) "+
            "left join (b_country) on (b_users.country_id=b_country.country_id) "+
            "left join (b_teams) on (b_users.b_team_id=b_teams.table_id) "+
            "where b_cpid.project_count > 0 and b_cpid.total_credit > 0 "+
            "order by b_cpid.table_id";

        
        String countQuery = "select count(*)as cnt, sum(total_credit) as tc, sum(rac) as rt from b_cpid where project_count > 0 and total_credit > 0";
        
        Statement statement = null;
        ResultSet resultSet = null;
        
        long totalCredit = 0;
        long totalRac = 0;
        long totalUsers = 0;
        
        // get the totals
        try {            
            statement = cDBConnection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,java.sql.ResultSet.CONCUR_READ_ONLY);
            statement.setFetchSize(Integer.MIN_VALUE);

            resultSet = statement.executeQuery( countQuery );

            if ( resultSet == null ) {
                throw new SQLException();
            }
            
            while( resultSet.next() )
            {
                totalUsers =  resultSet.getLong("cnt");
                totalCredit = resultSet.getLong("tc");
                totalRac = resultSet.getLong("rt");
            }        
            resultSet.close();
            statement.close();
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("ExportUserTable() - " + s.toString());
        }     
        
        int count=0;
        // now open the file
        try {
            GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(exportFile));
            String temp;
            
            temp = "<users>\n"+
                   "  <nusers>"+totalUsers+"</nusers>\n"+
                   "  <total_credit>"+totalCredit+".000000</total_credit>\n"+
                   "  <expavg_credit>"+totalRac+".000000</expavg_credit>\n";
            out.write(temp.getBytes());
            
            temp = "";
            
            // do SQL Query to get data
            try {            
                statement = cDBConnection.createStatement();
                resultSet = statement.executeQuery( query );

                if ( resultSet == null ) {
                    throw new SQLException();
                }
                
                String lastCpid="";
                
                while( resultSet.next() )
                {
                    
                    String user_cpid =  resultSet.getString("user_cpid");
                    totalCredit = resultSet.getInt("total_credit");
                    totalRac = resultSet.getInt("rac");
                    int rac_time = resultSet.getInt("rac_time");
                    int computer_count = resultSet.getInt("computer_count");
                    String user_name = resultSet.getString("name");
                    int user_id = resultSet.getInt("user_id");
                    String country = resultSet.getString("country");
                    int create_time = resultSet.getInt("create_time");
                    String url = resultSet.getString("url");
                    String team_name = resultSet.getString("team_name");
                    int ptc = resultSet.getInt("ptc");
                    int prac = resultSet.getInt("prac");
                    int prt = resultSet.getInt("prt");
                    String pname = resultSet.getString("pname");
                    String purl = resultSet.getString("purl");
                    int pcc = resultSet.getInt("pcc");
                    int world_rank = resultSet.getInt("wr");
                    
                    if (lastCpid.compareToIgnoreCase(user_cpid)!= 0) {
                        
                        // starting a new entry
                        if (count > 0) temp = "  </user>\n";
                        count++;
                        temp += "  <user>\n"+
                               "     <total_credit>"+totalCredit+"</total_credit>\n"+
                               "     <expavg_credit>"+totalRac+"</expavg_credit>\n"+
                               "     <expavg_time>"+rac_time+"</expavg_time>\n"+
                               "     <world_rank>"+world_rank+"</world_rank>\n"+
                               "     <name>"+user_name+"</name>\n"+
                               "     <user_cpid>"+user_cpid+"</user_cpid>\n"+
                               "     <computer_count>"+computer_count+"</computer_count>\n";
                        out.write(temp.getBytes());
                    }          
                    
                    temp =     "     <project>\n"+
                               "         <name>"+EscapeXMLChars(pname)+"</name>\n";
                    if (purl != null && purl.length() > 0) 
                        temp +="         <url>"+purl+"</url>\n";
                    
                    temp +=    "         <id>"+user_id+"</id>\n"+
                               "         <user_name>"+EscapeXMLChars(user_name)+"</user_name>\n"+
                               "         <create_time>"+create_time+"</create_time>\n"+
                               "         <total_credit>"+ptc+"</total_credit>\n"+
                               "         <expavg_credit>"+prac+"</expavg_credit>\n"+
                               "         <expavg_time>"+prt+"</expavg_time>\n";
                    if (country != null && country.length() > 0) {
                        temp +="         <country>"+EscapeXMLChars(country)+"</country>\n";
                    }
                    if (url != null && url.length() > 0)
                        temp +="         <user_url>http://"+url+"</user_url>\n";
                    if (team_name != null && team_name.length() > 0) {
                        temp +="         <team_id>"+"</team_id>\n"+
                               "         <team_name>"+EscapeXMLChars(team_name)+"</team_name>\n";
                               
                    }
                    temp +=    "         <computer_count>"+pcc+"</computer_count>\n"+
                               "     </project>\n";
                    
                    out.write(temp.getBytes());
                    
                    lastCpid = user_cpid;

                }  
                resultSet.close();
                statement.close();
            } catch (SQLException s) {
                if (statement != null) {
                    statement.close();
                    statement = null;
                }
                throw new SQLException("ExportUserTable() - " + s.toString());
            }
            // write out a user entry
            
            // write closing bit
            temp = "  </user>\n</users>\n";
            out.write(temp.getBytes());
            out.close();
            
        }
        catch (Exception e) {
            log.error(e);
        }
        
        log.info("Wrote "+count+" entries");
    }
    public void ExportTeamTable(String exportFile) throws SQLException {
        
        String query = "select b_cpid.user_cpid, b_cpid.total_credit, b_cpid.rac, b_cpid.rac_time, b_cpid.computer_count, b_users.name,b_users.user_id, "+
            "b_country.country,b_users.create_time,b_users.url,b_teams.name as team_name, b_users.total_credit as ptc, b_users.rac as prac, "+
            "b_users.rac_time as prt, projects.name as pname, projects.url as purl, b_users.computer_count as pcc, b_rank_world_tc.rank as wr "+
            "from b_cpid join (b_users, projects, b_rank_world_tc) "+
            "on (b_cpid.table_id=b_users.b_cpid_id and b_users.project_id=projects.project_id and b_rank_world_tc.b_cpid_id=b_cpid.table_id) "+
            "left join (b_country) on (b_users.country_id=b_country.country_id) "+
            "left join (b_teams) on (b_users.b_team_id=b_teams.table_id) "+
            "where b_cpid.project_count > 0 and b_cpid.total_credit > 0 "+
            "order by b_cpid.table_id";

        
        String countQuery = "select count(*)as cnt, sum(total_credit) as tc, sum(rac) as rt from b_cpid where project_count > 0 and total_credit > 0";
        
        Statement statement = null;
        ResultSet resultSet = null;
        
        long totalCredit = 0;
        long totalRac = 0;
        long totalUsers = 0;
        
        // get the totals
        try {            
            statement = cDBConnection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,java.sql.ResultSet.CONCUR_READ_ONLY);
            statement.setFetchSize(Integer.MIN_VALUE);

            resultSet = statement.executeQuery( countQuery );

            if ( resultSet == null ) {
                throw new SQLException();
            }
            
            while( resultSet.next() )
            {
                totalUsers =  resultSet.getLong("cnt");
                totalCredit = resultSet.getLong("tc");
                totalRac = resultSet.getLong("rt");
            }     
            resultSet.close();
            statement.close();
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("ExportUserTable() - " + s.toString());
        }     
        
        int count=0;
        // now open the file
        try {
            GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(exportFile));
            String temp;
            
            temp = "<users>\n"+
                   "  <nusers>"+totalUsers+"</nusers>\n"+
                   "  <total_credit>"+totalCredit+".000000</total_credit>\n"+
                   "  <expavg_credit>"+totalRac+".000000</expavg_credit>\n";
            out.write(temp.getBytes());
            
            temp = "";
            
            // do SQL Query to get data
            try {            
                statement = cDBConnection.createStatement();
                resultSet = statement.executeQuery( query );

                if ( resultSet == null ) {
                    throw new SQLException();
                }
                
                String lastCpid="";
                
                while( resultSet.next() )
                {
                    
                    String user_cpid =  resultSet.getString("user_cpid");
                    totalCredit = resultSet.getInt("total_credit");
                    totalRac = resultSet.getInt("rac");
                    int rac_time = resultSet.getInt("rac_time");
                    int computer_count = resultSet.getInt("computer_count");
                    String user_name = resultSet.getString("name");
                    int user_id = resultSet.getInt("user_id");
                    String country = resultSet.getString("country");
                    int create_time = resultSet.getInt("create_time");
                    String url = resultSet.getString("url");
                    String team_name = resultSet.getString("team_name");
                    int ptc = resultSet.getInt("ptc");
                    int prac = resultSet.getInt("prac");
                    int prt = resultSet.getInt("prt");
                    String pname = resultSet.getString("pname");
                    String purl = resultSet.getString("purl");
                    int pcc = resultSet.getInt("pcc");
                    int world_rank = resultSet.getInt("wr");
                    
                    if (lastCpid.compareToIgnoreCase(user_cpid)!= 0) {
                        
                        // starting a new entry
                        if (count > 0) temp = "  </user>\n";
                        count++;
                        temp += "  <user>\n"+
                               "     <total_credit>"+totalCredit+"</total_credit>\n"+
                               "     <expavg_credit>"+totalRac+"</expavg_credit>\n"+
                               "     <expavg_time>"+rac_time+"</expavg_time>\n"+
                               "     <world_rank>"+world_rank+"</world_rank>\n"+
                               "     <name>"+EscapeXMLChars(user_name)+"</name>\n"+
                               "     <user_cpid>"+user_cpid+"</user_cpid>\n"+
                               "     <computer_count>"+computer_count+"</computer_count>\n";
                        out.write(temp.getBytes());
                    }          
                    
                    temp =     "     <project>\n"+
                               "         <name>"+EscapeXMLChars(pname)+"</name>\n";
                    if (purl != null && purl.length() > 0) 
                        temp +="         <url>"+purl+"</url>\n";
                    
                    temp +=    "         <id>"+user_id+"</id>\n"+
                               "         <user_name>"+EscapeXMLChars(user_name)+"</user_name>\n"+
                               "         <create_time>"+create_time+"</create_time>\n"+
                               "         <total_credit>"+ptc+"</total_credit>\n"+
                               "         <expavg_credit>"+prac+"</expavg_credit>\n"+
                               "         <expavg_time>"+prt+"</expavg_time>\n";
                    if (country != null && country.length() > 0) {
                        temp +="         <country>"+EscapeXMLChars(country)+"</country>\n";
                    }
                    if (url != null && url.length() > 0)
                        temp +="         <user_url>http://"+url+"</user_url>\n";
                    if (team_name != null && team_name.length() > 0) {
                        temp +="         <team_id>"+"</team_id>\n"+
                               "         <team_name>"+EscapeXMLChars(team_name)+"</team_name>\n";
                               
                    }
                    temp +=    "         <computer_count>"+pcc+"</computer_count>\n"+
                               "     </project>\n";
                    
                    out.write(temp.getBytes());
                    
                    lastCpid = user_cpid;

                }  
                resultSet.close();
                statement.close();
            } catch (SQLException s) {
                if (statement != null) {
                    statement.close();
                    statement = null;
                }
                throw new SQLException("ExportUserTable() - " + s.toString());
            }
            // write out a user entry
            
            // write closing bit
            temp = "  </user>\n</users>\n";
            out.write(temp.getBytes());
            out.close();
            
        }
        catch (Exception e) {
            log.error(e);
        }
        
        log.info("Wrote "+count+" entries");
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
            throw new SQLException("RunQuery()",s);
        }
        catch (Exception e) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("RunQuery()",e);
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
            throw new SQLException("RunCountQuery()",s);
        }
        catch (Exception e) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("RunCountQuery()",e);
        }
    }
    
    public void ProcessHistory(String oldHistoryTable, String newHistoryTable, String sourceTable, String keyColumnName, String dataColumnName, String ymm, int day ) throws SQLException {
        
    	String query = "insert into "+newHistoryTable+" (select "+sourceTable+"."+keyColumnName;
    	
    	// put in the "d_" column names
    	for (int i=1;i<=91;i++) {
    		if (i == day) {
    			query += ","+sourceTable+"."+dataColumnName;
    		} else {
    			query += ",ifnull("+oldHistoryTable+".d_"+i+",0)";
    		}
    	}
    	
    	// put in the "ymm" column names
    	for (int i=0;i<10;i++) {
    		for (int j=1;j<=12;j++) {
    			String t = ""+i;
    			if (j<10) t += "0"+j;
    			else t += ""+j;
    			
    			if (ymm.compareTo(t) == 0) {
    				query += ","+sourceTable+"."+dataColumnName;
    			} else {
    				query += ",ifnull("+oldHistoryTable+".ymm_"+t+",0)";
    			}
    		}
    	}
    	
    	query += " from "+oldHistoryTable+" right join "+sourceTable+" on "+oldHistoryTable+"."+keyColumnName+"="+sourceTable+"."+keyColumnName+")";
    	
    	Statement statement = null;

    	try {   
    		
            statement = cDBConnection.createStatement();
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
            throw new SQLException("ProcessHistory()", s);
        }
        catch (Exception e) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("ProcessHistory()", e);
        }
    }
    
    public void ProcessRACHistory(String oldHistoryTable, String newHistoryTable, String sourceTable, String keyColumnName, String dataColumnName, String ymm, int day ) throws SQLException {
        
    	String query = "insert into "+newHistoryTable+" (select "+sourceTable+"."+keyColumnName;
    	
    	// put in the "d_" column names
    	for (int i=1;i<=91;i++) {
    		if (i == day) {
    			query += ","+sourceTable+"."+dataColumnName;
    		} else {
    			query += ",ifnull("+oldHistoryTable+".d_"+i+",0)";
    		}
    	}
    	query += " from "+oldHistoryTable+" right join "+sourceTable+" on "+oldHistoryTable+"."+keyColumnName+"="+sourceTable+"."+keyColumnName+")";
    	
    	Statement statement = null;

    	try {   
    		
            statement = cDBConnection.createStatement();
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
            throw new SQLException("ProcessRACHistory()", s);
        }
        catch (Exception e) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("ProcessRACHistory()", e);
        }
    }
    
    public void CreateCPIDTable (String tablename) throws SQLException {
        
    	String query = "CREATE TABLE "+tablename;
    	query += " (  `b_cpid_id` bigint(20) NOT NULL,"+
		" `user_cpid` varchar(32) NOT NULL default '',  `user_name` varchar(255) NOT NULL default '',"+
		" `join_date` DATE NOT NULL default '0000-00-00',"+
		" `join_year` varchar(4) NOT NULL default '',"+
		" `country_id` smallint(6) default NULL,"+
		" `project_count` smallint(6) NOT NULL default '0',  `active_project_count` smallint(6) NOT NULL default '0',"+
		" `active_computer_count` int(11) NOT NULL default '0',"+
		" `total_credit` bigint(20) NOT NULL default '0',  `rac` bigint(20) NOT NULL default '0',"+
		" `rac_time` bigint(20) NOT NULL default '0',"+
		"`hosts_visible` VARCHAR(1) NOT NULL DEFAULT 'N',"+
		"`global_credit` BIGINT UNSIGNED,"+
		"`global_rac` BIGINT UNSIGNED,"+
		"`global_new30days_credit` BIGINT UNSIGNED,"+
		"`global_new30days_rac` BIGINT UNSIGNED,"+
		"`global_new90days_credit` BIGINT UNSIGNED,"+
		"`global_new90days_rac` BIGINT UNSIGNED,"+
		"`global_new365days_credit` BIGINT UNSIGNED,"+
		"`global_new365days_rac` BIGINT UNSIGNED,"+
		"`global_1project_credit` BIGINT UNSIGNED,"+
		"`global_1project_rac` BIGINT UNSIGNED,"+
		"`global_5project_credit` BIGINT UNSIGNED,"+
		"`global_5project_rac` BIGINT UNSIGNED,"+
		"`global_10project_credit` BIGINT UNSIGNED,"+
		"`global_10project_rac` BIGINT UNSIGNED,"+
		"`global_20project_credit` BIGINT UNSIGNED,"+
		"`global_20project_rac` BIGINT UNSIGNED,"+
		"`global_country_credit` BIGINT UNSIGNED,"+
		"`global_country_rac` BIGINT UNSIGNED,"+
		"`global_joinyear_credit` BIGINT UNSIGNED,"+
		"`global_joinyear_rac` BIGINT UNSIGNED,"+
//		"`computer1_1project_credit` BIGINT UNSIGNED,"+
//		"`computer1_1project_rac` BIGINT UNSIGNED,"+
//		"`computer1_5project_credit` BIGINT UNSIGNED,"+
//		"`computer1_5project_rac` BIGINT UNSIGNED,"+
//		"`computer1_10project_credit` BIGINT UNSIGNED,"+
//		"`computer1_10project_rac` BIGINT UNSIGNED,"+
//		"`computer1_20project_credit` BIGINT UNSIGNED,"+
//		"`computer1_20project_rac` BIGINT UNSIGNED,"+
//		"`computer5_1project_credit` BIGINT UNSIGNED,"+
//		"`computer5_1project_rac` BIGINT UNSIGNED,"+
//		"`computer5_5project_credit` BIGINT UNSIGNED,"+
//		"`computer5_5project_rac` BIGINT UNSIGNED,"+
//		"`computer5_10project_credit` BIGINT UNSIGNED,"+
//		"`computer5_10project_rac` BIGINT UNSIGNED,"+
//		"`computer5_20project_credit` BIGINT UNSIGNED,"+
//		"`computer5_20project_rac` BIGINT UNSIGNED,"+
//		"`computer10_1project_credit` BIGINT UNSIGNED,"+
//		"`computer10_1project_rac` BIGINT UNSIGNED,"+
//		"`computer10_5project_credit` BIGINT UNSIGNED,"+
//		"`computer10_5project_rac` BIGINT UNSIGNED,"+
//		"`computer10_10project_credit` BIGINT UNSIGNED,"+
//		"`computer10_10project_rac` BIGINT UNSIGNED,"+
//		"`computer10_20project_credit` BIGINT UNSIGNED,"+
//		"`computer10_20project_rac` BIGINT UNSIGNED,"+
		" PRIMARY KEY  (`b_cpid_id`),"+
		"INDEX `country_id`(`country_id`),"+
		"INDEX `join_year`(`join_year`),"+
		"INDEX `user_cpid`(`user_cpid`),"+
		"INDEX `user_name`(`user_name`),"+
		"INDEX `active_project_count`(`active_project_count`)"+
//		"INDEX `active_computer_count`(`active_computer_count`)"+
		") ENGINE=MyISAM DEFAULT CHARSET=utf8;";
    	
    	Statement statement = null;
    	try {   
    		
            statement = cDBConnection.createStatement();
            
            log.debug("Creating table "+tablename);
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
            throw new SQLException("CreateCPIDTable()",s);
        }
    }
    
    public void CreateCTeamTable (String tablename) throws SQLException {
        
    	String query = "CREATE TABLE "+tablename;
    	query += " (`table_id` bigint(20) NOT NULL, `team_cpid` varchar(32) NOT NULL default '',"+
		" `name` varchar(255) NOT NULL default '',  `nusers` int(11) NOT NULL default '0',"+
		" `total_credit` bigint(20) NOT NULL default '0',  `rac` bigint(20) NOT NULL default '0',"+
		" `rac_time` bigint(20) NOT NULL default '0',"+
		" `project_count` smallint(6) NOT NULL default '0',  `active_project_count` smallint(6) NOT NULL default '0',"+
		" `create_time` int(11) NOT NULL default '0',"+
		" `country_id` smallint(6) NOT NULL default '0'," +
		" `global_credit` BIGINT UNSIGNED,"+
		" `global_rac` BIGINT UNSIGNED,"+	
		" `global_country_credit` BIGINT UNSIGNED,"+
		" `global_country_rac` BIGINT UNSIGNED,"+		
		" `global_1project_credit` BIGINT UNSIGNED,"+
		" `global_1project_rac` BIGINT UNSIGNED,"+
		" `global_5project_credit` BIGINT UNSIGNED,"+
		" `global_5project_rac` BIGINT UNSIGNED,"+
		" `global_10project_credit` BIGINT UNSIGNED,"+
		" `global_10project_rac` BIGINT UNSIGNED,"+
		" `global_20project_credit` BIGINT UNSIGNED,"+
		" `global_20project_rac` BIGINT UNSIGNED,"+		
		" PRIMARY KEY  (`table_id`)"+
		") ENGINE=MyISAM DEFAULT CHARSET=utf8;";
    	
    	Statement statement = null;
    	try {   
    		
            statement = cDBConnection.createStatement();
            
            log.debug("Creating table "+tablename);
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
            throw new SQLException("CreateCTeamTable()",s);
        }
    }
    
    public void CreateCPIDMapTable (String tablename) throws SQLException {
        
    	String query = "CREATE TABLE "+tablename;
    	query += " (`project_id` SMALLINT UNSIGNED NOT NULL,"+
		  "`user_id` BIGINT UNSIGNED NOT NULL,"+
		  "`b_cpid_id` BIGINT UNSIGNED NOT NULL,"+
		  "`user_cpid` VARCHAR(32) NOT NULL,"+
		  "`team_id` INTEGER UNSIGNED NOT NULL DEFAULT 0,"+
		  "`b_cteam_id` INTEGER UNSIGNED NOT NULL DEFAULT 0,"+
		  "`user_name` VARCHAR(255),"+
		  "`total_credit` BIGINT UNSIGNED DEFAULT 0,"+
		  "PRIMARY KEY (`project_id`, `user_id`),"+
		  "INDEX `user_name`(`user_name`),"+
		  "INDEX `b_cpid_id`(`b_cpid_id`),"+
		  //"INDEX `b_cteam_id`(`b_cteam_id`)"+
		  "INDEX `user_cpid`(`user_cpid`)"+
		  ")ENGINE = MyISAM DEFAULT CHARSET=utf8;";
    	
    	Statement statement = null;
    	try {   
    		
            statement = cDBConnection.createStatement();
            
            log.debug("Creating table "+tablename);
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
            throw new SQLException("CreateCPIDMapTable()",s);
        }
    }

    public void CreateUserRankingTCTable (String tablename) throws SQLException {
        
    	String query = "CREATE TABLE "+tablename;
    	query += " (`b_cpid_id` BIGINT UNSIGNED NOT NULL,"+
		  "`user_cpid` varchar(50) default NULL,"+
		  "`name` varchar(255) default NULL,"+
		  "`country_id` smallint(6) default NULL,"+
		  "`project_count` smallint(6) NOT NULL default '0',  `active_project_count` smallint(6) NOT NULL default '0',"+
		  "`join_date` varchar(10) NOT NULL default '0000-00-00',"+
		  "`join_year` varchar(4) NOT NULL default '',"+
		  "`active_computer_count` int(11) NOT NULL default '0',"+
		  "`total_credit` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`rac` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`rac_time` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`global_credit` BIGINT UNSIGNED,"+
		  "`global_new30days_credit` BIGINT UNSIGNED,"+
		  "`global_new90days_credit` BIGINT UNSIGNED,"+
		  "`global_new365days_credit` BIGINT UNSIGNED,"+
		  "`global_1project_credit` BIGINT UNSIGNED,"+
		  "`global_5project_credit` BIGINT UNSIGNED,"+
		  "`global_10project_credit` BIGINT UNSIGNED,"+
		  "`global_20project_credit` BIGINT UNSIGNED,"+
		  "`global_country_credit` BIGINT UNSIGNED,"+
		  "`global_joinyear_credit` BIGINT UNSIGNED,"+
//		  "`computer1_1project_credit` BIGINT UNSIGNED,"+
//		  "`computer1_5project_credit` BIGINT UNSIGNED,"+
//		  "`computer1_10project_credit` BIGINT UNSIGNED,"+
//		  "`computer1_20project_credit` BIGINT UNSIGNED,"+
//		  "`computer5_1project_credit` BIGINT UNSIGNED,"+
//		  "`computer5_5project_credit` BIGINT UNSIGNED,"+
//		  "`computer5_10project_credit` BIGINT UNSIGNED,"+
//		  "`computer5_20project_credit` BIGINT UNSIGNED,"+
//		  "`computer10_1project_credit` BIGINT UNSIGNED,"+
//		  "`computer10_5project_credit` BIGINT UNSIGNED,"+
//		  "`computer10_10project_credit` BIGINT UNSIGNED,"+
//		  "`computer10_20project_credit` BIGINT UNSIGNED,"+
		  "PRIMARY KEY (`b_cpid_id`),"+
		  "INDEX `country_id`(`country_id`),"+
		  "INDEX `join_year`(`join_year`),"+
		  "INDEX `global_credit`(`global_credit`),"+
		  "INDEX `global_new30days_credit`(`global_new30days_credit`),"+		  
		  "INDEX `global_new90days_credit`(`global_new90days_credit`),"+
		  "INDEX `global_new365days_credit`(`global_new365days_credit`),"+
		  "INDEX `global_1project_credit`(`global_1project_credit`),"+
		  "INDEX `global_5project_credit`(`global_5project_credit`),"+
		  "INDEX `global_10project_credit`(`global_10project_credit`),"+
		  "INDEX `global_20project_credit`(`global_20project_credit`),"+
		  "INDEX `global_country_credit`(`global_country_credit`),"+
		  "INDEX `global_joindyear_credit`(`global_joinyear_credit`)"+
//		  "INDEX `computer1_1project_credit`(`computer1_1project_credit`),"+
//		  "INDEX `computer1_5project_credit`(`computer1_5project_credit`),"+
//		  "INDEX `computer1_10project_credit`(`computer1_10project_credit`),"+
//		  "INDEX `computer1_20project_credit`(`computer1_20project_credit`),"+
//		  "INDEX `computer5_1project_credit`(`computer5_1project_credit`),"+
//		  "INDEX `computer5_5project_credit`(`computer5_5project_credit`),"+
//		  "INDEX `computer5_10project_credit`(`computer5_10project_credit`),"+
//		  "INDEX `computer5_20project_credit`(`computer5_20project_credit`),"+
//		  "INDEX `computer10_1project_credit`(`computer10_1project_credit`),"+
//		  "INDEX `computer10_5project_credit`(`computer10_5project_credit`),"+
//		  "INDEX `computer10_10project_credit`(`computer10_10project_credit`),"+
//		  "INDEX `computer10_20project_credit`(`computer10_20project_credit`)"+
		  ")ENGINE = MyISAM DEFAULT CHARSET=utf8;";
    	
    	Statement statement = null;
    	try {   
    		
            statement = cDBConnection.createStatement();
            
            log.debug("Creating table "+tablename);
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
            throw new SQLException("CreateUserRankingTable()",s);
        }
    }

    public void CreateUserRankingRacTable (String tablename) throws SQLException {
        
    	String query = "CREATE TABLE "+tablename;
    	query += " (`b_cpid_id` BIGINT UNSIGNED NOT NULL,"+
		  "`user_cpid` varchar(50) default NULL,"+
		  "`name` varchar(255) default NULL,"+
		  "`country_id` smallint(6) default NULL,"+
		  "`project_count` smallint(6) NOT NULL default '0',  `active_project_count` smallint(6) NOT NULL default '0',"+
		  "`join_date` varchar(10) NOT NULL default '0000-00-00',"+
		  "`join_year` varchar(4) NOT NULL default '',"+
		  "`active_computer_count` int(11) NOT NULL default '0',"+
		  "`total_credit` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`rac` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`rac_time` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`global_rac` BIGINT UNSIGNED,"+
		  "`global_new30days_rac` BIGINT UNSIGNED,"+
		  "`global_new90days_rac` BIGINT UNSIGNED,"+
		  "`global_new365days_rac` BIGINT UNSIGNED,"+
		  "`global_1project_rac` BIGINT UNSIGNED,"+
		  "`global_5project_rac` BIGINT UNSIGNED,"+
		  "`global_10project_rac` BIGINT UNSIGNED,"+
		  "`global_20project_rac` BIGINT UNSIGNED,"+
		  "`global_country_rac` BIGINT UNSIGNED,"+
		  "`global_joinyear_rac` BIGINT UNSIGNED,"+
//		  "`computer1_1project_rac` BIGINT UNSIGNED,"+
//		  "`computer1_5project_rac` BIGINT UNSIGNED,"+
//		  "`computer1_10project_rac` BIGINT UNSIGNED,"+
//		  "`computer1_20project_rac` BIGINT UNSIGNED,"+
//		  "`computer5_1project_rac` BIGINT UNSIGNED,"+
//		  "`computer5_5project_rac` BIGINT UNSIGNED,"+
//		  "`computer5_10project_rac` BIGINT UNSIGNED,"+
//		  "`computer5_20project_rac` BIGINT UNSIGNED,"+
//		  "`computer10_1project_rac` BIGINT UNSIGNED,"+
//		  "`computer10_5project_rac` BIGINT UNSIGNED,"+
//		  "`computer10_10project_rac` BIGINT UNSIGNED,"+
//		  "`computer10_20project_rac` BIGINT UNSIGNED,"+
		  "PRIMARY KEY (`b_cpid_id`),"+
		  "INDEX `country_id`(`country_id`),"+
		  "INDEX `join_year`(`join_year`),"+
		  "INDEX `global_rac`(`global_rac`),"+
		  "INDEX `global_new30days_rac`(`global_new30days_rac`),"+		  
		  "INDEX `global_new90days_rac`(`global_new90days_rac`),"+
		  "INDEX `global_new365days_rac`(`global_new365days_rac`),"+
		  "INDEX `global_1project_rac`(`global_1project_rac`),"+
		  "INDEX `global_5project_rac`(`global_5project_rac`),"+
		  "INDEX `global_10project_rac`(`global_10project_rac`),"+
		  "INDEX `global_20project_rac`(`global_20project_rac`),"+
		  "INDEX `global_country_rac`(`global_country_rac`),"+
		  "INDEX `global_joinyear_rac`(`global_joinyear_rac`)"+
//		  "INDEX `computer1_1project_rac`(`computer1_1project_rac`),"+
//		  "INDEX `computer1_5project_rac`(`computer1_5project_rac`),"+
//		  "INDEX `computer1_10project_rac`(`computer1_10project_rac`),"+
//		  "INDEX `computer1_20project_rac`(`computer1_20project_rac`),"+
//		  "INDEX `computer5_1project_rac`(`computer5_1project_rac`),"+
//		  "INDEX `computer5_5project_rac`(`computer5_5project_rac`),"+
//		  "INDEX `computer5_10project_rac`(`computer5_10project_rac`),"+
//		  "INDEX `computer5_20project_rac`(`computer5_20project_rac`),"+
//		  "INDEX `computer10_1project_rac`(`computer10_1project_rac`),"+
//		  "INDEX `computer10_5project_rac`(`computer10_5project_rac`),"+
//		  "INDEX `computer10_10project_rac`(`computer10_10project_rac`),"+
//		  "INDEX `computer10_20project_rac`(`computer10_20project_rac`)"+
		  ")ENGINE = MyISAM DEFAULT CHARSET=utf8;";
    	
    	Statement statement = null;
    	try {   
    		
            statement = cDBConnection.createStatement();
            
            log.debug("Creating table "+tablename);
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
            throw new SQLException("CreateUserRankingRacTable()",s);
        }
    }

    public void CreateCTeamRankingTCTable (String tablename) throws SQLException {
        
    	String query = "CREATE TABLE "+tablename;
    	query += " (`table_id` bigint(20) NOT NULL, `team_cpid` varchar(32) NOT NULL default '',"+
		" `name` varchar(255) NOT NULL default '',  `nusers` int(11) NOT NULL default '0',"+
		" `total_credit` bigint(20) NOT NULL default '0',  `rac` bigint(20) NOT NULL default '0',"+
		" `rac_time` bigint(20) NOT NULL default '0',"+
		" `project_count` smallint(6) NOT NULL default '0',  `active_project_count` smallint(6) NOT NULL default '0',"+
		" `create_time` int(11) NOT NULL default '0',"+
		" `country_id` smallint(6) NOT NULL default '0'," +
		" `global_credit` BIGINT UNSIGNED,"+
		" `global_country_credit` BIGINT UNSIGNED,"+
		" `global_1project_credit` BIGINT UNSIGNED,"+
		" `global_5project_credit` BIGINT UNSIGNED,"+
		" `global_10project_credit` BIGINT UNSIGNED,"+
		" `global_20project_credit` BIGINT UNSIGNED,"+
		" PRIMARY KEY  (`table_id`),"+
		"INDEX `global_credit`(`global_credit`),"+
		"INDEX `global_country_credit`(`global_country_credit`),"+
		"INDEX `global_1project_credit`(`global_1project_credit`),"+
		"INDEX `global_5project_credit`(`global_5project_credit`),"+
		"INDEX `global_10project_credit`(`global_10project_credit`),"+
		"INDEX `global_20project_credit`(`global_20project_credit`)"+
		") ENGINE=MyISAM DEFAULT CHARSET=utf8;";
    	
    	Statement statement = null;
    	try {   
    		
            statement = cDBConnection.createStatement();
            
            log.debug("Creating table "+tablename);
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
            throw new SQLException("CreateCTeamRankingTCTable()",s);
        }
    }

    public void CreateCTeamRankingRacTable (String tablename) throws SQLException {
        
    	String query = "CREATE TABLE "+tablename;
    	query += " (`table_id` bigint(20) NOT NULL, `team_cpid` varchar(32) NOT NULL default '',"+
		" `name` varchar(255) NOT NULL default '',  `nusers` int(11) NOT NULL default '0',"+
		" `total_credit` bigint(20) NOT NULL default '0',  `rac` bigint(20) NOT NULL default '0',"+
		" `rac_time` bigint(20) NOT NULL default '0',"+
		" `project_count` smallint(6) NOT NULL default '0',  `active_project_count` smallint(6) NOT NULL default '0',"+
		" `create_time` int(11) NOT NULL default '0',"+
		" `country_id` smallint(6) NOT NULL default '0'," +
		" `global_rac` BIGINT UNSIGNED,"+
		" `global_country_rac` BIGINT UNSIGNED,"+
		" `global_1project_rac` BIGINT UNSIGNED,"+
		" `global_5project_rac` BIGINT UNSIGNED,"+
		" `global_10project_rac` BIGINT UNSIGNED,"+
		" `global_20project_rac` BIGINT UNSIGNED,"+
		" PRIMARY KEY  (`table_id`),"+
		"INDEX `global_rac`(`global_rac`),"+
		"INDEX `global_country_rac`(`global_country_rac`),"+
		"INDEX `global_1project_rac`(`global_1project_rac`),"+
		"INDEX `global_5project_rac`(`global_5project_rac`),"+
		"INDEX `global_10project_rac`(`global_10project_rac`),"+
		"INDEX `global_20project_rac`(`global_20project_rac`)"+
		") ENGINE=MyISAM DEFAULT CHARSET=utf8;";
    	
    	Statement statement = null;
    	try {   
    		
            statement = cDBConnection.createStatement();
            
            log.debug("Creating table "+tablename);
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
            throw new SQLException("CreateCTeamRankingRacTable()",s);
        }
    }

    public void CreateHostMapTable (String tablename) throws SQLException {
        
    	String query = "CREATE TABLE "+tablename;
    	query += " (`project_id` INTEGER UNSIGNED NOT NULL,"+
		  "`host_id` BIGINT UNSIGNED NOT NULL,"+
		  "`host_cpid` VARCHAR(32) NOT NULL,"+
		  "`user_id` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`b_cpid_id` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`credit_per_cpu_sec` DOUBLE NOT NULL DEFAULT 0.0,"+
		  "PRIMARY KEY (`project_id`, `host_id`),"+
		  "INDEX `host_cpid`(`host_cpid`),"+
		  "INDEX `b_cpid_id`(`b_cpid_id`)"+
		  ")ENGINE = MyISAM DEFAULT CHARSET=utf8;";
    	
    	Statement statement = null;
    	try {   
    		
            statement = cDBConnection.createStatement();
            
            log.debug("Creating table "+tablename);
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
            throw new SQLException("CreateHostMapTable()",s);
        }
    }
    
    public void CreateCPCSTable (String tablename) throws SQLException {
        
    	String query = "CREATE TABLE "+tablename;
    	query += " (`project_id1` smallint(6) NOT NULL default '0',"+
		  "`project_id2` smallint(6) NOT NULL default '0',"+
		  "`sum1` double NOT NULL default '0',"+
		  "`sum2` double NOT NULL default '0',"+
		  "`count` int(11) NOT NULL default '0',"+
		  "`r_over_c` double NOT NULL default '0',"+
		  "PRIMARY KEY  (`project_id1`,`project_id2`)"+
		  ") ENGINE=MyISAM DEFAULT CHARSET=utf8;";
    	
    	Statement statement = null;
    	try {   
    		
            statement = cDBConnection.createStatement();
            
            log.debug("Creating table "+tablename);
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
            throw new SQLException("CreateCPCSTable()",s);
        }
    }
    
    public void CreateCountryRankTable (String tablename) throws SQLException {
        
    	String query = "CREATE TABLE "+tablename;
    	query += " (`country_id` int(11) NOT NULL default '0',"+
		  "`total_credit` bigint(20) NOT NULL default '0',"+
		  "`rac` bigint(20) NOT NULL default '0',"+
		  "`rank_tc` int(11) NOT NULL default '0',"+
		  "`rank_rac` int(11) NOT NULL default '0',"+
		  "`user_count` int(11) NOT NULL default '0',"+
		  "PRIMARY KEY  (`country_id`),"+
		  "KEY `rank_tc` (`rank_tc`,`rank_rac`)"+
		  ") ENGINE=MyISAM DEFAULT CHARSET=latin1;";
    	
    	Statement statement = null;
    	try {   
    		
            statement = cDBConnection.createStatement();
            
            log.debug("Creating table "+tablename);
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
            throw new SQLException("CreateCountryRankTable()",s);
        }
    }
    
    public void CreateIntHistoryTable (String tablename, String keyName) throws SQLException {
        
    	String query = "CREATE TABLE "+tablename;
    	query += " (`"+keyName+"` BIGINT UNSIGNED NOT NULL, "+
		  "`d_1` INT UNSIGNED NOT NULL default '0',"+
		  "`d_2` INT UNSIGNED NOT NULL default '0',"+
		  "`d_3` INT UNSIGNED NOT NULL default '0',"+
		  "`d_4` INT UNSIGNED NOT NULL default '0',"+
		  "`d_5` INT UNSIGNED NOT NULL default '0',"+
		  "`d_6` INT UNSIGNED NOT NULL default '0',"+
		  "`d_7` INT UNSIGNED NOT NULL default '0',"+
		  "`d_8` INT UNSIGNED NOT NULL default '0',"+
		  "`d_9` INT UNSIGNED NOT NULL default '0',"+
		  "`d_10` INT UNSIGNED NOT NULL default '0',"+
		  "`d_11` INT UNSIGNED NOT NULL default '0',"+
		  "`d_12` INT UNSIGNED NOT NULL default '0',"+
		  "`d_13` INT UNSIGNED NOT NULL default '0',"+
		  "`d_14` INT UNSIGNED NOT NULL default '0',"+
		  "`d_15` INT UNSIGNED NOT NULL default '0',"+
		  "`d_16` INT UNSIGNED NOT NULL default '0',"+
		  "`d_17` INT UNSIGNED NOT NULL default '0',"+
		  "`d_18` INT UNSIGNED NOT NULL default '0',"+
		  "`d_19` INT UNSIGNED NOT NULL default '0',"+
		  "`d_20` INT UNSIGNED NOT NULL default '0',"+
		  "`d_21` INT UNSIGNED NOT NULL default '0',"+
		  "`d_22` INT UNSIGNED NOT NULL default '0',"+
		  "`d_23` INT UNSIGNED NOT NULL default '0',"+
		  "`d_24` INT UNSIGNED NOT NULL default '0',"+
		  "`d_25` INT UNSIGNED NOT NULL default '0',"+
		  "`d_26` INT UNSIGNED NOT NULL default '0',"+
		  "`d_27` INT UNSIGNED NOT NULL default '0',"+
		  "`d_28` INT UNSIGNED NOT NULL default '0',"+
		  "`d_29` INT UNSIGNED NOT NULL default '0',"+
		  "`d_30` INT UNSIGNED NOT NULL default '0',"+
		  "`d_31` INT UNSIGNED NOT NULL default '0',"+
		  "`d_32` INT UNSIGNED NOT NULL default '0',"+
		  "`d_33` INT UNSIGNED NOT NULL default '0',"+
		  "`d_34` INT UNSIGNED NOT NULL default '0',"+
		  "`d_35` INT UNSIGNED NOT NULL default '0',"+
		  "`d_36` INT UNSIGNED NOT NULL default '0',"+
		  "`d_37` INT UNSIGNED NOT NULL default '0',"+
		  "`d_38` INT UNSIGNED NOT NULL default '0',"+
		  "`d_39` INT UNSIGNED NOT NULL default '0',"+
		  "`d_40` INT UNSIGNED NOT NULL default '0',"+
		  "`d_41` INT UNSIGNED NOT NULL default '0',"+
		  "`d_42` INT UNSIGNED NOT NULL default '0',"+
		  "`d_43` INT UNSIGNED NOT NULL default '0',"+
		  "`d_44` INT UNSIGNED NOT NULL default '0',"+
		  "`d_45` INT UNSIGNED NOT NULL default '0',"+
		  "`d_46` INT UNSIGNED NOT NULL default '0',"+
		  "`d_47` INT UNSIGNED NOT NULL default '0',"+
		  "`d_48` INT UNSIGNED NOT NULL default '0',"+
		  "`d_49` INT UNSIGNED NOT NULL default '0',"+
		  "`d_50` INT UNSIGNED NOT NULL default '0',"+
		  "`d_51` INT UNSIGNED NOT NULL default '0',"+
		  "`d_52` INT UNSIGNED NOT NULL default '0',"+
		  "`d_53` INT UNSIGNED NOT NULL default '0',"+
		  "`d_54` INT UNSIGNED NOT NULL default '0',"+
		  "`d_55` INT UNSIGNED NOT NULL default '0',"+
		  "`d_56` INT UNSIGNED NOT NULL default '0',"+
		  "`d_57` INT UNSIGNED NOT NULL default '0',"+
		  "`d_58` INT UNSIGNED NOT NULL default '0',"+
		  "`d_59` INT UNSIGNED NOT NULL default '0',"+
		  "`d_60` INT UNSIGNED NOT NULL default '0',"+
		  "`d_61` INT UNSIGNED NOT NULL default '0',"+
		  "`d_62` INT UNSIGNED NOT NULL default '0',"+
		  "`d_63` INT UNSIGNED NOT NULL default '0',"+
		  "`d_64` INT UNSIGNED NOT NULL default '0',"+
		  "`d_65` INT UNSIGNED NOT NULL default '0',"+
		  "`d_66` INT UNSIGNED NOT NULL default '0',"+
		  "`d_67` INT UNSIGNED NOT NULL default '0',"+
		  "`d_68` INT UNSIGNED NOT NULL default '0',"+
		  "`d_69` INT UNSIGNED NOT NULL default '0',"+
		  "`d_70` INT UNSIGNED NOT NULL default '0',"+
		  "`d_71` INT UNSIGNED NOT NULL default '0',"+
		  "`d_72` INT UNSIGNED NOT NULL default '0',"+
		  "`d_73` INT UNSIGNED NOT NULL default '0',"+
		  "`d_74` INT UNSIGNED NOT NULL default '0',"+
		  "`d_75` INT UNSIGNED NOT NULL default '0',"+
		  "`d_76` INT UNSIGNED NOT NULL default '0',"+
		  "`d_77` INT UNSIGNED NOT NULL default '0',"+
		  "`d_78` INT UNSIGNED NOT NULL default '0',"+
		  "`d_79` INT UNSIGNED NOT NULL default '0',"+
		  "`d_80` INT UNSIGNED NOT NULL default '0',"+
		  "`d_81` INT UNSIGNED NOT NULL default '0',"+
		  "`d_82` INT UNSIGNED NOT NULL default '0',"+
		  "`d_83` INT UNSIGNED NOT NULL default '0',"+
		  "`d_84` INT UNSIGNED NOT NULL default '0',"+
		  "`d_85` INT UNSIGNED NOT NULL default '0',"+
		  "`d_86` INT UNSIGNED NOT NULL default '0',"+
		  "`d_87` INT UNSIGNED NOT NULL default '0',"+
		  "`d_88` INT UNSIGNED NOT NULL default '0',"+
		  "`d_89` INT UNSIGNED NOT NULL default '0',"+
		  "`d_90` INT UNSIGNED NOT NULL default '0',"+
		  "`d_91` INT UNSIGNED NOT NULL default '0',"+
		  "`ymm_001` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_002` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_003` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_004` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_005` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_006` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_007` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_008` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_009` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_010` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_011` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_012` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_101` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_102` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_103` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_104` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_105` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_106` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_107` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_108` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_109` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_110` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_111` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_112` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_201` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_202` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_203` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_204` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_205` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_206` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_207` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_208` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_209` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_210` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_211` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_212` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_301` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_302` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_303` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_304` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_305` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_306` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_307` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_308` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_309` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_310` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_311` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_312` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_401` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_402` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_403` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_404` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_405` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_406` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_407` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_408` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_409` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_410` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_411` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_412` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_501` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_502` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_503` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_504` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_505` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_506` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_507` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_508` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_509` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_510` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_511` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_512` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_601` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_602` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_603` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_604` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_605` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_606` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_607` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_608` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_609` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_610` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_611` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_612` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_701` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_702` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_703` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_704` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_705` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_706` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_707` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_708` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_709` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_710` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_711` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_712` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_801` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_802` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_803` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_804` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_805` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_806` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_807` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_808` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_809` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_810` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_811` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_812` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_901` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_902` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_903` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_904` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_905` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_906` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_907` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_908` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_909` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_910` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_911` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_912` INT UNSIGNED NOT NULL DEFAULT 0,"+
		  "PRIMARY KEY (`"+keyName+"`))ENGINE = MyISAM DEFAULT CHARSET=utf8;";
    	
    	Statement statement = null;
    	try {   
    		
            statement = cDBConnection.createStatement();
            
            log.debug("Creating table "+tablename);
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
            throw new SQLException("CreateIntHistoryTable()",s);
        }
    }
    
    public void CreateBigIntHistoryTable (String tablename, String keyName) throws SQLException {
        
    	String query = "CREATE TABLE "+tablename;
    	query += " (`"+keyName+"` BIGINT UNSIGNED NOT NULL, "+
		  "`d_1` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_2` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_3` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_4` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_5` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_6` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_7` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_8` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_9` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_10` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_11` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_12` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_13` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_14` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_15` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_16` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_17` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_18` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_19` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_20` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_21` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_22` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_23` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_24` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_25` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_26` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_27` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_28` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_29` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_30` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_31` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_32` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_33` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_34` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_35` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_36` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_37` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_38` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_39` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_40` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_41` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_42` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_43` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_44` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_45` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_46` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_47` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_48` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_49` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_50` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_51` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_52` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_53` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_54` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_55` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_56` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_57` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_58` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_59` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_60` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_61` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_62` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_63` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_64` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_65` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_66` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_67` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_68` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_69` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_70` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_71` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_72` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_73` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_74` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_75` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_76` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_77` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_78` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_79` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_80` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_81` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_82` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_83` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_84` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_85` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_86` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_87` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_88` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_89` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_90` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_91` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`ymm_001` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_002` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_003` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_004` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_005` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_006` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_007` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_008` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_009` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_010` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_011` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_012` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_101` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_102` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_103` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_104` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_105` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_106` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_107` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_108` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_109` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_110` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_111` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_112` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_201` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_202` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_203` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_204` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_205` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_206` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_207` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_208` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_209` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_210` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_211` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_212` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_301` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_302` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_303` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_304` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_305` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_306` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_307` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_308` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_309` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_310` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_311` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_312` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_401` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_402` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_403` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_404` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_405` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_406` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_407` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_408` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_409` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_410` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_411` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_412` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_501` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_502` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_503` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_504` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_505` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_506` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_507` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_508` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_509` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_510` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_511` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_512` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_601` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_602` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_603` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_604` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_605` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_606` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_607` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_608` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_609` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_610` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_611` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_612` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_701` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_702` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_703` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_704` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_705` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_706` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_707` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_708` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_709` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_710` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_711` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_712` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_801` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_802` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_803` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_804` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_805` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_806` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_807` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_808` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_809` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_810` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_811` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_812` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_901` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_902` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_903` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_904` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_905` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_906` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_907` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_908` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_909` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_910` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_911` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "`ymm_912` BIGINT UNSIGNED NOT NULL DEFAULT 0,"+
		  "PRIMARY KEY (`"+keyName+"`))ENGINE = MyISAM DEFAULT CHARSET=utf8;";
    	
    	Statement statement = null;
    	try {   
    		
            statement = cDBConnection.createStatement();
            
            log.debug("Creating table "+tablename);
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
            throw new SQLException("CreateBigIntHistoryTable()",s);
        }
    }
    
    public void CreateRacHistoryTable (String tablename, String keyName) throws SQLException {
        
    	String query = "CREATE TABLE "+tablename;
    	query += " (`"+keyName+"` BIGINT UNSIGNED NOT NULL, "+
		  "`d_1` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_2` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_3` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_4` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_5` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_6` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_7` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_8` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_9` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_10` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_11` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_12` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_13` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_14` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_15` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_16` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_17` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_18` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_19` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_20` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_21` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_22` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_23` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_24` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_25` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_26` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_27` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_28` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_29` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_30` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_31` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_32` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_33` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_34` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_35` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_36` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_37` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_38` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_39` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_40` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_41` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_42` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_43` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_44` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_45` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_46` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_47` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_48` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_49` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_50` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_51` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_52` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_53` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_54` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_55` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_56` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_57` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_58` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_59` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_60` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_61` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_62` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_63` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_64` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_65` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_66` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_67` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_68` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_69` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_70` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_71` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_72` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_73` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_74` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_75` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_76` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_77` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_78` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_79` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_80` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_81` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_82` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_83` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_84` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_85` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_86` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_87` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_88` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_89` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_90` BIGINT UNSIGNED NOT NULL default '0',"+
		  "`d_91` BIGINT UNSIGNED NOT NULL default '0',"+
		  "PRIMARY KEY (`"+keyName+"`))ENGINE = MyISAM DEFAULT CHARSET=utf8;";
    	
    	Statement statement = null;
    	try {   
    		
            statement = cDBConnection.createStatement();
            
            log.debug("Creating table "+tablename);
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
            throw new SQLException("CreateRacHistoryTable()",s);
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
            throw new SQLException("RenameTable()", s);
        }
    }
    
    public void FlipTables (String tableOne, String tableTwo) throws SQLException {
        
    	boolean tableOneExists=true;
    	boolean tempTableExists=false;
    	boolean tableTwoExists=false;
    	
       // Check to see if oldName table exists
        try {
        	RunCountQuery("select count(*) as cnt from "+tableOne, "cnt");
        	tableOneExists = true;
        }
        catch (java.sql.SQLException s) {
        	tableOneExists = false;
        }
        
        // Check to see if newName table exists
        try {
        	RunCountQuery("select count(*) as cnt from "+tableTwo, "cnt");
        	tableTwoExists = true;
        }
        catch (java.sql.SQLException s) {
        	tableTwoExists = false;
        }
        
        // Check to see if tempName table exists
        try {
        	RunCountQuery("select count(*) as cnt from rentemp", "cnt");
        	tempTableExists = true;
        }
        catch (java.sql.SQLException s) {
        	tempTableExists = false;
        }
        
        if (tempTableExists == true) {
        	// failed something in the past?
        	// debugging and left around?
        	// Remove it.
        	DropTable("rentemp");
        }
        
        if (tableTwoExists == false && tableOneExists == false) {
        	// something's wrong, the one 
        	throw new SQLException("FlipAndDropTable()-Tables don't exist");
        }
        
        if (tableOneExists == true && tableTwoExists == false) {
        	RenameTable(tableOne,tableTwo);
        } else if (tableOneExists == false && tableTwoExists == true) {
        	RenameTable(tableTwo,tableOne);
        } else {
        	boolean firstRenameComplete = false;
        	boolean secondRenameComplete = false;
        	try {
        		RenameTable(tableOne,"rentemp");
        		firstRenameComplete = true;
        	}
        	catch (java.sql.SQLException s) {
        		log.error("Failed to do table rename");
        	}
        	if (firstRenameComplete == true) {
        		try {
            		RenameTable(tableTwo,tableOne);
            		secondRenameComplete = true;
            	}
            	catch (java.sql.SQLException s) {
            		log.error("Failed to do table rename");
            	}
        	}
        	if (firstRenameComplete == true && secondRenameComplete == true){
        		try {
            		RenameTable("rentemp",tableTwo);
            		firstRenameComplete = true;
            	}
            	catch (java.sql.SQLException s) {
            		log.error("Failed to do table rename");
            	}
        	} 
        	if (firstRenameComplete == true && secondRenameComplete == false) {
        		// failure, rename back
        		try {
            		RenameTable("rentemp",tableOne);
            		firstRenameComplete = true;
            	}
            	catch (java.sql.SQLException s) {
            		log.error("Failed to do table rename");
            	}
            	throw new SQLException("FlipAndDropTable()-Tables rename failed");
        	}
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
