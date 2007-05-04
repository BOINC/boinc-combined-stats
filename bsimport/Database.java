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

import java.sql.*;
import java.util.*;
import org.apache.log4j.Logger;
import java.util.zip.*;
import java.io.*;
import java.math.BigDecimal;

public class Database {

    String szConnectString = null;
    String szDBDriver = null;
    String szLogin = null;
    String szPassword = null;
    boolean bConnected = false;
    static Logger log = Logger.getLogger(Database.class.getName());  // log4j stuff
    
    private Connection cDBConnection;
    
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
        
        String query = "select pmodel_id,p_model from b_p_model";
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
        
        String query = "select pvendor_id,p_vendor from b_p_vendor";
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
        
        String query = "select osname_id,os_name from b_os_name";
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
            throw new SQLException("ReadOSNameTable - " + s.toString());
        }
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
            throw new SQLException("ReadCountryTable - " + s.toString());
        }
    }
    
   public void AddPModel(int table_id,  String p_model) throws SQLException {
        
        Statement statement = null;
        
        String query = "insert into b_p_model (p_model) values ('"+p_model+"')";
        
        
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

            //CommitTransaction(); 
            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("AddPModel - " + s.toString());
        }   
    }    
    public void AddPVendor(int table_id,  String p_vendor) throws SQLException {
        
        Statement statement = null;
        
        String query = "insert into b_p_vendor (p_vendor) values ('"+p_vendor+"')";
        
        
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

            //CommitTransaction();            
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("AddPVendor - " + s.toString());
        }
        
    }
        
    public void AddOSName(int table_id,  String os_name) throws SQLException {
        
        Statement statement = null;
        
        String query = "insert into b_os_name (os_name) values ('"+os_name+"')";
        
        
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

            //CommitTransaction();            
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("AddOSName - " + s.toString());
        }
        
    }
    

    
    public void AddCountry(int table_id,  String country) throws SQLException {
        
        Statement statement = null;
        
        String ccountry = country.replaceAll("'","\\\\'");
        String query = "insert into b_country (country) values ('"+ccountry+"')";
        
        
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

            //CommitTransaction();            
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("AddCountry - " + s.toString());
        }
        
    }
    
    public int UserUpdateRAC(int table_id,  String rac, String rac_time ) 
    throws SQLException {
    
        int updated_rows=0;
       
        Statement statement = null;
        String query = "update b_users set rac="+rac+",rac_time="+rac_time+" where table_id="+table_id;
        
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
            throw new SQLException("UserUpdateRAC - " + s.toString());
        }
    }

    
    public int UserUpdateEntry(int table_id, String name, int country_id, String user_cpid, String url, int teamid, int b_team_id, int b_cteam_id, String total_credit, String rac, String rac_time,int cpid_id ) 
        throws SQLException {
        
        int updated_rows=0;
        
        if (name.length() > 255) name = name.substring(0,254);
        if (url.length() > 200) url = url.substring(0,199);
        name = name.replaceAll("\\\\","\\\\\\\\");
        name = name.replaceAll("'", "\\\\'");        
        url = url.replaceAll("'","\\\\'");
        
        Statement statement = null;
        String query = "update b_users set name='"+name+"', country_id="+country_id+", user_cpid='"+user_cpid+"',url='"+url+"',teamid="+teamid;
        query += ",b_team_id="+b_team_id+",b_cteam_id="+b_cteam_id+",total_credit="+total_credit+",rac="+rac+",rac_time="+rac_time+",b_cpid_id="+cpid_id;
        if (teamid == 0)
            query += ",b_team_id=0,b_cteam_id=0 ";
        query +=" where table_id="+table_id;
        
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
            throw new SQLException("UserUpdateEntry - " + s.toString());
        }
    }
    
    public int UserAddEntry( int project_id, int user_id, String name, int country_id, String create_time, String user_cpid, String url,
            int teamid, int b_team_id,int b_cteam_id, String total_credit, String rac, String rac_time, int cpid_id) throws SQLException {
        
        Statement statement = null;
        int table_id=0;
        if (name.length() > 255) name = name.substring(0,254);
        if (url.length() > 200) url = url.substring(0,199);
        name = name.replaceAll("\\\\","\\\\\\\\");
        name = name.replaceAll("'", "\\\\'");
        url = url.replaceAll("'","\\\\'");
        
        
        String query = "insert into b_users (project_id,user_id,name,country_id,create_time,user_cpid,url,teamid,b_team_id,b_cteam_id,total_credit,rac,rac_time,b_cpid_id) values ("+
            project_id+","+user_id+",'"+name+"',"+country_id+","+create_time+",'"+user_cpid+"','"+url+"',"+teamid+","+b_team_id+","+b_cteam_id+","+total_credit+","+rac+","+rac_time+","+cpid_id+")";
        
        
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
            throw new SQLException("AddUserEntry - " + s.toString());
        }
    }
    
    public void UserRead(Hashtable<String, User> myUsers, int project_id) throws SQLException {
        
        myUsers.clear();
        try { Thread.sleep(1000); }
        catch (Exception e) {}
        String query = "select table_id,user_id,user_cpid from b_users where project_id="+project_id;
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
                u.table_id =  resultSet.getInt("table_id");
                u.user_id = resultSet.getInt("user_id");
                u.cpid = resultSet.getString("user_cpid");
                u.bCpidChange = false;
                String t = (String) new Integer(u.user_id).toString();
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

    public void UserReadFix(Vector<RacFix> myUsers, int project_id) throws SQLException {
        
        myUsers.clear();
        try { Thread.sleep(1000); }
        catch (Exception e) {}
        String query = "select table_id,total_credit,rac,rac_time from b_users where project_id="+project_id;
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
                RacFix u = new RacFix();
                u.table_id =  resultSet.getInt("table_id");
                u.total_credit = resultSet.getInt("total_credit");
                u.rac = resultSet.getInt("rac");
                u.rac_time = resultSet.getInt("rac_time");
                myUsers.add(u);                
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
    
    public void UserAddTotalCreditHistory(int table_id, int day, int week, String total_credit) 
    throws SQLException {
    
        Statement statement = null;
        
        String query = "insert into b_users_total_credit_hist (b_users_id,w_"+week+",d_"+day+") values ("+table_id+","+total_credit+","+total_credit+")";
        
        try {           
            statement = cDBConnection.createStatement();
            int res = statement.executeUpdate(query);
            if (res != 1) {
                //RollbackTransaction(); // problems, should only be 1 row
                log.debug("Number of rows updated is wrong: "+res);
            }
    
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
            throw new SQLException("UserAddTotalCreditHistory - " + s.toString());
        }
    }
    
    public int UserUpdateTotalCreditHistory(int table_id, int day, int week, String total_credit) 
    throws SQLException {
    
        Statement statement = null;
        int updated_rows=0;
        
        String query;
        if (week == 0)
            query = "update b_users_total_credit_hist set d_"+day+"="+total_credit+" where b_users_id="+table_id;
        else
            query = "update b_users_total_credit_hist set w_"+week+"="+total_credit+",d_"+day+"="+total_credit+" where b_users_id="+table_id;
        
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
            throw new SQLException("UserUpdateTotalCreditHistory - " + s.toString());
        }
    }
    
    public void UserAddRACHistory(int table_id, int day, String rac) 
    throws SQLException {
    
        Statement statement = null;
        
        String query = "insert into b_users_rac_hist (b_users_id,d_"+day+") values ("+table_id+","+rac+")";
        
        try {           
            statement = cDBConnection.createStatement();
            int res = statement.executeUpdate(query);
            if (res != 1) {
                //RollbackTransaction(); // problems, should only be 1 row
                log.debug("Number of rows updated is wrong: "+res);
            }
    
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
            throw new SQLException("UserAddRACHistory - " + s.toString());
        }
    }

    public int UserUpdateRACHistory(int table_id, int day, String rac) 
    throws SQLException {
    
        Statement statement = null;
        int updated_rows;
        
        String query = "update b_users_rac_hist set d_"+day+"="+rac+" where b_users_id="+table_id;
        
        
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
            throw new SQLException("UserUpdateRACHistory - " + s.toString());
        }
    }    
    
    public int UserUpdateTeam(int table_id, int team, int cteam_table_id) 
    throws SQLException {
    
        Statement statement = null;
        int updated_rows=0;
        
        String query = "update b_users set b_team_id="+team+", b_cteam_id="+cteam_table_id+" where table_id="+table_id;
        
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
            throw new SQLException("UpdateUserTeam - " + s.toString());
        }
    }    

    public int HostAddEntry(int project_id, int host_id, int user_id, int b_users_id, int p_vendor_id, int p_model_id,
            int os_name_id, String os_version, String create_time, String rpc_time, String timezone, int ncpus, String p_fpops,
            String p_iops, String p_membw, String m_nbytes, String m_cache, String m_swap, String d_total, String d_free,
            String n_bwup, String n_bwdown, String avg_turnaround, String host_cpid, String total_credit, String rac, String rac_time, 
            int cpid_id, String credit_per_cpu_sec) 
        throws SQLException {
        
        Statement statement = null;
        
        if (timezone.length()==0) timezone="0";
        
        int table_id=0;
        String query = "insert into b_hosts (project_id,host_id,user_id,b_users_id,p_vendor,p_model,os_name,os_version,create_time,rpc_time,timezone,ncpus,p_fpops,p_iops,p_membw,m_nbytes,m_cache,m_swap,d_total,d_free,n_bwup,n_bwdown,avg_turnaround,host_cpid,total_credit,rac,rac_time,b_cpid_id,credit_per_cpu_sec) values ("+
            project_id+","+host_id+","+user_id+","+b_users_id+","+p_vendor_id+","+p_model_id+","+os_name_id+",'"+os_version+"',"+create_time+","+rpc_time+","+timezone+","+ncpus
            +","+p_fpops+","+p_iops+","+p_membw+","+m_nbytes+","+m_cache+","+m_swap+","+d_total+","+d_free+","+n_bwup+","+n_bwdown+","+avg_turnaround
            +",'"+host_cpid+"',"+total_credit+","+rac+","+rac_time+","+cpid_id+","+credit_per_cpu_sec
            +")";
        
        
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
            throw new SQLException("AddHostEntry - " + s.toString());
        }
    }
    
    public int HostUpdateRAC(int table_id, String rac, String rac_time) 
    throws SQLException {
    
        int updated_rows=0;
        Statement statement = null;
        String query = "update b_hosts set rac="+rac+",rac_time="+rac_time+" where table_id="+table_id;
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
            throw new SQLException("HostUpdateRAC - " + s.toString());
        }
    }
    
    public int HostUpdateEntry(int table_id, int p_vendor_id, int p_model_id, int os_name_id, String os_version, String rpc_time,
            String timezone, int ncpus, String p_fpops, String p_iops, String p_membw, String m_nbytes, String m_cache, String m_swap, 
            String d_total, String d_free, String n_bwup, String n_bwdown, String avg_turnaround, String host_cpid, String total_credit,
            String rac, String rac_time, int cpid_id, int b_users_id, String credit_per_cpu_sec) 
    throws SQLException {
    
        int updated_rows=0;
        Statement statement = null;
        String query = "update b_hosts set p_vendor="+p_vendor_id+",p_model="+p_model_id+",os_name="+os_name_id
            +",rpc_time="+rpc_time+",timezone="+timezone+",ncpus="+ncpus+",p_fpops="+p_fpops+",p_iops="+p_iops
            +",p_membw="+p_membw+",m_nbytes="+m_nbytes+",m_cache="+m_cache+",m_swap="+m_swap+",d_total="+d_total
            +",d_free="+d_free+",n_bwup="+n_bwup+",n_bwdown="+n_bwdown+",avg_turnaround="+avg_turnaround
            +",host_cpid='"+host_cpid+"',total_credit="+total_credit+",rac="+rac+",rac_time="+rac_time
            +",b_cpid_id="+cpid_id+",b_users_id="+b_users_id+",credit_per_cpu_sec="+credit_per_cpu_sec+" where table_id="+table_id;
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
            throw new SQLException("HostUpdateEntry - " + s.toString());
        }
    }

    public int HostDeleteEntry(int table_id) 
    throws SQLException {
    
        Statement statement = null;
        String query = "delete from b_hosts where table_id="+table_id;
        int deleted_rows=0;
        
        try {           
            statement = cDBConnection.createStatement();
            deleted_rows = statement.executeUpdate(query);
            if (deleted_rows != 1) {
                //RollbackTransaction(); // problems, should only be 1 row
                log.debug("Number of rows deleted is wrong: "+deleted_rows);
            }
    
            // clean up
            statement.close();
            statement = null;
    
            //CommitTransaction();            
            return deleted_rows;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("HostDeleteEntry - " + s.toString());
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
    public void HostReadFix(Vector<RacFix> myHosts, int project_id) throws SQLException {
        
        myHosts.clear();
        try { Thread.sleep(1000); }
        catch (Exception e) {}
        String query = "select table_id,total_credit,rac,rac_time from b_hosts where rac > 0 and project_id="+project_id;
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
                RacFix u = new RacFix();
                u.table_id =  resultSet.getInt("table_id");
                u.total_credit = resultSet.getInt("total_credit");
                u.rac = resultSet.getInt("rac");
                u.rac_time = resultSet.getInt("rac_time");
                myHosts.add(u);                
            } 
            resultSet.close();
            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("HostReadFix() - " + s.toString());
        }        
    }
    
    public int TeamAddUserHistEntry( int project_id, int team_id, int team_table_id, int user_id, int b_users_id) throws SQLException {
        
        Statement statement = null;
        int table_id=0;
        

        
        String query = "insert into b_teams_users_hist (project_id,user_id,b_users_id,team_id,b_team_id,hist_date) values ("+
            project_id+","+user_id+","+b_users_id+","+team_id+","+team_table_id+",sysdate())";
        
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
            throw new SQLException("TeamAddUserHistEntry - " + s.toString());
        }
    }
    
    public int TeamUpdateRAC(int table_id, String rac, String rac_time) 
    throws SQLException {
    
        int updated_rows = 0;
         
        Statement statement = null;
        String query = "update b_teams set rac="+rac+",rac_time="+rac_time+" where table_id="+table_id;
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
            throw new SQLException("TeamUpdateRAC - " + s.toString());
        }
    }
    
    public int TeamUpdateEntry(int table_id, int cteam_table_id,int type, String name,int founder_id, int founder_b_user_id, String total_credit,
            String rac, String rac_time, int nusers, String founder_name, String create_time, String url, String description,
            int country_id) 
    throws SQLException {
    
        int updated_rows = 0;
        if (name.length() > 255) name = name.substring(0,254);
        if (founder_name.length() > 100) founder_name = founder_name.substring(0,99);
        if (url.length() > 200) url = url.substring(0,199);
        if (description.length() > 200) description = description.substring(0,199);
        name = name.replaceAll("\\\\","\\\\\\\\");
        name = name.replaceAll("'", "\\\\'");
        founder_name = founder_name.replaceAll("\\\\","\\\\\\\\");
        founder_name = founder_name.replaceAll("'", "\\\\'");
        url = url.replaceAll("'","\\\\'");
        description = description.replaceAll("\\\\","\\\\\\\\");
        description = description.replaceAll("'", "\\\\'");
        
        Statement statement = null;
        String query = "update b_teams set b_cteam_id="+cteam_table_id+",type="+type+",name='"+name+"',founder_id="+founder_id+",founder_b_user_id="+founder_b_user_id+
            ",total_credit="+total_credit+",rac="+rac+",rac_time="+rac_time+",nusers="+nusers+",founder_name='"+founder_name+
            "',create_time="+create_time+",url='"+url+"',description='"+description+"',country_id="+country_id+
            " where table_id="+table_id;
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
            throw new SQLException("TeamUpdateEntry - " + s.toString());
        }
    }
    
    public int TeamAddEntry( int project_id, int team_id, int cteam_table_id, int type, String name, int founder_id, int founder_b_user_id,
            String total_credit, String rac, String rac_time, int nusers, String founder_name, String create_time, String url, String description,
            int country_id) throws SQLException {
        
        Statement statement = null;
        int table_id=0;
        
        if (name.length() > 255) name = name.substring(0,254);
        if (founder_name.length() > 100) founder_name = founder_name.substring(0,99);
        if (url.length() > 200) url = url.substring(0,199);
        if (description.length() > 200) description = description.substring(0,199);
        name = name.replaceAll("\\\\","\\\\\\\\");
        name = name.replaceAll("'", "\\\\'");
        founder_name = founder_name.replaceAll("\\\\","\\\\\\\\");
        founder_name = founder_name.replaceAll("'", "\\\\'");
        url = url.replaceAll("'","\\\\'");
        description = description.replaceAll("\\\\","\\\\\\\\");
        description = description.replaceAll("'", "\\\\'");
        
        String query = "insert into b_teams (project_id,team_id,b_cteam_id,type,name,founder_id,founder_b_user_id,total_credit,rac,rac_time,nusers,founder_name,create_time,url,description,country_id) values ("+
            project_id+","+team_id+","+cteam_table_id+","+type+",'"+name+"',"+founder_id+","+founder_b_user_id+","+total_credit+","+rac+","+rac_time+","+
            nusers+",'"+founder_name+"',"+create_time+",'"+url+"','"+description+"',"+country_id+")";
        
        
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
            throw new SQLException("TeamAddEntry - " + s.toString());
        }
    }
    
    

    
    public int TeamDeleteEntry(int table_id) 
    throws SQLException {
    
        Statement statement = null;
        String query = "delete from b_teams where table_id="+table_id;
        int deleted_entries=0;
        
        try {           
            statement = cDBConnection.createStatement();
            deleted_entries = statement.executeUpdate(query);
            if (deleted_entries != 1) {
                //RollbackTransaction(); // problems, should only be 1 row
                log.debug("Number of rows deleted is wrong: "+deleted_entries);
            }
    
            // clean up
            statement.close();
            statement = null;
    
            //CommitTransaction();            
            return deleted_entries;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("TeamDeleteEntry - " + s.toString());
        }
    }
    
    public void TeamRead(Hashtable<String, Team> myTeams, int project_id) throws SQLException {
        
        myTeams.clear();
        System.gc();
        try { Thread.sleep(1000); }
        catch (Exception e) {}
        
        String query = "select table_id,team_id,b_cteam_id from b_teams where project_id="+project_id;
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
                t.table_id =  resultSet.getInt("table_id");
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
    public void TeamReadFix(Vector<RacFix> myTeams, int project_id) throws SQLException {
        
        myTeams.clear();
        try { Thread.sleep(1000); }
        catch (Exception e) {}
        String query = "select table_id,total_credit,rac,rac_time from b_teams where project_id="+project_id;
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
                RacFix u = new RacFix();
                u.table_id =  resultSet.getInt("table_id");
                u.total_credit = resultSet.getInt("total_credit");
                u.rac = resultSet.getInt("rac");
                u.rac_time = resultSet.getInt("rac_time");
                myTeams.add(u);                
            } 
            resultSet.close();
            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("TeamReadFix() - " + s.toString());
        }        
    }


    public void TeamAddTotalCreditHistory(int table_id, int day, int week, String total_credit) 
    throws SQLException {
    
        Statement statement = null;
        
        String query = "insert into b_teams_total_credit_hist (b_team_id,w_"+week+",d_"+day+") values ("+table_id+","+total_credit+","+total_credit+")";
        
        try {           
            statement = cDBConnection.createStatement();
            int res = statement.executeUpdate(query);
            if (res != 1) {
                //RollbackTransaction(); // problems, should only be 1 row
                log.debug("Number of rows updated is wrong: "+res);
            }
    
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
            throw new SQLException("TeamAddTotalCreditHistory - " + s.toString());
        }
    }
    
    public int TeamUpdateTotalCreditHistory(int table_id, int day, int week, String total_credit) 
    throws SQLException {
    
        Statement statement = null;
        int updated_rows=0;
        
        String query;
        if (week == 0)
            query = "update b_teams_total_credit_hist set d_"+day+"="+total_credit+" where b_team_id="+table_id;
        else
            query = "update b_teams_total_credit_hist set w_"+week+"="+total_credit+",d_"+day+"="+total_credit+" where b_team_id="+table_id;
        
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
            throw new SQLException("TeamUpdateTotalCreditHistory - " + s.toString());
        }
    }
    

    
    public void TeamAddRACHistory(int table_id, int day, String rac) 
    throws SQLException {
    
        Statement statement = null;
        
        String query = "insert into b_teams_rac_hist (b_team_id,d_"+day+") values ("+table_id+","+rac+")";
        
        try {           
            statement = cDBConnection.createStatement();
            int res = statement.executeUpdate(query);
            if (res != 1) {
                //RollbackTransaction(); // problems, should only be 1 row
                log.debug("Number of rows updated is wrong: "+res);
            }
    
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
            throw new SQLException("TeamAddRACHistory - " + s.toString());
        }
    }

    public int TeamUpdateRACHistory(int table_id, int day, String rac) 
    throws SQLException {
    
        Statement statement = null;
        int updated_rows;
        
        String query = "update b_teams_rac_hist set d_"+day+"="+rac+" where b_team_id="+table_id;
        
        
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
            throw new SQLException("TeamUpdateRACHistory - " + s.toString());
        }
    }  

    public void CTeamRead(Hashtable<String, CTeam> myCTeams) throws SQLException {
        
        myCTeams.clear();
        String query = "select table_id,name from b_cteams";
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
                
                myCTeams.put(t.name.toLowerCase(),t);
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
    
    public int CTeamGetTeamIDFromDB(String name) throws SQLException {
        
       
        Statement statement = null;
        ResultSet resultSet = null;
        int teamid=0;

        
        if (name.length() > 255) name = name.substring(0,254);
        name = name.replaceAll("\\\\","\\\\\\\\");
        name = name.replaceAll("'", "\\\\'");
        
        String query = "select table_id from b_cteams where name='"+name+"'";
        
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
            throw new SQLException("CTeamGetTeamIDFromDB() - " + s.toString());
        }        
    }
    
    public int CTeamAddEntry( String name) throws SQLException {
        
        Statement statement = null;
        int table_id=0;
        
        if (name.length() > 255) name = name.substring(0,254);
        name = name.replaceAll("\\\\","\\\\\\\\");
        name = name.replaceAll("'", "\\\\'");
        
        String query = "insert into b_cteams (name) values ('"+name+"')";
        
        
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
            throw new SQLException("CTeamAddEntry - " + s.toString());
        }
    }
    

    public void CPIDRead(Hashtable<String, CPID> myCPIDs) throws SQLException {
        
        myCPIDs.clear();
        String query = "select table_id,user_cpid from b_cpid";
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
                c.table_id =  resultSet.getInt("table_id");                
                c.user_cpid = resultSet.getString("user_cpid");  
                c.bFound = false;
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
    
    public int CPIDAddEntry(String cpid, String total_credit, String rac, String rac_time) throws SQLException {
        
        Statement statement = null;
        int table_id=0;
        
        String query = "insert into b_cpid (user_cpid,total_credit,rac,rac_time,project_count) values ('"+
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
    
    public void CPIDAddTotalCreditHistory(int table_id, int day, int week, String total_credit) 
    throws SQLException {
    
        Statement statement = null;
        
        String query = "insert into b_cpid_total_credit_hist (b_cpid_id,w_"+week+",d_"+day+") values ("+table_id+","+total_credit+","+total_credit+")";
        
        try {           
            statement = cDBConnection.createStatement();
            int res = statement.executeUpdate(query);
            if (res != 1) {
                //RollbackTransaction(); // problems, should only be 1 row
                log.debug("Number of rows updated is wrong: "+res);
            }
    
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
            throw new SQLException("CPIDAddTotalCredit - " + s.toString());
        }
    }
    
    public int CPIDUpdateTotalCreditHistory(int table_id, int day, int week, String total_credit) 
    throws SQLException {
    
        Statement statement = null;
        int updated_rows=0;
        
        String query;
        if (week == 0)
            query = "update b_cpid_total_credit_hist set d_"+day+"="+total_credit+" where b_cpid_id="+table_id;
        else
            query = "update b_cpid_total_credit_hist set w_"+week+"="+total_credit+",d_"+day+"="+total_credit+" where b_cpid_id="+table_id;
        
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
            throw new SQLException("CPIDUpdateTotalCreditHistory - " + s.toString());
        }
    }
    
    public int CPIDChangeCPID(int table_id, String new_cpid) 
    throws SQLException {
    
        Statement statement = null;
        int updated_rows=0;
        
        String query = "update b_cpid set user_cpid='"+new_cpid+"' where table_id="+table_id;
        
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
    
    public void CPIDAddRACHistory(int table_id, int day, String rac) 
    throws SQLException {
    
        Statement statement = null;
        
        String query = "insert into b_cpid_rac_hist (b_cpid_id,d_"+day+") values ("+table_id+","+rac+")";
        
        try {           
            statement = cDBConnection.createStatement();
            int res = statement.executeUpdate(query);
            if (res != 1) {
                //RollbackTransaction(); // problems, should only be 1 row
                log.debug("Number of rows updated is wrong: "+res);
            }
    
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
            throw new SQLException("CPIDAddRACHistory - " + s.toString());
        }
    }

    public int CPIDUpdateRACHistory(int table_id, int day, String rac) 
    throws SQLException {
    
        Statement statement = null;
        int updated_rows;
        
        String query = "update b_cpid_rac_hist set d_"+day+"="+rac+" where b_cpid_id="+table_id;
        
        
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
            throw new SQLException("CPIDUpdateRACHistory - " + s.toString());
        }
    }
    
    public void AddToCPIDHistory(int project_id,int user_id,String old_cpid, String new_cpid) throws SQLException {
        
        Statement statement = null;
        
        String query = "insert into b_users_cpid_history (project_id,user_id,old_cpid,new_cpid,changetime) values ("+
            project_id+","+user_id+",'"+old_cpid+"','"+new_cpid+"',sysdate())";
        
        
        try {           
            statement = cDBConnection.createStatement();
            int res = statement.executeUpdate(query);
            if (res != 1) {
                //RollbackTransaction(); // problems, should only be 1 row
                log.debug("Number of rows updated is wrong: "+res);
            }

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
            log.error(s);
            throw new SQLException("AddToCPIDHistory - " + s.toString());
        }
    }
    
    public int ProjectUpdateTime(int project_id, String updateTime) 
    throws SQLException {
    
        Statement statement = null;
        int updated_rows;
        
        String query = "update b_projects set last_update='"+updateTime+"' where project_id="+project_id;
        
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
    
    public void ProjectsRead(Vector<Project> myProjects) throws SQLException {
        
        myProjects.clear();
        String query = "select project_id,name,host_file,user_file,team_file,shortname,data_dir,active,retired,gr from b_projects where active='Y'";
        
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
    
    public void CalculateCPCSTable() throws SQLException {
      
        String query;
        Statement statement = null;
        ResultSet resultSet = null;
        int pmax=0;
        
        log.info("Starting Credit_per_cpu_sec analysis");
        try {            
        
            // Clear b_hosts_cpcs table
            statement = cDBConnection.createStatement();
            query = "truncate table b_hosts_cpcs";
            statement.execute(query);
            
            // populate b_hosts_cpcs with new data
            query = "insert into b_hosts_cpcs (select project_id,host_cpid,credit_per_cpu_sec from b_hosts where host_cpid <> '' and credit_per_cpu_sec > 0.0 and rac > 0)";
            statement.execute(query);
            statement.close();
            
            // get max project_id
            query = "select max(project_id) as pmax from b_projects";
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
            
            // read in the b_hosts_cpcs, ordered by host_cpid
            // for each host_cpid/project pair, update the matrix
            Vector <Integer> projectIDs = new Vector<Integer>();
            Vector <String> vcpcs = new Vector<String>();
            String last_cpid="";
            
            query = "select project_id,host_cpid,credit_per_cpu_sec from b_hosts_cpcs order by host_cpid";
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
    
            
            //for (int i=1;i<=pmax;i++) {
            //    for (int j=1;j<=pmax;j++)
            //        System.out.print("["+counts[i][j]+"]");
            //    System.out.println("");
            //}
            
            //for (int i=1;i<=pmax;i++) {
            //    for (int j=1;j<=pmax;j++) {
            //        double v1,v2;
            //        if (counts[i][j] > 0) {
            //            v1 = sum1[i][j] / (double)counts[i][j];
            //            v2 = sum2[i][j] / (double)counts[i][j];
            //            double res = (v1/v2);
            //            System.out.print("["+res+"]");
            //        } else
            //        System.out.print("[x]");
            //    }
            //    System.out.println("");
            //}
            
            // truncate table b_cpcs
            query = "truncate table b_cpcs";
            statement = cDBConnection.createStatement();
            int updated_rows = statement.executeUpdate(query);
            if (updated_rows != 1) {
                //RollbackTransaction(); // problems, should only be 1 row
                log.debug("Number of rows updated is wrong in CalculateCPCSTable: "+updated_rows);
            }
    
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
                        
                        query = "insert into b_cpcs (project_id1,project_id2,sum1,sum2,count,r_over_c) "+
                                "values ("+i+","+j+","+sum1[i][j].toString()+","+sum2[i][j].toString()+","+counts[i][j]+","+res+")";
                        
                        statement = cDBConnection.createStatement();
                        updated_rows = statement.executeUpdate(query);
                        if (updated_rows != 1) {
                            //RollbackTransaction(); // problems, should only be 1 row
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
            throw new SQLException("CalculateCPCSTable() - " + s.toString());        
        }
        
        log.info("CPCS Analysis Complete");
           
    } 
    public void DoProjectRankings (int project_id) throws SQLException {
        
        String query = "set @rownum=0";
        String query2 ="update b_users, (select table_id,@rownum := @rownum + 1 as rank from b_users where project_id="+project_id+" order by total_credit desc) pr set b_users.project_rank_credit=pr.rank where b_users.table_id=pr.table_id;";
        String query3 ="update b_users, (select table_id,@rownum := @rownum + 1 as rank from b_users where project_id="+project_id+" order by rac desc) pr set b_users.project_rank_rac=pr.rank where b_users.table_id=pr.table_id;";
        
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
            throw new SQLException("DoProjectRankings() - " + s.toString());
        }                
    }
    
    public void DoProjectStats () throws SQLException {
        
        String query1 = "update b_projects, (select project_id, total_credit, user_count, host_count, team_count, country_count, active_users, active_hosts, active_teams, rac from b_projects) y "+
            "set b_projects.y_total_credit=y.total_credit, b_projects.y_user_count=y.user_count,b_projects.y_host_count=y.host_count,b_projects.y_team_count=y.team_count,b_projects.y_country_count=y.country_count, "+
            "b_projects.y_active_users=y.active_users,b_projects.y_active_hosts=y.active_hosts,b_projects.y_active_teams=y.active_teams, b_projects.y_rac=y.rac where b_projects.project_id=y.project_id";
        String query2 = "update b_projects, (select project_id, count(*) as cnt, sum(total_credit) as tc, sum(rac) as rc from b_users group by project_id) cc "+
            "set b_projects.user_count=cc.cnt, b_projects.total_credit=cc.tc, b_projects.rac=cc.rc where b_projects.project_id=cc.project_id";
        String query3 = "update b_projects, (select project_id, count(*) as cnt from b_users where rac > 0 group by project_id) pc set b_projects.active_users=pc.cnt where b_projects.project_id=pc.project_id";
        String query4 = "update b_projects, (select project_id, count(*) as cnt from b_hosts group by project_id) pc set b_projects.host_count=pc.cnt where b_projects.project_id=pc.project_id";
        String query5 = "update b_projects, (select project_id, count(*) as cnt from b_teams group by project_id) pc set b_projects.team_count=pc.cnt where b_projects.project_id=pc.project_id";
        String query6 = "update b_projects, (select project_id, count(*) as cnt from b_hosts where rac > 0 group by project_id) pc set b_projects.active_hosts=pc.cnt where b_projects.project_id=pc.project_id";
        String query7 = "update b_projects, (select project_id, count(*) as cnt from b_teams where rac > 0 group by project_id) pc set b_projects.active_teams=pc.cnt where b_projects.project_id=pc.project_id";
        
        Statement statement = null;

        
        try {            
            statement = cDBConnection.createStatement();
            
            log.info("Updating yesterday project counts");
            statement.execute(query1);
            log.info("Getting project total credit, rac, user counts");
            statement.execute(query2);
            log.info("Getting project active user counts");
            statement.execute(query3);
            log.info("Getting project host counts");
            statement.execute(query4);
            log.info("Getting project team counts");
            statement.execute(query5);
            log.info("Getting project active hosts counts");
            statement.execute(query6);
            log.info("Getting project active teams counts");
            statement.execute(query7);

            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("DoProjectStats() - " + s.toString());
        }                
    }
    
    public void DoTeamStats (int year_day) throws SQLException {
        
        
        String query1 = "update b_cteams, (select b_cteam_id, sum(total_credit) as tc, sum(rac) as rc, max(rac_time) as rt from b_teams group by b_cteam_id) cc set b_cteams.total_credit=cc.tc, b_cteams.rac=cc.rc, b_cteams.rac_time=cc.rt where b_cteams.table_id=b_cteam_id";        
        String query2 = "update b_cteams, (select b_cteam_id, count(*) as cnt from b_users where b_cteam_id > 0 group by b_cteam_id) cc set b_cteams.nusers=cc.cnt where b_cteams.table_id=cc.b_cteam_id";
        String query3 = "truncate table b_rank_team_tc";
        String query4 = "truncate table b_rank_team_rac";
        String query5 = "set @rownum=0";
        String query6 = "insert into b_rank_team_tc (rank,b_team_id) (select @rownum := @rownum + 1 as rank, table_id from b_cteams where project_count > 0 order by total_credit desc)";
        String query7 = "insert into b_rank_team_rac (rank,b_team_id) (select @rownum := @rownum + 1 as rank, table_id from b_cteams where project_count > 0 order by rac desc)";
        String query8 = "insert into b_cteams_nusers_hist (b_team_id) (select b_cteams.table_id from b_cteams left join b_cteams_nusers_hist on b_cteams.table_id=b_cteams_nusers_hist.b_team_id where b_cteams_nusers_hist.b_team_id is null)";
        String query9 = "insert into b_cteams_rac_hist (b_team_id) (select b_cteams.table_id from b_cteams left join b_cteams_rac_hist on b_cteams.table_id=b_cteams_rac_hist.b_team_id where b_cteams_rac_hist.b_team_id is null)";
        String querya = "insert into b_cteams_total_credit_hist (b_team_id) (select b_cteams.table_id from b_cteams left join b_cteams_total_credit_hist on b_cteams.table_id=b_cteams_total_credit_hist.b_team_id where b_cteams_total_credit_hist.b_team_id is null)";
        String queryb = "update b_cteams_rac_hist, (select table_id,rac from b_cteams) cc set b_cteams_rac_hist.d_"+year_day+"=cc.rac where b_cteams_rac_hist.b_team_id=cc.table_id";
        String queryc = "update b_cteams_total_credit_hist, (select table_id,total_credit from b_cteams) cc set b_cteams_total_credit_hist.d_"+year_day+"=cc.total_credit where b_cteams_total_credit_hist.b_team_id=cc.table_id";
        String queryd = "update b_cteams_nusers_hist, (select table_id,nusers from b_cteams) cc set b_cteams_nusers_hist.d_"+year_day+"=cc.nusers where b_cteams_nusers_hist.b_team_id=cc.table_id";
        String querye = "update b_cteams set project_count=0";
        String queryf = "update b_cteams, (select b_cteam_id, count(*) as cnt from b_teams where b_cteam_id > 0 group by b_cteam_id) cc set b_cteams.project_count=cc.cnt where b_cteams.table_id=cc.b_cteam_id";
        
        Statement statement = null;

        
        try {            
            statement = cDBConnection.createStatement();
            
            log.info("Updating combined teams total credit, rac");
            statement.execute(query1);
            log.info("Updating combined teams user count");
            statement.execute(query2);
            log.info("Updating combined team ranks");
            statement.execute(query3);
            statement.execute(query4);
            statement.execute(query5);
            statement.execute(query6);
            statement.execute(query5);
            statement.execute(query7);
            log.info("Updating combined team histories");
            statement.execute(query8);
            statement.execute(query9);
            statement.execute(querya);
            statement.execute(queryb);
            statement.execute(queryc);
            statement.execute(queryd);
            log.info("Updating combined team project counts");
            statement.execute(querye);
            statement.execute(queryf);
            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("DoTeamStats() - " + s.toString());
        }                
    }
    
    public void DoUpdateComputerCounts () throws SQLException {
        
        String query = "update b_users set computer_count=0,active_computer_count=0";
        String query2 ="update b_users,(select b_users_id,count(*) as cnt from b_hosts where b_users_id > 0 group by b_users_id) cc set b_users.computer_count = cc.cnt where b_users.table_id=cc.b_users_id";
        String query3 ="update b_users,(select b_users_id,count(*) as cnt from b_hosts where b_users_id > 0 and rac > 0 group by b_users_id) cc set b_users.active_computer_count = cc.cnt where b_users.table_id=cc.b_users_id";
        
        Statement statement = null;

        
        try {            
            statement = cDBConnection.createStatement();

            statement.execute(query);
            statement.execute(query2);
            statement.execute(query3);
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
    

    public void DoUpdateTeamUserCounts (int project_id) throws SQLException {
        
        String query = "update b_teams set nusers=0 where project_id="+project_id;
        String query2 ="update b_teams,(select b_team_id,count(*) as cnt from b_users where project_id="+project_id+" and b_team_id > 0 group by b_team_id) cc set b_teams.nusers = cc.cnt where b_teams.table_id=cc.b_team_id";
        
        Statement statement = null;

        
        try {            
            statement = cDBConnection.createStatement();

            statement.execute(query);
            statement.execute(query2);
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


    public void DoCPIDDataUpdates (int tc_day, int rac_day, int week) throws SQLException {
        
        // make sure all the rac/tc hist are in there
        String query = "insert into b_cpid_rac_hist (b_cpid_id) (select table_id from b_cpid left join b_cpid_rac_hist on b_cpid.table_id=b_cpid_rac_hist.b_cpid_id where b_cpid_rac_hist.b_cpid_id is null)";
        String query2 ="insert into b_cpid_total_credit_hist (b_cpid_id) (select table_id from b_cpid left join b_cpid_total_credit_hist on b_cpid.table_id=b_cpid_total_credit_hist.b_cpid_id where b_cpid_total_credit_hist.b_cpid_id is null)";

        // update project_count, total_credit, rac, rac_time (this takes a bit)
        String query3a = "update b_cpid set project_count=0,active_project_count=0";
        String query3b ="update b_cpid, (select b_cpid_id, count(*) as cnt, sum(total_credit) as tc, sum(rac) as rs, max(rac_time) as rt from b_users group by b_cpid_id) cc "+
            "set b_cpid.project_count=cc.cnt,b_cpid.total_credit=cc.tc, b_cpid.rac=cc.rs, b_cpid.rac_time=cc.rt "+
            "where b_cpid.table_id=cc.b_cpid_id";
        
        // active project count
        String query4 = "update b_cpid, (select user_cpid, count(*) as proj_cnt from b_users where rac > 0 group by user_cpid) cc "+
            "set b_cpid.active_project_count=cc.proj_cnt where b_cpid.user_cpid=cc.user_cpid";
        
        // drop any entreis that have a project_count of 0?
        
        // computer count
        //String query5 = "truncate table b_hosts_combined";
        //String query6 = "insert into b_hosts_combined (select host_cpid, b_cpid_id,sum(total_credit),sum(rac),max(rac_time) from b_hosts group by host_cpid)";
        //String query7 = "update b_cpid, (select b_cpid_id, count(*) as cnt from b_hosts_combined group by b_cpid_id) cc set b_cpid.computer_count=cc.cnt where b_cpid.table_id=cc.b_cpid_id";
        // active computer count
        //String query8 = "update b_cpid, (select b_cpid_id, count(*) as cnt from b_hosts_combined where rac > 0 group by b_cpid_id) cc set b_cpid.active_computer_count=cc.cnt where b_cpid.table_id=cc.b_cpid_id";        
        
        String query5 = "update b_cpid set computer_count=0,active_computer_count=0";
        String query6 = "update b_cpid, (select b_cpid_id, count(*) as host_count from (select distinct host_cpid,b_cpid_id from b_hosts where b_cpid_id > 0 and rac > 0) hc group by b_cpid_id) cc set b_cpid.active_computer_count=cc.host_count where b_cpid.table_id=cc.b_cpid_id";
        String query7 = "update b_cpid, (select b_cpid_id, count(*) as host_count from (select distinct host_cpid,b_cpid_id from b_hosts where b_cpid_id > 0) hc group by b_cpid_id) cc set b_cpid.computer_count=cc.host_count where b_cpid.table_id=cc.b_cpid_id";
        
        // now store tc,rac history
        String query9  = "update b_cpid_rac_hist, b_cpid set b_cpid_rac_hist.d_"+rac_day+"=b_cpid.rac where b_cpid_rac_hist.b_cpid_id=b_cpid.table_id";
        String query10 = "update b_cpid_total_credit_hist, b_cpid set b_cpid_total_credit_hist.d_"+tc_day+"=b_cpid.total_credit, b_cpid_total_credit_hist.w_"+week+"=b_cpid.total_credit where b_cpid_total_credit_hist.b_cpid_id=b_cpid.table_id";
        
        // do world rank calculations
        String query11 = "truncate table b_rank_user_tc_p0c0";
        String query12 = "set @rownum=0";
        String query13 = "insert into b_rank_user_tc_p0c0 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where project_count > 0 order by total_credit desc)";
        String query14 = "truncate table b_rank_user_rac_p0c0";
        String query15 = "insert into b_rank_user_rac_p0c0 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where project_count > 0 order by rac desc)";

        String query64 = "truncate table b_rank_user_tc_p2c0";
        String query65 = "insert into b_rank_user_tc_p2c0 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where active_project_count >=2 order by total_credit desc)";
        String query66 = "truncate table b_rank_user_rac_p2c0";
        String query67 = "insert into b_rank_user_rac_p2c0 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where active_project_count >=2  order by rac desc)";
        String query68 = "truncate table b_rank_user_tc_p5c0";
        String query69 = "insert into b_rank_user_tc_p5c0 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where active_project_count >= 5 order by total_credit desc)";
        String query70 = "truncate table b_rank_user_rac_p5c0";
        String query71 = "insert into b_rank_user_rac_p5c0 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where active_project_count >= 5 order by rac desc)";

        
        // more rankings
        String query16 = "truncate table b_rank_user_tc_p1c1";
        String query17 = "insert into b_rank_user_tc_p1c1 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where project_count > 0 and computer_count=1 order by total_credit desc)";
        String query18 = "truncate table b_rank_user_tc_p1c5";
        String query19 = "insert into b_rank_user_tc_p1c5 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where project_count > 0 and computer_count <=5 order by total_credit desc)";
        String query20 = "truncate table b_rank_user_tc_p1c10";
        String query21 = "insert into b_rank_user_tc_p1c10 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where project_count > 0 and computer_count <=10 order by total_credit desc)";
        String query22 = "truncate table b_rank_user_tc_p1c20";
        String query23 = "insert into b_rank_user_tc_p1c20 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where project_count > 0 and computer_count <=20 order by total_credit desc)";

        String query24 = "truncate table b_rank_user_tc_p2c1";
        String query25 = "insert into b_rank_user_tc_p2c1 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where active_project_count >=2 and computer_count=1 order by total_credit desc)";
        String query26 = "truncate table b_rank_user_tc_p2c5";
        String query27 = "insert into b_rank_user_tc_p2c5 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where active_project_count >=2 and computer_count <=5 order by total_credit desc)";
        String query28 = "truncate table b_rank_user_tc_p2c10";
        String query29 = "insert into b_rank_user_tc_p2c10 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where active_project_count >=2 and computer_count <=10 order by total_credit desc)";
        String query30 = "truncate table b_rank_user_tc_p2c20";
        String query31 = "insert into b_rank_user_tc_p2c20 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where active_project_count >=2 and computer_count <=20 order by total_credit desc)";

        String query32 = "truncate table b_rank_user_tc_p5c1";
        String query33 = "insert into b_rank_user_tc_p5c1 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where active_project_count >=5 and computer_count=1 order by total_credit desc)";
        String query34 = "truncate table b_rank_user_tc_p5c5";
        String query35 = "insert into b_rank_user_tc_p5c5 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where active_project_count >=5 and computer_count <=5 order by total_credit desc)";
        String query36 = "truncate table b_rank_user_tc_p5c10";
        String query37 = "insert into b_rank_user_tc_p5c10 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where active_project_count >=5 and computer_count <=10 order by total_credit desc)";
        String query38 = "truncate table b_rank_user_tc_p5c20";
        String query39 = "insert into b_rank_user_tc_p5c20 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where active_project_count >=5 and computer_count <=20 order by total_credit desc)";
        
        // by rac this time
        String query40 = "truncate table b_rank_user_rac_p1c1";
        String query41 = "insert into b_rank_user_rac_p1c1 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where project_count > 0 and computer_count=1 order by rac desc)";
        String query42 = "truncate table b_rank_user_rac_p1c5";
        String query43 = "insert into b_rank_user_rac_p1c5 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where project_count > 0 and computer_count <=5 order by rac desc)";
        String query44 = "truncate table b_rank_user_rac_p1c10";
        String query45 = "insert into b_rank_user_rac_p1c10 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where project_count > 0 and computer_count <=10 order by rac desc)";
        String query46 = "truncate table b_rank_user_rac_p1c20";
        String query47 = "insert into b_rank_user_rac_p1c20 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where project_count > 0 and computer_count <=20 order by rac desc)";

        String query48 = "truncate table b_rank_user_rac_p2c1";
        String query49 = "insert into b_rank_user_rac_p2c1 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where active_project_count >=2 and computer_count=1 order by rac desc)";
        String query50 = "truncate table b_rank_user_rac_p2c5";
        String query51 = "insert into b_rank_user_rac_p2c5 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where active_project_count >=2 and computer_count <=5 order by rac desc)";
        String query52 = "truncate table b_rank_user_rac_p2c10";
        String query53 = "insert into b_rank_user_rac_p2c10 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where active_project_count >=2 and computer_count <=10 order by rac desc)";
        String query54 = "truncate table b_rank_user_rac_p2c20";
        String query55 = "insert into b_rank_user_rac_p2c20 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where active_project_count >=2 and computer_count <=20 order by rac desc)";

        String query56 = "truncate table b_rank_user_rac_p5c1";
        String query57 = "insert into b_rank_user_rac_p5c1 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where active_project_count >=5 and computer_count=1 order by rac desc)";
        String query58 = "truncate table b_rank_user_rac_p5c5";
        String query59 = "insert into b_rank_user_rac_p5c5 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where active_project_count >=5 and computer_count <=5 order by rac desc)";
        String query60 = "truncate table b_rank_user_rac_p5c10";
        String query61 = "insert into b_rank_user_rac_p5c10 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where active_project_count >=5 and computer_count <=10 order by rac desc)";
        String query62 = "truncate table b_rank_user_rac_p5c20";
        String query63 = "insert into b_rank_user_rac_p5c20 (rank,b_cpid_id) (select @rownum := @rownum + 1 as rank, table_id from b_cpid where active_project_count >=5 and computer_count <=20 order by rac desc)";
 
        
        Statement statement = null;

        
        try {            
            statement = cDBConnection.createStatement();

            log.info("Making sure all CPIDs have history entries");
            statement.execute(query);
            statement.execute(query2);
            log.info("Updating cpid project count, total credit, rac, rac time");
            statement.execute(query3a);
            statement.execute(query3b);
            log.info("Updating cpid active project count");
            statement.execute(query4);
            //log.info("Building combined host table");
            //statement.execute(query5);
            //statement.execute(query6);
            //log.info("Updating cpid computer count");
            //statement.execute(query7);
            //log.info("Updating cpid active computer counts");
            //statement.execute(query8);
            log.info("Zeroing cpid computer counts");
            statement.execute(query5);
            log.info("Updating cpid active computer counts");
            statement.execute(query6);
            log.info("Updating cpid computer counts");
            statement.execute(query7);
            log.info("storing cpid rac and total_credit");
            statement.execute(query9);
            statement.execute(query10);
            log.info("Doing world rank by total credit");
            statement.execute(query11);
            statement.execute(query12);
            statement.execute(query13);
            log.info("Doing world rank by rac");
            statement.execute(query14);
            statement.execute(query12);
            statement.execute(query15);
            log.info("Doing world rank by tc 1 computer");
            statement.execute(query16);
            statement.execute(query12);
            statement.execute(query17);
            log.info("Doing world rank by tc <= 5 computers");
            statement.execute(query18);
            statement.execute(query12);
            statement.execute(query19);
            log.info("Doing world rank by tc <= 10 computers");
            statement.execute(query20);
            statement.execute(query12);
            statement.execute(query21);
            log.info("Doing world rank by tc <= 20 computers");
            statement.execute(query22);
            statement.execute(query12);
            statement.execute(query23);
            log.info("Doing world rank by tc 1 computer 2+ projects");
            statement.execute(query24);
            statement.execute(query12);
            statement.execute(query25);
            log.info("Doing world rank by tc <= 5 computers 2+ projects");
            statement.execute(query26);
            statement.execute(query12);
            statement.execute(query27);
            log.info("Doing world rank by tc <= 10 computers 2+ projects");
            statement.execute(query28);
            statement.execute(query12);
            statement.execute(query29);
            log.info("Doing world rank by tc <= 20 computers 2+ projects");
            statement.execute(query30);
            statement.execute(query12);
            statement.execute(query31);
            log.info("Doing world rank by tc 1 computer 5+ projects");
            statement.execute(query32);
            statement.execute(query12);
            statement.execute(query33);
            log.info("Doing world rank by tc <= 5 computers 5+ projects");
            statement.execute(query34);
            statement.execute(query12);
            statement.execute(query35);
            log.info("Doing world rank by tc <= 10 computers 5+ projects");
            statement.execute(query36);
            statement.execute(query12);
            statement.execute(query37);
            log.info("Doing world rank by tc <= 20 computers 5+ projects");
            statement.execute(query38);
            statement.execute(query12);
            statement.execute(query39);
            
            log.info("Doing world rank by rac 1 computer");
            statement.execute(query40);
            statement.execute(query12);
            statement.execute(query41);
            log.info("Doing world rank by rac <= 5 computers");
            statement.execute(query42);
            statement.execute(query12);
            statement.execute(query43);
            log.info("Doing world rank by rac <= 10 computers");
            statement.execute(query44);
            statement.execute(query12);
            statement.execute(query45);
            log.info("Doing world rank by rac <= 20 computers");
            statement.execute(query46);
            statement.execute(query12);
            statement.execute(query47);
            log.info("Doing world rank by rac 1 computer 2+ projects");
            statement.execute(query48);
            statement.execute(query12);
            statement.execute(query49);
            log.info("Doing world rank by rac <= 5 computers 2+ projects");
            statement.execute(query50);
            statement.execute(query12);
            statement.execute(query51);
            log.info("Doing world rank by rac <= 10 computers 2+ projects");
            statement.execute(query52);
            statement.execute(query12);
            statement.execute(query53);
            log.info("Doing world rank by rac <= 20 computers 2+ projects");
            statement.execute(query54);
            statement.execute(query12);
            statement.execute(query55);
            log.info("Doing world rank by rac 1 computer 5+ projects");
            statement.execute(query56);
            statement.execute(query12);
            statement.execute(query57);
            log.info("Doing world rank by rac <= 5 computers 5+ projects");
            statement.execute(query58);
            statement.execute(query12);
            statement.execute(query59);
            log.info("Doing world rank by rac <= 10 computers 5+ projects");
            statement.execute(query60);
            statement.execute(query12);
            statement.execute(query61);
            log.info("Doing world rank by rac <= 20 computers 5+ projects");
            statement.execute(query62);
            statement.execute(query12);
            statement.execute(query63);
            
            log.info("Doing world rank by tc 1+ computer 2+ projects");
            statement.execute(query64);
            statement.execute(query12);
            statement.execute(query65);
            log.info("Doing world rank by rac 1+ computers 2+ projects");
            statement.execute(query66);
            statement.execute(query12);
            statement.execute(query67);
            log.info("Doing world rank by rac 1+ computers 5+ projects");
            statement.execute(query68);
            statement.execute(query12);
            statement.execute(query69);
            log.info("Doing world rank by rac 1+ computers 5+ projects");
            statement.execute(query70);
            statement.execute(query12);
            statement.execute(query71);
            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("DoCPIDDataUpdates() - " + s.toString());
        }                
    }
    
public void DoCombinedStatsProjectUpdates (int tc_day, int rac_day, int week) throws SQLException {
        
    
        // update user count, total credit, rac
        String query1 = "update b_projects, (select count(*) as cnt, sum(total_credit) as tc, sum(rac) as rc from b_cpid where project_count > 0) cc set b_projects.user_count=cc.cnt, b_projects.total_credit=cc.tc,b_projects.rac=cc.rc where b_projects.project_id=19";
        String query2 = "update b_projects, (select count(*) as cnt from b_cpid where project_count > 0 and rac > 0) cc set b_projects.active_users=cc.cnt where b_projects.project_id=19";
        
        // update country count
        String query3 = "update b_projects, (select count(*) as cnt from (select distinct country_id from b_users) uc) cc set b_projects.country_count=cc.cnt where b_projects.project_id=19";
        
        // update computer count
        String query4 = "update b_projects, (select sum(computer_count) as cnt from b_cpid) cc set b_projects.host_count=cc.cnt where b_projects.project_id=19";
        String query5 = "update b_projects, (select sum(active_computer_count) as cnt from b_cpid) cc set b_projects.active_hosts=cc.cnt where b_projects.project_id=19";
                
        
        // update team counts
        String query6 = "update b_projects, (select count(*) as cnt from b_cteams where project_count > 0) cc set b_projects.team_count=cc.cnt where b_projects.project_id=19";
        String query7 = "update b_projects, (select count(*) as cnt from b_cteams where project_count > 0 and rac > 0) cc set b_projects.active_teams=cc.cnt where b_projects.project_id=19";
        

        Statement statement = null;

        
        try {            
            statement = cDBConnection.createStatement();

            log.info("updating project 19 user count, total credit, rac");
            statement.execute(query1);
            log.info("updating project 19 active user count");
            statement.execute(query2);
            log.info("updating project 19 country count");
            statement.execute(query3);
            log.info("Updating project 19 computer count");
            statement.execute(query4);
            log.info("Updating project 19 active computer count");
            statement.execute(query5);
            log.info("Updating project 19 team count");
            statement.execute(query6);
            log.info("Updating project 19 active team count");
            statement.execute(query7);
            statement.close();

            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("DoCombinedStatsProjectUpdates() - " + s.toString());
        }                
    }

    public void DoGRCombinedStatsProjectUpdates (int tc_day, int rac_day, int week) throws SQLException {
        

        // Update b_cpid table to mark all GR users
        String query0 = "update b_cpid,(select distinct a.b_cpid_id from b_users a, b_projects b where a.project_id=b.project_id and b.gr='Y') grusers set b_cpid.gr='Y' where b_cpid.table_id=grusers.b_cpid_id";
        
        // update the b_cteams table to mark all GR teams
        String query1 = "update b_cteams,(select distinct a.b_cteam_id from b_teams a, b_projects b where a.project_id=b.project_id and b.gr='Y') grteams set b_cteams.gr='Y' where b_cteams.table_id=grteams.b_cteam_id";
        
        // update user count, total credit, rac
        String query2 = "update b_projects, (select sum(total_credit) as tc, sum(rac) as rc from b_projects where gr='Y') cc set b_projects.total_credit=cc.tc,b_projects.rac=cc.rc where b_projects.project_id=39";
        String query3 = "update b_projects, (select count(*) as cnt from b_cpid where project_count > 0 and rac > 0 and gr='Y') cc set b_projects.active_users=cc.cnt where b_projects.project_id=39";
        String query4 = "update b_projects, (select count(*) as cnt from b_cpid where project_count > 0 and gr='Y') cc set b_projects.user_count=cc.cnt where b_projects.project_id=39";
        
        // update computer count
        String query5 = "update b_projects, (select sum(computer_count) as cnt from b_cpid where gr='Y') cc set b_projects.host_count=cc.cnt where b_projects.project_id=39";
        String query6 = "update b_projects, (select sum(active_computer_count) as cnt from b_cpid where gr='Y') cc set b_projects.active_hosts=cc.cnt where b_projects.project_id=39";
                
        // update team counts
        String query7 = "update b_projects, (select count(*) as cnt from b_cteams where project_count > 0 and gr='Y') cc set b_projects.team_count=cc.cnt where b_projects.project_id=39";
        String query8 = "update b_projects, (select count(*) as cnt from b_cteams where project_count > 0 and rac > 0 and gr='Y') cc set b_projects.active_teams=cc.cnt where b_projects.project_id=39";

        // update country count
        //String query9 = "update b_projects, (select count(*) as cnt from (select distinct country_id from b_users) uc) cc set b_projects.country_count=cc.cnt where b_projects.project_id=39";

    
        Statement statement = null;
    
        
        try {            
            statement = cDBConnection.createStatement();
    
            log.info("Marking GR project users");
            statement.execute(query0);
            log.info("Marking GR project teams");
            statement.execute(query1);
            log.info("updating project 39 total credit, rac");
            statement.execute(query2);
            log.info("updating project 39 active user count");
            statement.execute(query3);
            log.info("updating project 39 user count");
            statement.execute(query4);            
            log.info("Updating project 39 computer count");
            statement.execute(query5);
            log.info("Updating project 39 active computer count");
            statement.execute(query6);
            log.info("Updating project 39 team count");
            statement.execute(query7);
            log.info("Updating project 39 active team count");
            statement.execute(query8);
//          log.info("updating project 39 country count");
//          statement.execute(query9);
            statement.close();
    
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("DoCombinedStatsProjectUpdates() - " + s.toString());
        }                
    }

    public void StoreProjectHistories (int year_day) throws SQLException {
    
    
        // make sure there is an entry for each project
        String query1 = "insert into b_project_active_hosts_hist (project_id) (select b_projects.project_id from b_projects left join b_project_active_hosts_hist on b_projects.project_id=b_project_active_hosts_hist.project_id where b_project_active_hosts_hist.project_id is null)";
        String query2 = "insert into b_project_active_teams_hist (project_id) (select b_projects.project_id from b_projects left join b_project_active_teams_hist on b_projects.project_id=b_project_active_teams_hist.project_id where b_project_active_teams_hist.project_id is null)";
        String query3 = "insert into b_project_active_users_hist (project_id) (select b_projects.project_id from b_projects left join b_project_active_users_hist on b_projects.project_id=b_project_active_users_hist.project_id where b_project_active_users_hist.project_id is null)";
        String query4 = "insert into b_project_country_hist (project_id) (select b_projects.project_id from b_projects left join b_project_country_hist on b_projects.project_id=b_project_country_hist.project_id where b_project_country_hist.project_id is null)";        
        String query5 = "insert into b_project_hosts_hist (project_id) (select b_projects.project_id from b_projects left join b_project_hosts_hist on b_projects.project_id=b_project_hosts_hist.project_id where b_project_hosts_hist.project_id is null)";
        String query6 = "insert into b_project_teams_hist (project_id) (select b_projects.project_id from b_projects left join b_project_teams_hist on b_projects.project_id=b_project_teams_hist.project_id where b_project_teams_hist.project_id is null)";
        String query7 = "insert into b_project_rac_hist (project_id) (select b_projects.project_id from b_projects left join b_project_rac_hist on b_projects.project_id=b_project_rac_hist.project_id where b_project_rac_hist.project_id is null)";
        String query8 = "insert into b_project_teams_hist (project_id) (select b_projects.project_id from b_projects left join b_project_teams_hist on b_projects.project_id=b_project_teams_hist.project_id where b_project_teams_hist.project_id is null)";
        String query9 = "insert into b_project_total_credit_hist (project_id) (select b_projects.project_id from b_projects left join b_project_total_credit_hist on b_projects.project_id=b_project_total_credit_hist.project_id where b_project_total_credit_hist.project_id is null)";
        String querya = "insert into b_project_users_hist (project_id) (select b_projects.project_id from b_projects left join b_project_users_hist on b_projects.project_id=b_project_users_hist.project_id where b_project_users_hist.project_id is null)";
        
        // store the data
        String queryb = "update b_project_active_users_hist, (select project_id,active_users as cnt from b_projects) cc set d_"+year_day+"=cc.cnt where b_project_active_users_hist.project_id=cc.project_id";
        String queryc = "update b_project_active_hosts_hist, (select project_id,active_hosts as cnt from b_projects) cc set d_"+year_day+"=cc.cnt where b_project_active_hosts_hist.project_id=cc.project_id";
        String queryd = "update b_project_active_teams_hist, (select project_id,active_teams as cnt from b_projects) cc set d_"+year_day+"=cc.cnt where b_project_active_teams_hist.project_id=cc.project_id";
        String querye = "update b_project_users_hist, (select project_id,user_count as cnt from b_projects) cc set d_"+year_day+"=cc.cnt where b_project_users_hist.project_id=cc.project_id";
        String queryf = "update b_project_hosts_hist, (select project_id,host_count as cnt from b_projects) cc set d_"+year_day+"=cc.cnt where b_project_hosts_hist.project_id=cc.project_id";
        String queryg = "update b_project_teams_hist, (select project_id,team_count as cnt from b_projects) cc set d_"+year_day+"=cc.cnt where b_project_teams_hist.project_id=cc.project_id";
        String queryh = "update b_project_country_hist, (select project_id,country_count as cnt from b_projects) cc set d_"+year_day+"=cc.cnt where b_project_country_hist.project_id=cc.project_id";
        String queryi = "update b_project_rac_hist, (select project_id,rac as cnt from b_projects) cc set d_"+year_day+"=cc.cnt where b_project_rac_hist.project_id=cc.project_id";
        String queryj = "update b_project_total_credit_hist, (select project_id,total_credit as cnt from b_projects) cc set d_"+year_day+"=cc.cnt where b_project_total_credit_hist.project_id=cc.project_id";
        
    
        Statement statement = null;
    
        try {            
            statement = cDBConnection.createStatement();
    
            statement.execute(query1);
            statement.execute(query2);
            statement.execute(query3);
            statement.execute(query4);
            statement.execute(query5);
            statement.execute(query6);
            statement.execute(query7);
            statement.execute(query8);
            statement.execute(query9);
            statement.execute(querya);
            statement.execute(queryb);
            statement.execute(queryc);
            statement.execute(queryd);
            statement.execute(querye);
            statement.execute(queryf);
            statement.execute(queryg);
            statement.execute(queryh);
            statement.execute(queryi);
            statement.execute(queryj);
            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("StoreProjectHistories() - " + s.toString());
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
    
    public void ExportUserTable(String exportFile) throws SQLException {
        
        String query = "select b_cpid.user_cpid, b_cpid.total_credit, b_cpid.rac, b_cpid.rac_time, b_cpid.computer_count, b_users.name,b_users.user_id, "+
            "b_country.country,b_users.create_time,b_users.url,b_teams.name as team_name, b_users.total_credit as ptc, b_users.rac as prac, "+
            "b_users.rac_time as prt, b_projects.name as pname, b_projects.url as purl, b_users.computer_count as pcc, b_rank_user_tc_p0c0.rank as wr "+
            "from b_cpid join (b_users, b_projects, b_rank_user_tc_p0c0) "+
            "on (b_cpid.table_id=b_users.b_cpid_id and b_users.project_id=b_projects.project_id and b_rank_user_tc_p0c0.b_cpid_id=b_cpid.table_id) "+
            "left join (b_country) on (b_users.country_id=b_country.country_id) "+
            "left join (b_teams) on (b_users.b_team_id=b_teams.table_id) "+
            "where b_cpid.project_count > 0 and b_cpid.total_credit > 0 "+
            "order by b_cpid.rac desc,b_cpid.table_id";

        
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
                               "         <name>"+pname+"</name>\n";
                    if (purl != null && purl.length() > 0) 
                        temp +="         <url>"+purl+"</url>\n";
                    
                    temp +=    "         <id>"+user_id+"</id>\n"+
                               "         <user_name>"+user_name+"</user_name>\n"+
                               "         <create_time>"+create_time+"</create_time>\n"+
                               "         <total_credit>"+ptc+"</total_credit>\n"+
                               "         <expavg_credit>"+prac+"</expavg_credit>\n"+
                               "         <expavg_time>"+prt+"</expavg_time>\n";
                    if (country != null && country.length() > 0) {
                        temp +="         <country>"+"</country>\n";
                    }
                    if (url != null && url.length() > 0)
                        temp +="         <user_url>http://"+url+"</user_url>\n";
                    if (team_name != null && team_name.length() > 0) {
                        temp +="         <team_id>"+"</team_id>\n"+
                               "         <team_name>"+"</team_name>\n";
                               
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
    
    public void ExportHostTable(String exportFile) throws SQLException {
        
        String query = "select b_cpid.user_cpid, b_cpid.total_credit, b_cpid.rac, b_cpid.rac_time, b_cpid.computer_count, b_users.name,b_users.user_id, "+
            "b_country.country,b_users.create_time,b_users.url,b_teams.name as team_name, b_users.total_credit as ptc, b_users.rac as prac, "+
            "b_users.rac_time as prt, b_projects.name as pname, b_projects.url as purl, b_users.computer_count as pcc, b_rank_world_tc.rank as wr "+
            "from b_cpid join (b_users, b_projects, b_rank_world_tc) "+
            "on (b_cpid.table_id=b_users.b_cpid_id and b_users.project_id=b_projects.project_id and b_rank_world_tc.b_cpid_id=b_cpid.table_id) "+
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
                               "         <name>"+pname+"</name>\n";
                    if (purl != null && purl.length() > 0) 
                        temp +="         <url>"+purl+"</url>\n";
                    
                    temp +=    "         <id>"+user_id+"</id>\n"+
                               "         <user_name>"+user_name+"</user_name>\n"+
                               "         <create_time>"+create_time+"</create_time>\n"+
                               "         <total_credit>"+ptc+"</total_credit>\n"+
                               "         <expavg_credit>"+prac+"</expavg_credit>\n"+
                               "         <expavg_time>"+prt+"</expavg_time>\n";
                    if (country != null && country.length() > 0) {
                        temp +="         <country>"+"</country>\n";
                    }
                    if (url != null && url.length() > 0)
                        temp +="         <user_url>http://"+url+"</user_url>\n";
                    if (team_name != null && team_name.length() > 0) {
                        temp +="         <team_id>"+"</team_id>\n"+
                               "         <team_name>"+"</team_name>\n";
                               
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
            "b_users.rac_time as prt, b_projects.name as pname, b_projects.url as purl, b_users.computer_count as pcc, b_rank_world_tc.rank as wr "+
            "from b_cpid join (b_users, b_projects, b_rank_world_tc) "+
            "on (b_cpid.table_id=b_users.b_cpid_id and b_users.project_id=b_projects.project_id and b_rank_world_tc.b_cpid_id=b_cpid.table_id) "+
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
                               "         <name>"+pname+"</name>\n";
                    if (purl != null && purl.length() > 0) 
                        temp +="         <url>"+purl+"</url>\n";
                    
                    temp +=    "         <id>"+user_id+"</id>\n"+
                               "         <user_name>"+user_name+"</user_name>\n"+
                               "         <create_time>"+create_time+"</create_time>\n"+
                               "         <total_credit>"+ptc+"</total_credit>\n"+
                               "         <expavg_credit>"+prac+"</expavg_credit>\n"+
                               "         <expavg_time>"+prt+"</expavg_time>\n";
                    if (country != null && country.length() > 0) {
                        temp +="         <country>"+"</country>\n";
                    }
                    if (url != null && url.length() > 0)
                        temp +="         <user_url>http://"+url+"</user_url>\n";
                    if (team_name != null && team_name.length() > 0) {
                        temp +="         <team_id>"+"</team_id>\n"+
                               "         <team_name>"+"</team_name>\n";
                               
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
            statement.execute(query);
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
    
    public void OldDataUserTC(Vector<RacFix> v, int project_id, String dateFetch) throws SQLException {
        
        v.clear();
        try { Thread.sleep(1000); }
        catch (Exception e) {}
        String query = "select user_id,total_credit from users_pit_hist where project_id="+project_id+" and timestamp='"+dateFetch+"'";
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
                RacFix u = new RacFix();
                u.table_id =  resultSet.getInt("user_id");
                u.total_credit = resultSet.getInt("total_credit");
                v.add(u);                
            } 
            resultSet.close();
            statement.close();
            return;
        } catch (SQLException s) {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            throw new SQLException("TeamReadFix() - " + s.toString());
        }        
    }
}
