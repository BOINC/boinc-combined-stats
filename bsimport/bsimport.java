
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
// BOINC Combined Statistics Import/Export System
//
// This behemoth will import our data files into
// the mysql tables, run the calculations, and 
// export the mess for others to import
//
// TODO:
//   -add command line processor
//      -run specific project import
//      -run host/user/team import only
//      -don't incriment counter/use current
//      -run big updates/rankings
//   -if team file import fails, we update the team rac and store history, but
//    not the user members history. Need to do that.
//   -if team file import fails, need to update team user count history? (or is this in the big ugly sql query set) 
//   -export hosts
//   -export teams
//   -optimize host read by not having it read into memory those with a rac of 0 and last_rpc of > 4 months ago
//   -delete unused entries (project_count=0) from b_cpid and history tables???
//   -delete unused entries (project_count=0) from b_cteams and history tables???
//   -do we even need a b_cpid_??? hist table? We compute the sums off the b_users table anyway
//      If we stored the ranking history, then would proably need to keep that.
//   -store a week history for hosts???
//   -grid republic import
//   -grid republic calculations
//   -grid republic export
//   -for retired projects, process cpid changes
//   -do rankings for/by country?
//   -do rankings for/by os_name?
//   -do rankings for/by cpu type??
//   -team user rankings?
//   -store tc ranking history?
//       -world on b_cpid
//       -world on b_cteams
//       -project rank on b_users
//       -project rank on b_teams


import java.io.*;
import java.util.*;
import java.util.zip.*;
import org.xml.sax.*;
import org.xml.sax.helpers.*;
//import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class bsimport {

    Database myDB;
    Hashtable<String, Integer> hCountries = null;
    Hashtable<String, User> hUsers = null;
    Hashtable<String, Team> hTeams = null;
    Hashtable<String, Host> hHosts=null;
    Hashtable<String, Integer> hPvendor=null;
    Hashtable<String, Integer> hPmodel=null;
    Hashtable<String, Integer> hOsname=null;   
    Hashtable<String,CPID> hCpid=null;
    Hashtable<String, CPIDChange> hCpidChanges=null;
    Hashtable<String, CTeam> hCTeam=null;
    long racTime = 0;
    int tc_day=0;
    int rac_day=0;
    int week_day=0;
    int year_day=0;
    String PLATFORM_DIR_CHAR=File.separator;
    
    static Logger log = Logger.getLogger(bsimport.class.getName());  // log4j stuff
    
    public bsimport()
    {
        
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        
        //      
        // Configure the log4j system
        //
        PropertyConfigurator.configure("log4j.props");

        log.info("Boinc Stats Import Starting up");
        
        bsimport myRun = new bsimport();
        myRun.RunImport();
    }

    
    public void RunImport() {
        
        log.info("Begin stats update "+System.currentTimeMillis());
        myDB = new Database();
        
        ConfigReader cr = new ConfigReader();
        try {
            cr.ReadConfigFile("bsimport.ini");
        } 
        catch (java.io.IOException ioerror) {
            log.error("Failed to read bsimport.ini file");
            return;
        }
        
        String database_driver = cr.GetValueString("database", "driver");
        String database_name = cr.GetValueString("database","dbname");
        String database_user = cr.GetValueString("database", "dbuser");
        String database_pass = cr.GetValueString("database","dbpass");
        
        myDB.SetDatabaseInfo(database_driver,database_name, database_user,database_pass);
        
        try {
            
            // Init hashtables
            hCountries = new Hashtable<String, Integer>();
            hUsers = new Hashtable<String, User>();
            hHosts = new Hashtable<String, Host>();
            hPvendor = new Hashtable<String, Integer>();
            hPmodel = new Hashtable<String, Integer>();
            hOsname = new Hashtable<String, Integer>();
            hTeams = new Hashtable<String, Team>();
            hCpid = new Hashtable<String,CPID>();
            hCpidChanges = new Hashtable<String,CPIDChange>();
            hCTeam = new Hashtable<String, CTeam>();

            // login to dB
            myDB.LoginToDatabase();

            // TODO:
            if (false) {
                //for (int i=37;i>0;i--)
                //    if (i != 19)
                //        ConvertOldUserStatsData(i);
                
                //ConvertOldProjectStatsData("b_project_users_hist","user_count");
                //ConvertOldProjectStatsData("b_project_hosts_hist","host_count");
                //ConvertOldProjectStatsData("b_project_teams_hist","team_count");
                //ConvertOldProjectStatsData("b_project_country_hist","country_count");
                //ConvertOldProjectStatsData("b_project_total_credit_hist","total_credit");
                //ConvertOldProjectStatsData("b_project_active_users_hist","active_users");
                //ConvertOldProjectStatsData("b_project_active_hosts_hist","active_hosts");
                //ConvertOldProjectStatsData("b_project_active_teams_hist","active_teams");
                if (1==1)
                    return;
            }
            
            //myDB.CalculateCPCSTable();
            //if (racTime==racTime)
            //    return;
            
            // compute the time for our rac decay
            racTime = System.currentTimeMillis() / 1000;
            
            // Read in the country id's
            myDB.ReadCountryTable(hCountries);
            log.info("Found " + hCountries.size() + " country entries");

            // Read in the p_vendor table
            myDB.ReadPVendorTable(hPvendor);
            log.info("Found " + hPvendor.size() + " p_vendor entries");
            
            // Read in the p_model table
            myDB.ReadPModelTable(hPmodel);
            log.info("Found " + hPmodel.size() + " p_model entries");
            
            // Read in the os_name table
            myDB.ReadOSNameTable(hOsname);
            log.info("Found " + hOsname.size() + " os_name entries");
            
            // Read in the cpid table
            myDB.CPIDRead(hCpid);
            log.info("Found " + hCpid.size() + " cpid entries");
            
            // Read in the cteam table
            myDB.CTeamRead(hCTeam);
            log.info("Found "+hCTeam.size() + " Combined team entries");
            
            // Get the history day update value
            tc_day = myDB.GetCurrentDay("tc");
            if (tc_day == 0) {
                // error - as in big error!
                log.error("Failed to get our total_credit day counter from the DB");
                return;
            }
            if (cr.GetValueBoolean("import","advance_counters",true)) {
                tc_day++;
                if (tc_day > 91)
                    tc_day = 1;
            }
            
            rac_day = myDB.GetCurrentDay("rac");
            if (rac_day == 0) {
                // error - as in big error!
                log.error("Failed to get our rac day counter from the DB");
                return;
            }
            if (cr.GetValueBoolean("import","advance_counters",true)) {
                rac_day++;
                if (rac_day > 31)
                    rac_day = 1;
            }
            
            year_day = myDB.GetCurrentDay("year");
            if (year_day == 0) {
                log.error("Failed to get our project day counter from the DB");
                return;
            }
            
            if (cr.GetValueBoolean("import","advance_counters",true)) {
                year_day++;
                if (year_day > 365)
                    year_day=1;
            }
            
            Calendar cal = Calendar.getInstance(TimeZone.getDefault());
            String df = "w";
            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat(df);
            sdf.setTimeZone(TimeZone.getDefault());
            week_day = Integer.parseInt(sdf.format(cal.getTime()));
            
            // For each active project in the list:
            // Read in the current user list (table_id/userid/cpid)
            // Read in the current host list (table_id/hostid)
            // Read the current user.xml file:
            
            Vector<Project> myProjects = new Vector<Project>();
            int updateCount=0;
            int updateTeamCount=0;
            int updateHostCount=0;
            
            if (cr.GetValueBoolean("import", "read_files", true)) {
                try {
                	System.gc();
                    myDB.ProjectsRead(myProjects);
                    
                    for(int i=0;i<myProjects.size();i++) {
                        
                        try {
                            Project p = (Project)myProjects.elementAt(i);
                           
                            log.info("Starting project "+p.name+" data import: "+System.currentTimeMillis());
                            if (p.team_file != null) {
                                // cache the user ids
                                myDB.UserRead(hUsers, p.project_id);
                                log.info("Read in "+hUsers.size()+" users from project " + p.name + "(project_id="+ p.project_id+")");
                                
                                // Read in the team list from the db
                                myDB.TeamRead(hTeams, p.project_id);
                                log.info("Read in "+hTeams.size()+" teams from project " + p.name + "(project_id="+ p.project_id+")");
                                
                                // Read the current team.xml file:
                                // --update existing data
                                // --add new entries
                                String filename = p.data_dir+p.shortname+PLATFORM_DIR_CHAR+p.team_file;
                                updateTeamCount = ReadProjectTeamXMLFile(filename,p.project_id);
                                if (updateTeamCount == 0)
                                    log.warn("Failed to import data from file "+filename);
                                
                                // --remove entries that don't exist
                                if (updateTeamCount > 0) {
                                    int deleted = 0;
                                    Iterator<String> it = hTeams.keySet().iterator();
                                    while (it.hasNext()) {
                                        String t = (String)it.next();
                                        Team tt = (Team)hTeams.get(t);
                                        myDB.TeamDeleteEntry(tt.table_id);
                                        deleted++;
                                    }
                                    log.info("Deleted "+deleted+" teams from project "+p.name+" (project_id="+p.project_id+")");
                                }
                            
                                if (p.user_file != null) {    
                                    //Read in the team list from the db
                                    myDB.TeamRead(hTeams, p.project_id);
                                    log.info("Reread in "+hTeams.size()+" teams from project " + p.name + "(project_id="+ p.project_id+")");
                                    
                                    // --Update existing data
                                    // --Add new entries
                                    filename = p.data_dir+p.shortname+PLATFORM_DIR_CHAR+p.user_file;
                                    updateCount = ReadProjectUserXMLFile(filename, p.project_id);
                                
                                    if (updateCount == 0)
                                        log.warn("Failed to import data from file "+filename);
                                    else {
                                        // set our update time
                                        File f = new File (filename);
                                        long timestamp = f.lastModified();
                                        Date when = new Date(timestamp);
                                        sdf = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                                        sdf.setTimeZone(TimeZone.getDefault());
                                        String lastImport = sdf.format(when);
                                        myDB.ProjectUpdateTime(p.project_id, lastImport);
                                         
                                    }
                                }
                                // Read the current host.xml file:
                                if (p.host_file != null) {
                                    myDB.HostRead(hHosts, p.project_id);
                                    log.info("Read in "+hHosts.size()+" hosts from project "+p.name+" (project_id="+p.project_id+")");
                                    
                                    // --update existing data
                                    // --add new entries         
                                    filename = p.data_dir+p.shortname+PLATFORM_DIR_CHAR+p.host_file;
                                    updateHostCount = ReadProjectHostXMLFile(filename,p.project_id);
                                    if (updateHostCount == 0)
                                        log.warn("Failed to import data from file "+filename);
                                    
                                    // --remove entries that don't exist any more
                                    //   entries were removed from the hashtable as
                                    // they were processed
                                    if (updateHostCount > 0) {
                                        int deleted=0;
                                        Iterator<String> it = hHosts.keySet().iterator();
                                        while (it.hasNext()) {
                                            String hk = (String) it.next();
                                            Host hh = (Host)hHosts.get(hk);
                                            myDB.HostDeleteEntry(hh.table_id);
                                            deleted++;
                                        }
                                        log.info("Deleted "+deleted+" hosts from project "+p.name+" (project_id="+p.project_id+")");
                                    } 
                                  
                                    
                                    if (updateCount == 0){
                                        // we had isues with the update
                                        // grab the RAC from the DB for anyone where it is > 0
                                        // and recalculate before we do the rankings
                                        // Also need to poke the values into the history tables
                                        // otherwise they won't get done
                                        UpdateProjectUserRAC(p.project_id);
                                        
                                    } 
                                    if (updateTeamCount == 0) {
                                        // we had isues with the update
                                        // grab the RAC from the DB for anyone where it is > 0
                                        // and recalculate before we do the rankings
                                        // Also need to poke the values into the history tables
                                        // otherwise they won't get done
                                        UpdateProjectTeamRAC(p.project_id);
                                        
                                    }
                                    if (updateHostCount == 0) {
                                        // we had isues with the update
                                        // grab the RAC from the DB for anyone where it is > 0
                                        // and recalculate before we do the rankings
                                        // Also need to poke the values into the history tables
                                        // otherwise they won't get done
                                        UpdateProjectHostRAC(p.project_id);
                                        
                                    }
                                    
                                    // create a backup of the files for each one that was good
                                    String backup_file;
                                    if (updateCount > 0) {
                                        filename = p.data_dir+p.shortname+PLATFORM_DIR_CHAR+p.user_file;
                                        backup_file = p.data_dir+p.shortname+PLATFORM_DIR_CHAR+p.shortname+"_"+p.user_file;
                                        log.info("Creating backup file "+backup_file);
                                        BackupFile(filename,backup_file);
                                    }
                                    if (updateHostCount > 0) {
                                        filename = p.data_dir+p.shortname+PLATFORM_DIR_CHAR+p.host_file;
                                        backup_file = p.data_dir+p.shortname+PLATFORM_DIR_CHAR+p.shortname+"_"+p.host_file;
                                        log.info("Creating backup file "+backup_file);
                                        BackupFile(filename,backup_file);
                                    }
                                    if (updateTeamCount > 0) {
                                        filename = p.data_dir+p.shortname+PLATFORM_DIR_CHAR+p.team_file;
                                        backup_file = p.data_dir+p.shortname+PLATFORM_DIR_CHAR+p.shortname+"_"+p.team_file;
                                        log.info("Creating backup file "+backup_file);
                                        BackupFile(filename,backup_file);
                                    }
                                    
    
                                    // the team nusers is no longer exported by
                                    // the project data export, need to calculate 
                                    // the user count ourself.
                                    log.info("Starting Team user count update");
                                    myDB.DoUpdateTeamUserCounts(p.project_id);
                                    
                                    // Calculate the project rankings
                                    // This will update the by rac and by total_credit rank
                                    log.info("Starting project rank calculations");
                                    myDB.DoProjectRankings(p.project_id);
                                    
                                    // now store the rankings in the history
                                    // TODO: if we are going to store rankings that is
                                    
                                }
                            }
                        }
                        catch (Exception e) {
                            log.error(e);
                        }
                    }
                }
                catch (java.sql.SQLException se) {
                    log.error(se);
                }
            
                // send our hash tables off to the gc
                hCountries = null;
                hUsers = null;
                hHosts = null;
                hPvendor = null;
                hPmodel = null;
                hOsname = null;
                hTeams = null;
                hCpid = null;
                hCpidChanges = null;
                hCTeam = null;
                System.gc();
                
                log.debug("End data file import: "+System.currentTimeMillis());
            
            } // end if read_files=true
            
            // Now run the big stats updates to do
            try {
                if (cr.GetValueBoolean("import", "compute_stats", true)) {
                    log.info("Updating project user computer counts");
                    myDB.DoUpdateComputerCounts();
                    log.info("Updating project stats");
                    myDB.DoProjectStats();
                    log.info("Running Combined User Stats");
                    myDB.DoCPIDDataUpdates(tc_day, rac_day, week_day);
                    log.info("Running Combined Team Stats");
                    myDB.DoTeamStats(year_day);
                    log.info("updating project 19 (Boinc Combined)");
                    myDB.DoCombinedStatsProjectUpdates(tc_day, rac_day, week_day);
                    log.info("updating project 39 (Grid Republic)");
                    myDB.DoGRCombinedStatsProjectUpdates(tc_day, rac_day, week_day);
                    log.info("Storing project histories");
                    myDB.StoreProjectHistories(year_day);
    
                    // do credit_per_cpu_sec analysis
                    myDB.CalculateCPCSTable();
                } // end if compute_stats is true
                
                // set our update time to our racTime               
                long timestamp = racTime * 1000; 
                Date when = new Date(timestamp);
                sdf = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                sdf.setTimeZone(TimeZone.getDefault());
                String lastImport = sdf.format(when);
                myDB.ProjectUpdateTime(19, lastImport);

                // now set the current day since we are done with the import
                if (cr.GetValueBoolean("import","advance_counters",true)) {
                    myDB.SetCurrentDay("tc",tc_day);
                    myDB.SetCurrentDay("rac", rac_day);
                    myDB.SetCurrentDay("week", week_day);
                    myDB.SetCurrentDay("year",year_day);
                }
                
                String export_path = cr.GetValueString("export", "basepath");
                export_path += File.separatorChar;
                
                // Export data
                String export_file;
                
                // user file
                export_file = export_path + "user.xml.gz";
                if (cr.GetValueBoolean("export", "export_users", false)) {
                    log.info("Exporting "+export_file+ " " + System.currentTimeMillis());
                    myDB.ExportUserTable(export_file);
                }
                
                // team file
                export_file = export_path + "team.xml.gz";
                if (cr.GetValueBoolean("export", "export_teams", false)) {
                    log.info("Exporting "+export_file+ " " + System.currentTimeMillis());
                    myDB.ExportTeamTable(export_file);
                }
            
                // host file
                export_file = export_path + "host.xml.gz";
                if (cr.GetValueBoolean("export", "export_hosts", false)) {
                    log.info("Exporting "+export_file+ " " + System.currentTimeMillis());
                    myDB.ExportHostTable(export_file);
                }
            }
            catch (Exception e) {
                log.error(e);
            }
            

            
            log.info("End stats update "+System.currentTimeMillis());
            
        }
        catch (java.sql.SQLException se) {
            log.error(se);
        }
    }
    

    public int ReadProjectUserXMLFile(String filename, int project_id) {

        int added=0,updated=0;
        
        try {
            // open compressed file
            //if (filename.substring(filename.length()-3,filename.length().equals(".gz"))) {
            GZIPInputStream in = new GZIPInputStream(new FileInputStream(filename));
 
                        
            XMLReader parser = XMLReaderFactory.createXMLReader("org.apache.xerces.parsers.SAXParser");
            org.xml.sax.ContentHandler handler = new UserXMLFileHandler(hUsers,hCountries,hCpid,hCpidChanges,hTeams,myDB,project_id,racTime,tc_day,rac_day); 
            
            parser.setContentHandler(handler);
            InputSource source = new InputSource(in);
            parser.parse(source);
            
            added = ((UserXMLFileHandler)handler).added;
            updated = ((UserXMLFileHandler)handler).updated;
            log.info("Updated "+updated+" Users and added "+added+" for project "+project_id);

            in.close();
                        
        }
        catch (java.io.FileNotFoundException e) {
            log.error(e);
        }
        catch (java.io.IOException ioe) {
            log.error(ioe);
        }
        catch (SAXException se) {
            log.error(se);
        } 
        
        return (added+updated);
    }

    public int ReadProjectHostXMLFile(String filename, int project_id) {

        int added=0,updated=0;
        
        try {
            // open compressed file
            //if (filename.substring(filename.length()-3,filename.length().equals(".gz"))) {
            GZIPInputStream in = new GZIPInputStream(new FileInputStream(filename));
            
            
            XMLReader parser = XMLReaderFactory.createXMLReader("org.apache.xerces.parsers.SAXParser");
            org.xml.sax.ContentHandler handler = new HostXMLFileHandler(hUsers,hHosts, hPvendor, hPmodel, hOsname, hCpid,hCpidChanges, myDB, project_id, racTime); 
            
            parser.setContentHandler(handler);
            InputSource source = new InputSource(in);
            parser.parse(source);
            
            added = ((HostXMLFileHandler)handler).added;
            updated = ((HostXMLFileHandler)handler).updated;
            int anonymous = ((HostXMLFileHandler)handler).anonymous;
            int skipped = ((HostXMLFileHandler)handler).update_skipped;
            log.info("Updated "+updated+" hosts ("+skipped+" skipped) and added "+added+" for project "+project_id + "("+ anonymous+" anonymous)");
            in.close();
                        
        }
        catch (java.io.FileNotFoundException e) {
            log.error(e);
        }
        catch (java.io.IOException ioe) {
            log.error(ioe);
        }
        catch (SAXException se) {
            log.error(se);
        }
        return (added+updated);
    }    
    
    public int ReadProjectTeamXMLFile(String filename, int project_id) {

        int added=0,updated=0;
        
        try {
            // open compressed file
            //if (filename.substring(filename.length()-3,filename.length().equals(".gz"))) {
            GZIPInputStream in = new GZIPInputStream(new FileInputStream(filename));
            
            
            XMLReader parser = XMLReaderFactory.createXMLReader("org.apache.xerces.parsers.SAXParser");
            org.xml.sax.ContentHandler handler = new TeamXMLFileHandler(hUsers,hTeams,hCountries, hCTeam, myDB, project_id, racTime, tc_day,rac_day); 
            
            parser.setContentHandler(handler);
            InputSource source = new InputSource(in);
            parser.parse(source);
            
            added = ((TeamXMLFileHandler)handler).added;
            updated = ((TeamXMLFileHandler)handler).updated;
            
            log.info("Updated "+updated+" teams and added "+added+" for project "+project_id);
            in.close();
                        
        }
        catch (java.io.FileNotFoundException e) {
            log.error(e);
        }
        catch (java.io.IOException ioe) {
            log.error(ioe);
        }
        catch (SAXException se) {
            log.error(se);
        }
        return (added+updated);
    }     
    
    /** A convenience method to throw an exception */
    private static void abort(String msg) throws IOException { 
      throw new IOException(msg); 
    }
    
    public void BackupFile(String from_name, String to_name)
    {
        try {

            File from_file = new File(from_name);  // Get File objects from Strings
            File to_file = new File(to_name);
            
            // First make sure the source file exists, is a file, and is readable.
            if (!from_file.exists())
              abort("FileCopy: no such source file: " + from_name);
            if (!from_file.isFile())
              abort("FileCopy: can't copy directory: " + from_name);
            if (!from_file.canRead())
              abort("FileCopy: source file is unreadable: " + from_name);
            
            // If the destination is a directory, use the source file name
            // as the destination file name
            if (to_file.isDirectory())
              to_file = new File(to_file, from_file.getName());
            
            // If the destination exists, make sure it is a writeable file
            // and ask before overwriting it.  If the destination doesn't
            // exist, make sure the directory exists and is writeable.
            if (to_file.exists()) {
                log.debug("Overwiring existing file");
            }
            else {  
              // if file doesn't exist, check if directory exists and is writeable.
              // If getParent() returns null, then the directory is the current dir.
              // so look up the user.dir system property to find out what that is.
              String parent = to_file.getParent();  // Get the destination directory
              if (parent == null) parent = System.getProperty("user.dir"); // or CWD
              File dir = new File(parent);          // Convert it to a file.
              if (!dir.exists())
                abort("FileCopy: destination directory doesn't exist: " + parent);
              if (dir.isFile())
                abort("FileCopy: destination is not a directory: " + parent);
              if (!dir.canWrite())
                abort("FileCopy: destination directory is unwriteable: " + parent);
            }
            
            // If we've gotten this far, then everything is okay.
            // So we copy the file, a buffer of bytes at a time.
            FileInputStream from = null;  // Stream to read from source
            FileOutputStream to = null;   // Stream to write to destination
            try {
              from = new FileInputStream(from_file);  // Create input stream
              to = new FileOutputStream(to_file);     // Create output stream
              byte[] buffer = new byte[4096];         // A buffer to hold file contents
              int bytes_read;                         // How many bytes in buffer
              // Read a chunk of bytes into the buffer, then write them out, 
              // looping until we reach the end of the file (when read() returns -1).
              // Note the combination of assignment and comparison in this while
              // loop.  This is a common I/O programming idiom.
              while((bytes_read = from.read(buffer)) != -1) // Read bytes until EOF
                to.write(buffer, 0, bytes_read);            //   write bytes 
            }
            // Always close the streams, even if exceptions were thrown
            finally {
              if (from != null) try { from.close(); } catch (IOException e) { ; }
              if (to != null) try { to.close(); } catch (IOException e) { ; }
            }
            
        }
        catch (Exception e)
        {
            log.error(e);
        }
    }
    
    private String DecayRac(String current_rac, String rac_time) {
        double M_LN2 = 0.693147180559945309417;
        double credit_half_life = 86400 * 7;
        
        Double d = new Double(rac_time);      
        double avg_time = d.doubleValue();
        d = new Double(current_rac);
        double avg = d.doubleValue();
        
        if (racTime == 0) {
            // get current time
            racTime = System.currentTimeMillis() / 1000;
        }

        double diff = racTime - avg_time;
        double weight = java.lang.Math.exp(-diff * M_LN2/credit_half_life);
        double avgr = avg * weight;
        
        if (avgr < 0.009) avgr = 0.0;
        d = new Double(avgr);
        return d.toString();      
    }
    
    void UpdateProjectUserRAC(int project_id) {
        
        log.info("Updating Rac & Storing history for users in project "+project_id);
        int rc=0;
        
        Vector<RacFix> users = new Vector<RacFix>();
        try {
            // read in all user entries
            myDB.UserReadFix(users, project_id);

            for (int i=0;i<users.size();i++) {
                RacFix u = (RacFix)users.elementAt(i);
            
                String rac = ""+u.rac;
                // for each entry, if rac > 0, recalc & store new rac
                if (u.rac > 0) {
                    rac = DecayRac(rac,""+u.rac_time);
                    try {
                        myDB.UserUpdateRAC(u.table_id, rac, ""+racTime);
                        rc++;
                    }
                    catch (java.sql.SQLException ss) {
                        log.error(ss);
                    }
                }
                // store history table data
                try {
                    myDB.UserUpdateRACHistory(u.table_id, rac_day, ""+u.rac);
                    myDB.UserUpdateTotalCreditHistory(u.table_id, tc_day, week_day, ""+u.total_credit);
                }
                catch (java.sql.SQLException ss) {
                    log.error(ss);
                }
            }
            log.info("Stored history data for "+users.size()+" entries and updated "+rc+" RAC entries");
        }
        catch (java.sql.SQLException se) {
            log.error(se);
        }
    }
    

    void UpdateProjectTeamRAC(int project_id) {
        
        log.info("Updating Rac & Storing history for teams in project "+project_id);
        int rc=0;
        
        Vector<RacFix> teams = new Vector<RacFix>();
        try {
            // read in all user entries
            myDB.TeamReadFix(teams, project_id);

            for (int i=0;i<teams.size();i++) {
                RacFix u = (RacFix)teams.elementAt(i);
            
                String rac = ""+u.rac;
                // for each entry, if rac > 0, recalc & store new rac
                if (u.rac > 0) {
                    rac = DecayRac(rac,""+u.rac_time);
                    try {
                        myDB.TeamUpdateRAC(u.table_id, rac, ""+racTime);
                        rc++;
                    }
                    catch (java.sql.SQLException ss) {
                        log.error(ss);
                    }
                }
                // store history table data
                try {
                    myDB.TeamUpdateRACHistory(u.table_id, rac_day, ""+u.rac);
                    myDB.TeamUpdateTotalCreditHistory(u.table_id, tc_day, week_day, ""+u.total_credit);
                }
                catch (java.sql.SQLException ss) {
                    log.error(ss);
                }
            }
            log.info("Stored history data for "+teams.size()+" entries and updated "+rc+" RAC entries");
        }
        catch (java.sql.SQLException se) {
            log.error(se);
        }
    }
    
    void UpdateProjectHostRAC(int project_id) {
        
        log.info("Updating Rac & Storing history for hosts in project "+project_id);
        int rc=0;
        
        Vector<RacFix> hosts = new Vector<RacFix>();
        try {
            // read in all user entries
            myDB.HostReadFix(hosts, project_id);

            for (int i=0;i<hosts.size();i++) {
                RacFix u = (RacFix)hosts.elementAt(i);
            
                String rac = ""+u.rac;
                // for each entry, if rac > 0, recalc & store new rac
                if (u.rac > 0) {
                    rac = DecayRac(rac,""+u.rac_time);
                    try {
                        myDB.HostUpdateRAC(u.table_id, rac, ""+racTime);
                        rc++;
                    }
                    catch (java.sql.SQLException ss) {
                        log.error(ss);
                    }
                }
                // store history table data
                //try {
                    // TODO: If you uncomment, need to change read query to include all hosts, not just those with rac > 0
                    //myDB.HostUpdateRACHistory(u.table_id, rac_day, ""+u.rac);
                    //myDB.HostUpdateTotalCreditHistory(u.table_id, tc_day, week_day, ""+u.total_credit);
                //}
                //catch (java.sql.SQLException ss) {
                //    log.error(ss);
                //}
            }
            log.info("Updated "+rc+" RAC entries");
        }
        catch (java.sql.SQLException se) {
            log.error(se);
        }
    }
    
    void ConvertOldUserStatsData(int project_id) {

        Vector<RacFix> v = new Vector<RacFix>();
        long count=13;
        int pweek=41;
        
        // read in to the hash table the user list
        try {
            myDB.UserRead(hUsers, project_id);
            
            // loop from Oct 12 (dp=91) to dp=30 (all we have)
            int day=1;
            
            while (day == 1 || day > 42) {
                long daysago = 1000*3600*24*count;
                long timestamp = (System.currentTimeMillis()) - daysago;
                Date when = new Date(timestamp);
                java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
                sdf.setTimeZone(TimeZone.getDefault());
                String lastImport = sdf.format(when);
                                
                sdf = new java.text.SimpleDateFormat("w");
                int week= Integer.parseInt(sdf.format(when));
                
                if (week < pweek) {
                    pweek = week;
                } else {
                    week = 0;
                }
                
                myDB.OldDataUserTC(v, project_id, lastImport);
                for (int i=0;i<v.size();i++) {
                    RacFix r = (RacFix) v.elementAt(i);
                    if (hUsers.containsKey(new Integer(r.table_id).toString())) {
                        User u = (User)hUsers.get(new Integer(r.table_id).toString());
                        myDB.UserUpdateTotalCreditHistory(u.table_id, day, week, new Integer(r.total_credit).toString());
                    }
                }
                log.info("P:"+project_id+" D:"+lastImport+" d_"+day+" C:"+v.size());
                count++;
                day--;
                if (day == 0) day = 91;
                v.clear();
            }
        }
        catch (java.sql.SQLException se) {
            log.error(se);
        }
    }
    
    void ConvertOldProjectStatsData(String stats_table, String column) {
             
        int start=1;
        int end=7;
        long count=5;
        
        while (start != end) {
            long daysago = 1000*3600*24*count;
            long timestamp = (System.currentTimeMillis()) - daysago;
            Date when = new Date(timestamp);
            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
            sdf.setTimeZone(TimeZone.getDefault());
            String lastImport = sdf.format(when);
    
            log.debug(lastImport+" ["+when+ "] "+timestamp);
            
            // build query
            String q = "update "+stats_table+", (select project_id,"+column+" from projects_stats where timestamp='"+lastImport+"') cc "+
                       "set "+stats_table+".d_"+start+"=cc."+column+" where "+stats_table+".project_id=cc.project_id";
            
            log.debug(q);
            try {
                int cnt = myDB.RunQuery(q);
                log.info("Updated "+cnt+" rows");
            }
            catch (java.sql.SQLException e) {
                log.error(e);
            }
            
            start = start -1;
            if (start < 1) start = 365;
            count++;
        }
        
    }
    
    void ConvertOldUserData() {
        
        int start=1;
        int end=32;
        long count=3;
        
        while (start != end) {
            long daysago = 1000*3600*24*count;
            long timestamp = (System.currentTimeMillis()) - daysago;
            Date when = new Date(timestamp);
            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
            sdf.setTimeZone(TimeZone.getDefault());
            String lastImport = sdf.format(when);
    
            log.debug(lastImport+" ["+when+ "] "+timestamp);
            
            // build query
            String q = "update b_users_total_credit_hist, (select a.table_id,b.total_credit from b_users a, users_pit_hist b " +
                       "where b.timestamp='"+lastImport+"' and a.project_id=b.project_id and a.user_id=b.user_id) cc "+
                       "set d_"+start+"=cc.total_credit where b_users_total_credit_hist.b_users_id=cc.table_id";
            
            log.debug(q);
            try {
                int cnt = myDB.RunQuery(q);
                log.info("Updated "+cnt+" rows");
            }
            catch (java.sql.SQLException e) {
                log.error(e);
            }
            
            start = start -1;
            if (start < 1) start = 91;
            count++;
        }
        
    }
    
}
