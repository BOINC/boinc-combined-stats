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
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.*;
import java.util.zip.*;
import org.xml.sax.*;
import org.xml.sax.helpers.*;
//import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class ProjectImport {

    Database myDB;
    Hashtable<String, Country> hCountries = null;
    Hashtable<String, User> hUsers = null;
    Hashtable<String, Team> hTeams = null;
    Hashtable<String, Host> hHosts=null;
    Hashtable<String, Integer> hPvendor=null;
    Hashtable<String, Integer> hPmodel=null;
    Hashtable<String, Integer> hOsname=null;   
    Hashtable<String,CPID> hCpid=null;
    //Hashtable<String, CPIDChange> hCpidChanges=null;
    Hashtable<String, CTeam> hCTeam=null;
    long racTime = 0;
    int tc_day=0;
    int rac_day=0;
    int week_day=0;
    int year_day=0;
    String PLATFORM_DIR_CHAR=File.separator;
    
    static Logger log = Logger.getLogger(ProjectImport.class.getName());  // log4j stuff
    
    public ProjectImport()
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

        log.info("Boinc Stats File Importer Starting");
        
        ProjectImport myRun = new ProjectImport();
        myRun.RunImport();
    }

    
    public void RunImport() {
        
        log.info("Begin File Import");
        myDB = new Database();
        
        ConfigReader cr = new ConfigReader();
        try {
            cr.ReadConfigFile("projectimport.ini");
        } 
        catch (java.io.IOException ioerror) {
            log.error("Failed to read projectimport.ini file");
            return;
        }
        
        String database_driver = cr.GetValueString("database", "driver");
        String database_name = cr.GetValueString("database","dbname");
        String database_user = cr.GetValueString("database", "dbuser");
        String database_pass = cr.GetValueString("database","dbpass");
        boolean addMissingTypes = cr.GetValueBoolean("import", "update_types_tables",false);
        boolean backupFiles = cr.GetValueBoolean("import","backup_files_after_import",false);
        boolean forceImport = cr.GetValueBoolean("import", "read_all_if_one",false);
        String lockfile = cr.GetValueString("lockfile", "name","/home/drews/boinc/boinc.lck");
        int lockRetries = cr.GetValueInt("lockfile", "retries",5);
        int lockRetryWait = cr.GetValueInt("lockfile","retry_wait", 1);
        String dbTempFile = cr.GetValueString("database", "tempfile","/tmp/import.tmp");
        
        myDB.SetDatabaseInfo(database_driver,database_name, database_user,database_pass);
        
        File file=null;
    	FileChannel channel=null;
    	FileLock lock=null;        	
   	
        try {
            
        	// create lock file
        	boolean waitingOnLock = true;
        	int lockTries=0;
        	
        	while (waitingOnLock == true) {
	        	try {
	        		file = new File (lockfile);
	        		channel = new RandomAccessFile(file,"rw").getChannel();
	        		// Use the file channel to create a lock on the file.
	                // This method blocks until it can retrieve the lock.
	                //lock = channel.lock();
	                // Try acquiring the lock without blocking. This method returns
	                // null or throws an exception if the file is already locked.
	        		try {
	                    lock = channel.tryLock();
	                    
	                } catch (OverlappingFileLockException e) {
	                    // File is already locked in this thread or virtual machine
	                	
	                }
	                if (lock == null) {
	                	lockTries++;
	                	if(lockTries > lockRetries) {
	                		log.error("Failed to aquire file lock - giving up");
	                		return;
	                	}
	                	try {
	                		log.info("Waiting on lock file - sleeping");
	                		Thread.sleep(lockRetryWait*60*1000); // Try to sleep for 5 min
	                	}
	                	catch (Exception se) {}
	                } else {
	                	waitingOnLock = false;
	                }
	                
	        	}
	        	catch (Exception e) {
	        		log.error("Error",e);
	        		return;
	        	}
        	}        	
        	
            // Init hashtables
            hCountries = new Hashtable<String, Country>();
            hUsers = new Hashtable<String, User>();
            hHosts = new Hashtable<String, Host>();
            hPvendor = new Hashtable<String, Integer>();
            hPmodel = new Hashtable<String, Integer>();
            hOsname = new Hashtable<String, Integer>();
            hTeams = new Hashtable<String, Team>();
            hCpid = new Hashtable<String,CPID>();
            hCTeam = new Hashtable<String, CTeam>();

            // login to dB
            myDB.LoginToDatabase();

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
            
            // For each active project in the list:
            // Read in the current user list (table_id/userid/cpid)
            // Read in the current host list (table_id/hostid)
            // Read the current user file:
            
            Vector<Project> myProjects = new Vector<Project>();
            int updateUserCount=0;
            int updateTeamCount=0;
            int updateHostCount=0;
            
            
            
            if (cr.GetValueBoolean("import", "read_files", true)) {
                try {
                	System.gc();
                    myDB.ProjectsRead(myProjects);
                    
                    for(int i=0;i<myProjects.size();i++) {
                        
                    	// For each project, if the file exists, compare the time stamp
                    	// with the one in the DB. If different then import
                    	
                    	// Import process is for each file (team, user, host):
                    	// -drop backup table
                    	// -create new backup table
                    	// -import the file into the backup table
                    	// -do needed calculations
                    	// -add indexes                    	
                    	// -store new time stamp in the database
                    	// -if all goes well, drop production table, rename backup to production

                    	// TODO: Validate imported data
                    	// TODO: Sanitize/check lengths on all data
                    	// TODO: Create a bcs_id in the users table
                    	// NOTE: bcs_id could be dangerous - wcg just had a duplicate cpid show
                    	//       up in their data set alone.
                    	// TODO: Record rank history (global) based on bcs_id
                    	// TODO: Create rank based on bcs_id
                    	
                    	boolean userImported=false;
                    	boolean hostImported=false;
                    	boolean teamImported=false;
                    	boolean userImportNeeded=false;
                    	boolean hostImportNeeded=false;
                    	boolean teamImportNeeded=false;
                    	                    
                        try {
                            Project p = (Project)myProjects.elementAt(i);
                           
                            // Skip our Grid Republic and Combined projects
                        	if (p.project_id == 1 || p.project_id == 39)
                        		continue;
                        
                        	// Check time of teamfile
                        	String filename = p.data_dir+p.shortname+PLATFORM_DIR_CHAR+p.team_file;
                        	File f = new File (filename);
                            long timestamp = f.lastModified();
                            Date when = new Date(timestamp);
                            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm");
                            sdf.setTimeZone(TimeZone.getDefault());
                            String currentFileTime = sdf.format(when);
                            String lastImportTime = myDB.ProjectGetTeamUpdateTime(p.project_id);
                            if (lastImportTime == null || currentFileTime.substring(0, 16).compareTo(lastImportTime.substring(0,16)) != 0) {
                            	teamImportNeeded = true;
                            }
                            // Check time of user file
                        	filename = p.data_dir+p.shortname+PLATFORM_DIR_CHAR+p.user_file;
                        	f = new File (filename);
                            timestamp = f.lastModified();
                            when = new Date(timestamp);
                            sdf = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm");
                            sdf.setTimeZone(TimeZone.getDefault());
                            currentFileTime = sdf.format(when);
                            lastImportTime = myDB.ProjectGetUserUpdateTime(p.project_id);
                            if (lastImportTime == null || currentFileTime.substring(0, 16).compareTo(lastImportTime.substring(0,16)) != 0) {
                            	userImportNeeded = true;
                            }	
                            // Check time of host file
                        	filename = p.data_dir+p.shortname+PLATFORM_DIR_CHAR+p.host_file;
                        	f = new File (filename);
                            timestamp = f.lastModified();
                            when = new Date(timestamp);
                            sdf = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm");
                            sdf.setTimeZone(TimeZone.getDefault());
                            currentFileTime = sdf.format(when);
                            lastImportTime = myDB.ProjectGetHostUpdateTime(p.project_id);
                            if (lastImportTime == null || currentFileTime.substring(0, 16).compareTo(lastImportTime.substring(0,16)) != 0) {
                            	hostImportNeeded = true;
                            }	
                            	
                            if (forceImport == true && (hostImportNeeded == true || userImportNeeded == true || teamImportNeeded == true)) {
                            	hostImportNeeded = true;
                            	userImportNeeded = true;
                            	teamImportNeeded = true;
                            }
                            
                            //
                            // Start with the TEAM file
                            //
                            
                            // Cache the team info from the current table
                            if (teamImportNeeded == true || userImportNeeded == true || hostImportNeeded == true) {
                            	log.info("Starting project "+p.name+" data import ("+i+" of "+myProjects.size()+")");

	                            hTeams.clear();
	                            try {
	                            	myDB.TeamRead(hTeams, "teams_"+p.shortname);
	                            	log.info("Read in "+hTeams.size()+" teams from project " + p.name);
	                            } catch (java.sql.SQLException ss) {
	                            	// table may not exist if project is new
	                            	log.error("Failed to read from table teams_"+p.shortname);
	                            	try {
	                            		myDB.CreateTeamsTable(p.shortname);
	                            	} catch (java.sql.SQLException ss2) {
	                            		log.error("Failed to create table teams_"+p.shortname);
	                            	}
	                            }
                            }
                            // if the team file exists, blast it into the backup table
                            if (p.team_file != null && teamImportNeeded == true) {
                            	
                            	// drop team backup table (may not exist)
                                try {
                                	myDB.DropTable("teams_"+p.shortname+"_bk");
                                }
                                catch (java.sql.SQLException s) {
                                	// ignoring this, table probably doesn't exist (it shouldn't)
                                }
                                
                                try {
	                                // create team backup table
	                                myDB.CreateTeamsTable(p.shortname+"_bk");
	                                
	                                // Read the current team.xml file:
	                                filename = p.data_dir+p.shortname+PLATFORM_DIR_CHAR+p.team_file;
	                                updateTeamCount = ReadProjectTeamXMLFile(filename,"teams_"+p.shortname+"_bk",dbTempFile);
	                                
	                                if (updateTeamCount < 0) {
	                                    log.error("Failed to import data from file "+filename+" - file incomplete");
	                                } else {
                                	
	                                	try {
	                                		// Update Project Rankings
	                                		log.debug("Updating project team rankings");
	                                		myDB.DoProjectTeamRankings("teams_"+p.shortname+"_bk");
	                                		
	                                		// Add Indexes
	                                		log.debug("Adding table indexes");
	                                		String query = "ALTER TABLE teams_"+p.shortname+"_bk  ADD INDEX `project_rank_credit`(`project_rank_credit`), ADD INDEX `project_rank_rac`(`project_rank_rac`)";
	                                		myDB.RunQuery(query);
	                                		
	                                	} catch (java.sql.SQLException ss) {
	                                		log.error("Failed to swap team tables",ss);
	                                	}
	                                	
	                                	// set our update time
	                                    f = new File (filename);
	                                    timestamp = f.lastModified();
	                                    when = new Date(timestamp);
	                                    sdf = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm");
	                                    sdf.setTimeZone(TimeZone.getDefault());
	                                    String lastImport = sdf.format(when);
	                                    myDB.ProjectUpdateTeamsTime(p.project_id, lastImport);
	                                    teamImported = true;
	                                }
	                            }
	                            catch (java.sql.SQLException sqle) {
	                            	log.error("Failed processing TEAM file", sqle);
	                            }
	                            catch (Exception e) {
	                            	log.error("Failed processing TEAM File", e);
	                            }
                            }

                            //
                            // now do the USER file
                            //
                           
                            // Cache the user info from the current table
                            if (userImportNeeded == true || hostImportNeeded == true) {
	                            hUsers.clear();
	                            try {
	                            	myDB.UserRead(hUsers, "users_"+p.shortname);
	                            	log.info("Read in "+hUsers.size()+" users from project " + p.name);
	                            } catch (java.sql.SQLException ss) {
	                            	// table may not exist if project is new
	                            	log.error("Failed to read from table users_"+p.shortname);
	                            	try {
	                            		myDB.CreateUsersTable(p.shortname);
	                            	} catch (java.sql.SQLException ss2) {
	                            		log.error("Failed to create table users_"+p.shortname);
	                            	}
	                            }
                            }
                            
                            if (p.user_file != null && userImportNeeded == true) {
                            	
                            	// drop user backup table (may not exist)
                                try {
                                	myDB.DropTable("users_"+p.shortname+"_bk");
                                }
                                catch (java.sql.SQLException s) {
                                	// ignoring this, table probably doesn't exist (it shouldn't)
                                }
                                
                            	// create team backup table
                                myDB.CreateUsersTable(p.shortname+"_bk");

                                // import file
                                filename = p.data_dir+p.shortname+PLATFORM_DIR_CHAR+p.user_file;
                                updateUserCount = ReadProjectUserXMLFile(filename, "users_"+p.shortname+"_bk", p.project_id,dbTempFile);

                                if (updateUserCount < 0)
                                    log.error("Failed to import data from file "+filename+" - file incomplete");
                                else {

                                	try {
                                		// Update Rankings
                                		log.debug("Updating User Rankings");
                                		myDB.DoProjectUserRankings("users_"+p.shortname+"_bk");
                                		
                                		//Update user counts in teams table
                                		if (teamImported == true) {
                                			log.debug("Updating user counts for teams");
                                			myDB.DoUpdateTeamUserCounts("users_"+p.shortname+"_bk", "teams_"+p.shortname+"_bk");
                                		}
                                		
                                		// Add indexes
                                		log.debug("Adding indexes to table");
                                		String query = "ALTER TABLE users_"+p.shortname+"_bk ADD INDEX `user_cpid`(`user_cpid`), ADD INDEX `b_cpid_id`(`b_cpid_id`), ADD INDEX `b_cteam_id`(`b_cteam_id`), ADD INDEX `project_rank_credit`(`project_rank_credit`), ADD INDEX `project_rank_rac`(`project_rank_rac`)";
                                		myDB.RunQuery(query);
                                		

                                	} catch (java.sql.SQLException ss) {
                                		log.error("Failed to swap user tables",ss);
                                	}
                                	
                                    // set our update time
                                    f = new File (filename);
                                    timestamp = f.lastModified();
                                    when = new Date(timestamp);
                                    sdf = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm");
                                    sdf.setTimeZone(TimeZone.getDefault());
                                    String lastImport = sdf.format(when);
                                    myDB.ProjectUpdateUsersTime(p.project_id, lastImport);
                                    userImported=true;
                                     
                                }
                            }
                            
                            //
                            // Now do HOST file
                            //
                            
                            // Since we are not tracking history, we don't need to cache 
                            // this set...
                            
                            // make sure production table exists
                            try {
                        		myDB.CreateHostsTable(p.shortname);
                        	} catch (java.sql.SQLException ss2) {
                        		// ignore creation error
                        	}
                        	
                            if (p.host_file != null && hostImportNeeded == true) {
                            	
                            	
                            	log.debug("Starting host file import");
                            	
                            	// create backup table
                            	try {
                            		myDB.DropTable("hosts_"+p.shortname+"_bk");
                            	} catch (java.sql.SQLException ss2) {
                            		// shouldn't exist anyway
                            	}
                            	try {
                            		myDB.CreateHostsTable(p.shortname+"_bk");
                            	} catch (java.sql.SQLException ss2) {
                            		log.error("Failed to create backup table hosts_"+p.shortname+"_bk");
                            	}
                            	
                            	
                            	try {
                            		filename = p.data_dir+p.shortname+PLATFORM_DIR_CHAR+p.host_file;
                            		updateHostCount = ReadProjectHostXMLFile(filename,p.project_id,"hosts_"+p.shortname+"_bk",addMissingTypes,dbTempFile);
                            	}
                            	catch (Exception e) {
                            		log.error("Error",e);
                            	}
                                if (updateHostCount < 0) {
                                    log.error("Failed to import data from file "+filename+" - file incomplete");
                                } else {
                                	try {

                                		if (cr.GetValueBoolean("import", "rank_hosts",false)==true) {
	                                		// Update Rankings
	                                		log.debug("Updating Host Rankings");
	                                		myDB.DoProjectHostRankings("hosts_"+p.shortname+"_bk");
                                		}
                                		
                                		// Update computer counts in user table
                                		if (userImported==true) {
                                			log.debug("Updating user computer counts");
                                			myDB.DoUpdateComputerCounts("users_"+p.shortname+"_bk", "hosts_"+p.shortname+"_bk");
                                		}
                                		
                                		// Add indexes
                                		//log.debug("Adding indexes to table");
                                		//String query = "ALTER TABLE hosts_"+p.shortname+"_bk ADD INDEX `host_cpid`(`host_cpid`), ADD INDEX `b_cpid_id`(`b_cpid_id`), ADD INDEX `user_id`(`user_id`), ADD INDEX `project_rank_credit`(`project_rank_credit`), ADD INDEX `project_rank_rac`(`project_rank_rac`)";
                                		//myDB.RunQuery(query);
                                		//log.debug("Done adding indexes");

                                	} catch (java.sql.SQLException ss) {
                                		log.error("Failed to swap host tables",ss);
                                	}
                                	
                                	// set our update time
                                    f = new File (filename);
                                    timestamp = f.lastModified();
                                    when = new Date(timestamp);
                                    sdf = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm");
                                    sdf.setTimeZone(TimeZone.getDefault());
                                    String lastImport = sdf.format(when);
                                    myDB.ProjectUpdateHostsTime(p.project_id, lastImport);
                                    hostImported=true;
                                }
                            }   

                            // Move tables to production
                            try {
	                            if (teamImportNeeded == true && teamImported == true) {
	                        		// rename production table to old
	                        		log.debug("Renaming team table to production");
	                        		myDB.RenameTable("teams_"+p.shortname, "teams_"+p.shortname+"_old");
	                        		// rename backup to production
	                        		myDB.RenameTable("teams_"+p.shortname+"_bk", "teams_"+p.shortname);
	                        		// drop old
	                        		myDB.DropTable("teams_"+p.shortname+"_old");
	                            } else {
	                            	// drop backup table
	                            	if (teamImportNeeded == true) {
	                            		myDB.DropTable("teams_"+p.shortname+"_bk");
	                            	}
	                            }
	                            if (userImportNeeded == true && userImported == true) {
                            		// rename production table to old
                            		log.debug("Renaming user table to production");
                            		myDB.RenameTable("users_"+p.shortname, "users_"+p.shortname+"_old");
                            		// rename backup to production
                            		myDB.RenameTable("users_"+p.shortname+"_bk", "users_"+p.shortname);
                            		// drop old
                            		myDB.DropTable("users_"+p.shortname+"_old");
	                            } else {
	                            	if (userImportNeeded == true) {
	                            		myDB.DropTable("users_"+p.shortname+"_bk");
	                            	}
	                            }
	                            if (hostImportNeeded == true && hostImported == true) {
	                            	log.debug("Renaming host table to production");
	                            	// rename production table to old
                            		myDB.RenameTable("hosts_"+p.shortname, "hosts_"+p.shortname+"_old");
                            		// rename backup to production
                            		myDB.RenameTable("hosts_"+p.shortname+"_bk", "hosts_"+p.shortname);
                            		// drop old
                            		myDB.DropTable("hosts_"+p.shortname+"_old");
	                            } else {
	                            	if (hostImportNeeded == true) {
	                            		myDB.DropTable("hosts_"+p.shortname+"_bk");
	                            	}
	                            }
                            } 
                            catch (java.sql.SQLException se) {
                            	log.error("Failed to move tables to production",se);
                            }
                            
                            if (teamImportNeeded == true || userImportNeeded == true || hostImportNeeded == true) {
                            	// Update projects table with new counts
                            	try {
                            		log.info("Updating projects table counts");
                            		myDB.DoProjectStats(p.project_id,"users_"+p.shortname,"hosts_"+p.shortname,"teams_"+p.shortname);
                            	}
                            	catch (java.sql.SQLException se) {
                            		log.error("Failed to update project table stats",se);
                            	}
                            	log.info("Importing files done "+p.name+" data import");                            	
                            }
                            
                            // create a backup of the files for each one that was good
                            String backup_file;
                            if (backupFiles == true && userImportNeeded == true && updateUserCount >= 0) {
                                filename = p.data_dir+p.shortname+PLATFORM_DIR_CHAR+p.user_file;
                                backup_file = p.data_dir+p.shortname+PLATFORM_DIR_CHAR+p.shortname+"_"+p.user_file;
                                log.debug("Creating backup file "+backup_file);
                                BackupFile(filename,backup_file);
                            }
                            if (backupFiles == true && hostImportNeeded == true && updateHostCount >= 0) {
                                filename = p.data_dir+p.shortname+PLATFORM_DIR_CHAR+p.host_file;
                                backup_file = p.data_dir+p.shortname+PLATFORM_DIR_CHAR+p.shortname+"_"+p.host_file;
                                log.debug("Creating backup file "+backup_file);
                                BackupFile(filename,backup_file);
                            }
                            if (backupFiles == true && teamImportNeeded == true && updateTeamCount >= 0) {
                                filename = p.data_dir+p.shortname+PLATFORM_DIR_CHAR+p.team_file;
                                backup_file = p.data_dir+p.shortname+PLATFORM_DIR_CHAR+p.shortname+"_"+p.team_file;
                                log.debug("Creating backup file "+backup_file);
                                BackupFile(filename,backup_file);
                            }
                        }
                        catch (Exception e) {
                            log.error("Exception",e);
                        }
                    }
                }
                catch (java.sql.SQLException se) {
                    log.error("SQL Error",se);
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
                hCTeam = null;
                System.gc();
                
                log.info("End data file import");
            
            } // end if read_files=true
            
           
                        
            log.info("End data import");
            
        }
        catch (java.sql.SQLException se) {
            log.error("SQL Exception",se);
        }
        catch (Exception e) {
        	log.error("Exception",e);
        }
        
        // Release lock file
        try {
        	// Release the lock
        	lock.release();
        	// close the file
        	channel.close();
        }
        catch (Exception e) {}
    }
    

    public int ReadProjectUserXMLFile(String filename, String tablename, int project_id, String tempfile) {

        int added=-1;
        
        try {
            // open compressed file
            GZIPInputStream in = new GZIPInputStream(new FileInputStream(filename));
                        
            XMLReader parser = XMLReaderFactory.createXMLReader("org.apache.xerces.parsers.SAXParser");
            org.xml.sax.ContentHandler handler = new UserXMLFileHandler(tablename, project_id,hUsers,hCountries,hTeams,hCpid,myDB,racTime,tempfile); 
            
            parser.setContentHandler(handler);
            InputSource source = new InputSource(in);
            parser.parse(source);
            in.close();
            ((UserXMLFileHandler)handler).close();
            if (((UserXMLFileHandler)handler).fileOK == false) 
            	return -1;
                        
            myDB.RunQuery("LOAD DATA LOCAL INFILE '"+tempfile+"' IGNORE INTO TABLE `"+tablename+"` FIELDS TERMINATED BY '\t' ");
            
            added = ((UserXMLFileHandler)handler).added;
            log.info("Added "+added+" users to table "+tablename);
            
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
        catch (Exception e) {
        	log.error("Error Importing", e);
        }
        return (added);
    }

    public int ReadProjectHostXMLFile(String filename, int project_id, String tablename, boolean addMissingTypes, String tempfile) {

        int added=-1;
        
        try {
            // open compressed file
            GZIPInputStream in = new GZIPInputStream(new FileInputStream(filename));
            
            XMLReader parser = XMLReaderFactory.createXMLReader("org.apache.xerces.parsers.SAXParser");
            org.xml.sax.ContentHandler handler = new HostXMLFileHandler(hUsers,hPvendor, hPmodel, hOsname, myDB, racTime,tablename,addMissingTypes,tempfile); 
            
            parser.setContentHandler(handler);
            InputSource source = new InputSource(in);
            parser.parse(source);
            ((HostXMLFileHandler)handler).close();
            added = ((HostXMLFileHandler)handler).added;
            in.close();
            
            if (((HostXMLFileHandler)handler).fileOK == false) 
            	return -1;
            myDB.RunQuery("LOAD DATA LOCAL INFILE '"+tempfile+"' IGNORE INTO TABLE `"+tablename+"` FIELDS TERMINATED BY '\t' ");
            
            int anonymous = ((HostXMLFileHandler)handler).anonymous;            
            log.info("Added "+added+" to hosts to table "+tablename+" ("+ anonymous+" anonymous)");

        }
        catch (java.io.FileNotFoundException e) {
            log.error("File Not Found",e);
        }
        catch (java.io.IOException ioe) {
            log.error("I/O Error",ioe);
        }
        catch (SAXException se) {
            log.error("SAX Error",se);
        }
        catch (Exception e) {
        	log.error("Error importing",e);
        }
        return (added);
    }    
    
    public int ReadProjectTeamXMLFile(String filename, String tablename, String tempfile) {

        int added=-1;
        
        try {
            // open compressed file
            GZIPInputStream in = new GZIPInputStream(new FileInputStream(filename));
            
            XMLReader parser = XMLReaderFactory.createXMLReader("org.apache.xerces.parsers.SAXParser");
            org.xml.sax.ContentHandler handler = new TeamXMLFileHandler(tablename,hTeams,hCountries,hCTeam, myDB, racTime,tempfile); 
            
            parser.setContentHandler(handler);
            InputSource source = new InputSource(in);
            parser.parse(source);
            in.close();
            ((TeamXMLFileHandler)handler).close();
            if (((TeamXMLFileHandler)handler).fileOK == false) 
            	return -1;
            
            myDB.RunQuery("LOAD DATA LOCAL INFILE '"+tempfile+"' IGNORE INTO TABLE `"+tablename+"` FIELDS TERMINATED BY '\t' ");
            
            added = ((TeamXMLFileHandler)handler).added;            
            log.info("Added "+added+" teams to table "+tablename);
            
        }
        catch (java.io.FileNotFoundException e) {
            log.error("File Not Found",e);
        }
        catch (java.io.IOException ioe) {
            log.error("I/O Error",ioe);
        }
        catch (SAXException se) {
            log.error("SAX Error",se);
        }
        catch (Exception e) {
        	log.error("Failed import",e);
        }        
        return (added);
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
            
            // Attempt to set the file time/date to match
            try {
            	File f = new File (from_name);
            	long timestamp = f.lastModified();
            	f = new File (to_name);
            	f.setLastModified(timestamp);
            }
            catch (Exception e) {
            	log.warn("Unable to set file date/time on copied file",e);
            }
            
        }
        catch (Exception e)
        {
            log.error(e);
        }
        
    }
    
}
