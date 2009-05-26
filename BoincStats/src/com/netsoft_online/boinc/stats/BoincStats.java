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
import java.nio.channels.*;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
//import org.apache.log4j.BasicConfigurator;

public class BoincStats {

    Database myDB;
    //Hashtable<String, Integer> hCountries = null;
    Hashtable<String,CPID> hCpid=null;
    Hashtable<String, CTeam> hCTeam=null;
    double racTime = 0;
    int history_day=0;
    String ymm="000";
    long maxCTeamID=0;
    long maxCPIDID=0;
    String PLATFORM_DIR_CHAR=File.separator;
    
    static Logger log = Logger.getLogger(BoincStats.class.getName());  // log4j stuff
    
    public BoincStats()
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

        log.info("Boinc Stats Starting up");
        
        BoincStats myRun = new BoincStats();
        myRun.RunImport();
    }

    
    public void RunImport() {
                
        myDB = new Database();
        
        ConfigReader cr = new ConfigReader();
        try {
            cr.ReadConfigFile("boincstats.ini");
        } 
        catch (java.io.IOException ioerror) {
            log.error("Failed to read boincstats.ini file");
            return;
        }
        
        String database_driver = cr.GetValueString("database", "driver");
        String database_name = cr.GetValueString("database","dbname");
        String database_user = cr.GetValueString("database", "dbuser");
        String database_pass = cr.GetValueString("database","dbpass");
        String lockfile = cr.GetValueString("lockfile", "name","/home/drews/boinc/boinc.lck");
        String dbTempFile = cr.GetValueString("database", "tempfile","/tmp/boinc.tmp");
        int lockRetries = cr.GetValueInt("lockfile", "retries",60);
        int lockRetryWait = cr.GetValueInt("lockfile","retry_wait", 5);
        
        myDB.SetDatabaseInfo(database_driver,database_name, database_user,database_pass);
        
        try {
            
        	// create lock file
        	File file=null;
        	FileChannel channel=null;
        	FileLock lock=null;
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
            //hCountries = new Hashtable<String, Integer>();
            hCpid = new Hashtable<String,CPID>();
            hCTeam = new Hashtable<String, CTeam>();

            // login to dB
            myDB.LoginToDatabase();
           
            // compute the time for our rac decay
            racTime = System.currentTimeMillis() / 1000;
            
            // Read in the cpid table
            maxCPIDID = myDB.CPIDRead(hCpid);
            log.info("Found " + hCpid.size() + " cpid entries");
            
            // Read in the cteam table
            maxCTeamID=myDB.CTeamRead(hCTeam);
            log.info("Found "+hCTeam.size() + " Combined team entries");
                        
            // Get the history day update value
            history_day = myDB.GetCurrentDay("history90");
            if (history_day == 0) {
                // error - as in big error!
                log.warn("Failed to get our total_credit day counter from the DB");
                if (cr.GetValueBoolean("import","advance_counters",true))
                	history_day=0;
                else 
                	history_day=1;
            }
            if (cr.GetValueBoolean("import","advance_counters",true)) {
                history_day++;
                if (history_day > 91)
                    history_day = 1;
            }
            
            
            Calendar cal = Calendar.getInstance(TimeZone.getDefault());
            
            String year, month;
            String df = "yyyy";
            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat(df);
            sdf.setTimeZone(TimeZone.getDefault());
            year = sdf.format(cal.getTime());
            df = "MM";
            sdf = new java.text.SimpleDateFormat(df);
            sdf.setTimeZone(TimeZone.getDefault());
            month = sdf.format(cal.getTime());
            if (month.length()==1) month = "0"+month;
            ymm = year.charAt(3)+month;
            
            //
            // Combined Stats Processing
            //
            
            // For each project, read in the list of users from its 
            // project table. Sum up the total_credit, rac in our
            // memory hash, update project/active project count.
            //
            // In theory, all the CPIDs should be in the hash table
            // from the project data import program.
            //
            boolean errorHappened = false;
            if (cr.GetValueBoolean("import","compute_stats",true)) {   
            	log.info("Begin stats calculations");
            	
	            // 
	            // Drop the backup tables (shouldn't exist)
	            //
	            try {
	            	myDB.DropTable("cpid_bk");
	            } catch (java.sql.SQLException ss) {}
	            try {
	            	myDB.DropTable("cteams_bk");
	            } catch (java.sql.SQLException ss) {}
	            try {
	            	myDB.DropTable("rank_user_tc_bk");
	            } catch (java.sql.SQLException ss) {}
	            try {
	            	myDB.DropTable("rank_user_rac_bk");
	            } catch (java.sql.SQLException ss) {}
	            try {
	            	myDB.DropTable("rank_team_tc_bk");
	            } catch (java.sql.SQLException ss) {}
	            try {
	            	myDB.DropTable("rank_team_rac_bk");
	            } catch (java.sql.SQLException ss) {}
	            try {
	            	myDB.DropTable("cpid_map_bk");
	            } catch (java.sql.SQLException ss) {}
	            try {
	            	myDB.DropTable("host_map_bk");
	            } catch (java.sql.SQLException ss) {}
	            try {
	            	myDB.DropTable("cpcs_bk");
	            } catch (java.sql.SQLException ss) {}
	            try {
	            	myDB.DropTable("country_rank_bk");
	            } catch (java.sql.SQLException ss) {}
	            
	            //
	            // Now create the "backup" tables            
	            //
	            if (errorHappened == false) {
	            	try {
		            	// Create backup cpid table
		            	myDB.CreateCPIDTable("cpid_bk");
		            	
		            	// Create backup cteam table
		            	myDB.CreateCTeamTable("cteams_bk");
		            	
		            	// Create backup global_user_rankings table
		            	myDB.CreateUserRankingTCTable("rank_user_tc_bk");
	
		            	// Create backup global_user_rankings table
		            	myDB.CreateUserRankingRacTable("rank_user_rac_bk");
	
		            	// Create backup rank_team_tc table
		            	myDB.CreateCTeamRankingTCTable("rank_team_tc_bk");
		            	
		            	// Create backup rank_team_rac table
		            	myDB.CreateCTeamRankingRacTable("rank_team_rac_bk");
		            	
		            	// Create backup user mapping table
		            	myDB.CreateCPIDMapTable("cpid_map_bk");
		            	
		            	// Create backup host mapping table
		            	myDB.CreateHostMapTable("host_map_bk");
		            	
		            	// Create backup cpcs table
		            	myDB.CreateCPCSTable("cpcs_bk");
		            	
		            	// Create backup country rank table
		            	myDB.CreateCountryRankTable("country_rank_bk");
	            	}
	            	catch (java.sql.SQLException s) {
	            		log.error("SQL Error",s);
	            		errorHappened = true;
	            	}
	            }
	            
	            // Also read in all the team data and update the
	            // team hash table entries. Again, all teams should
	            // already be in the cteam table
	            
	            Vector<Project> myProjects = new Vector<Project>();
	            if (errorHappened == false) {
		            try {
		                myDB.ProjectsRead(myProjects);
		                
		                for(int i=0;i<myProjects.size();i++) {
		                    
		                    try {
		                        Project p = (Project)myProjects.elementAt(i);
		                       
		                        log.info("Reading project "+p.name+" data");
		                        
		                        // Read all user data into the hash
		                        long count = myDB.ProjectUserRead("users_"+p.shortname, hCpid, racTime);
		                        log.info("Read "+count+" User Entries");
		                        
		                        // Read all team data into the hash
		                        count = myDB.ProjectTeamRead("teams_"+p.shortname, hCTeam, racTime);
		                        log.info("Read "+count+" Team Entries");
		                    }
		                    catch (Exception e) {
		                        log.error(e);
		                    }
		                }	                
		            }
		            catch (java.sql.SQLException se) {
		                log.error(se);
		                errorHappened = true;
		            }
	            }            
	            
	            // 
	            // Dump hashes into their backup tables
	            // TODO: INI option to retire/not retire old cpids 
	            //
	            if (errorHappened == false) {
		            // Enumerate the hCpid hash
		            log.info("Saving cpid data");
		            File outFile;
		            PrintWriter Out = null;
		            
		            try {
		          	  outFile = new File(dbTempFile);
		          	  Out = new PrintWriter(new BufferedWriter(new FileWriter(outFile)));
		            } catch (IOException ex){
		          	  log.error("Error opening file " + dbTempFile,ex);
		          	  return;
		            }
		            
		            Enumeration<String> k = hCpid.keys();
		            while (k.hasMoreElements()) {
		            	try {
		            		String key = (String) k.nextElement();
		            		CPID myCPID = hCpid.get(key);
		            		if (myCPID.b_cpid_id == 0) {
		            			myCPID.b_cpid_id = ++maxCPIDID;
		            		}
		            		if (myCPID.project_count > 0) {
		            			//myDB.CPIDAddEntry("cpid_bk", myCPID.b_cpid_id, myCPID.user_cpid, myCPID.name, myCPID.create_time,myCPID.country_id, myCPID.project_count, myCPID.active_project_count, 0, myCPID.total_credit, myCPID.rac, myCPID.rac_time, myCPID.hosts_visible);
		            			String joindate="0000-00-00";
		            			String joinyear="0000";
		            			if (myCPID.create_time > 0) {
			            			Date when = new Date((long)myCPID.create_time*(long)1000);
			            	        sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
			            	        sdf.setTimeZone(TimeZone.getDefault());
			            	        joindate = sdf.format(when);
			            	        sdf = new java.text.SimpleDateFormat("yyyy");
			            	        joinyear = sdf.format(when);		            	        
		            			}
		            			if (myCPID.name == null) myCPID.name = "";
		            			
		            			Out.println(myCPID.b_cpid_id+"\t"+myCPID.user_cpid+"\t"+myDB.MakeSafe(myCPID.name)+"\t"+joindate+"\t"+joinyear+"\t"+myCPID.country_id+"\t"+myCPID.project_count+"\t"+myCPID.active_project_count+"\t0\t"+myCPID.total_credit+"\t"+myCPID.rac+"\t"+myCPID.rac_time+"\t"+myCPID.hosts_visible+"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t");
		            		}
		            	}
		            	//catch (java.sql.SQLException s) {
		            	//	log.error("SQL Error",s);
		            	//}
		            	catch (Exception e) {
		            		log.error("Exception",e);
		            	}
		            }
		            
		            try {
		            	Out.close();
		            	myDB.RunQuery("LOAD DATA LOCAL INFILE '"+dbTempFile+"' IGNORE INTO TABLE `cpid_bk` FIELDS TERMINATED BY '\t' ");
		            }
		            catch (java.sql.SQLException s) {
		            	log.error("SQL Error",s);		            	
		            }
		            catch (Exception e) {
		            	log.error("Exception",e);		            	
		            }
		            
		            // Enumerate the hCpid hash
		            // TODO: Add INI option to keep old teams
		            log.info("Saving cteam data");
		            k = hCTeam.keys();
		            while (k.hasMoreElements()) {
		            	try {
		            		String key = (String) k.nextElement();
		            		CTeam myTeam = hCTeam.get(key);
		            		if (myTeam.b_cteam_id == 0) {
		            			myTeam.b_cteam_id = ++maxCTeamID;
		            		}
		            		if (myTeam.project_count > 0) 
		            			myDB.CTeamAddEntry("cteams_bk", myTeam.b_cteam_id, myTeam.team_cpid, myTeam.name, 0, myTeam.total_credit, myTeam.rac, myTeam.rac_time, myTeam.project_count, myTeam.active_project_count, myTeam.country_id, myTeam.create_time);
		            	}
		            	catch (java.sql.SQLException s) {
		            		log.error("SQL Error",s);
		            	}
		            }
	            }
	            
	            //
	            // Create a "mapping" table so we have an easy way
	            // to find out what projects a cpid or cteam is part of
	            // and the host table for cpcs calculation and global user
	            // host counts
	            //
	            if (errorHappened == false) {
		            log.info("creating cpid and host map tables");
		            for(int i=0;i<myProjects.size();i++) {
		                
		                try {
		                    Project p = (Project)myProjects.elementAt(i);
		                    myDB.MapUserTableInsert("cpid_map_bk", p.shortname, p.project_id);
		                    myDB.MapHostTableInsert("host_map_bk",p.shortname,p.project_id);
		                }
		                catch (java.sql.SQLException s) {
		                	log.error("SQL Error",s);
		                	errorHappened = true;
		                }
		            }
	            }            
	            
	            // 
	            // Do Cross-project credit per cpu second
	            //
	            if (errorHappened == false) {
	            	try {
	            		myDB.CalculateCPCSTable("host_map_bk", "cpcs_bk");
	            	}
	            	catch (java.sql.SQLException s) {
	            		log.error("SQL Error",s);
	            		errorHappened = true;
	            	}
	            }
	
	            
	            // 
	            // Update user host counts
	            //
	            if (errorHappened == false) {
		            log.info("Updating user's active host counts");
		            if (errorHappened == false) {
		            	try {
		            		myDB.CPIDUpdateHostCounts("cpid_bk", "host_map_bk");
		            	}
		            	catch (java.sql.SQLException s) {
		            		log.error("SQL Error",s);
		            		errorHappened = true;
		            	}
		            }
	            }
	            
	            // 
	            // Update cteam user counts
	            //
	            if (errorHappened == false) {
		            log.info("Updating team user counts");
		            if (errorHappened == false) {
		            	try {
		            		myDB.CTeamUpdateUserCounts("cteams_bk", "cpid_map_bk");
		            	}
		            	catch (java.sql.SQLException s) {
		            		log.error("SQL Error",s);
		            		errorHappened = true;
		            	}
		            }
	            }            
	            
	            //
	            // Calculate global rankings on the cpid and cteam tables
	            // This is all the fun for the global_user_rankings table
	            // and the global_team_rankings table
	            //
	            // TODO: Remove/speed up(?) computer count rankings
	            if (errorHappened == false) {
	            	try {
	            		myDB.DoGlobalUserRankings("cpid_bk","rank_user_tc_bk","rank_user_rac_bk");
	            		myDB.DoGlobalCTeamRankings("cteams_bk", "rank_team_tc_bk", "rank_team_rac_bk");
	            	}
	            	catch (java.sql.SQLException s) {
	            		log.error("SQL Error",s);
	            		errorHappened = true;
	            	}
	            }
	            
	            // 
	            // Calculate Country totals and rankings
	            if (errorHappened == false) {
	            	try {
	            		myDB.DoGlobalCountryRankings("cpid_bk","country_rank_bk");
	            	}
	            	catch (java.sql.SQLException s) {
	            		log.error("SQL Error",s);
	            		errorHappened = true;
	            	}
	            }
	            
	            // 
	            // set autonumber on cpid table
	            //
	            if (errorHappened == false) {
	            	try {
	            		maxCPIDID++;
	            		myDB.RunQuery("ALTER TABLE `cpid_bk` MODIFY COLUMN `b_cpid_id` BIGINT(20) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT = "+maxCPIDID);
	            	}
	            	catch (java.sql.SQLException s) {
	            		log.error("SQL Error",s);
	            		errorHappened = true;
	            	}
	            }
	            
	            //
	            // set autonumber on cteams table
	            //
	            if (errorHappened == false) {
	            	try {
	            		maxCTeamID++;
	            		myDB.RunQuery("ALTER TABLE `cteams_bk` MODIFY COLUMN `table_id` BIGINT(20) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT = "+maxCTeamID);
	            	}
	            	catch (java.sql.SQLException s) {
	            		log.error("SQL Error",s);
	            		errorHappened = true;
	            	}
	            }
	            
	            
	            // 
	            // Update project "1" entries (total_credit, rac,user,host,team,country counts)
	            //
	            if (errorHappened == false) {
	            	try {
	            		log.info("Updating combined project stats totals");
	            		myDB.DoCombinedStatsProjectUpdates("cpid_bk","cteams_bk");
	            	}
	            	catch (java.sql.SQLException s) {
	            		log.error("SQL Error",s);
	            	}
	            }
	            
	            //
	            // Move tables from backup to production
	            //
	            if (errorHappened == false) {
	            	
	            	try {
		            	myDB.FlipTables("cpid", "cpid_bk");
		            	myDB.DropTable("cpid_bk");
		            } catch (java.sql.SQLException s) {
		            	log.error("Failed to flip/drop cpid tables");
		            }
		                        	
	            	try {
		            	myDB.FlipTables("cteams", "cteams_bk");
		            	myDB.DropTable("cteams_bk");
		            } catch (java.sql.SQLException s) {
		            	log.error("Failed to flip/drop cteams tables");
		            }
		            
		            try {
		            	myDB.FlipTables("rank_user_tc", "rank_user_tc_bk");
		            	myDB.DropTable("rank_user_tc_bk");
		            } catch (java.sql.SQLException s) {
		            	log.error("Failed to flip/drop rank_user_tc tables");
		            }
	
		            try {
		            	myDB.FlipTables("rank_user_rac", "rank_user_rac_bk");
		            	myDB.DropTable("rank_user_rac_bk");
		            } catch (java.sql.SQLException s) {
		            	log.error("Failed to flip/drop rank_user_rac tables");
		            }
	
	            	// rank_team_tc table
		            try {
		            	myDB.FlipTables("rank_team_tc", "rank_team_tc_bk");
		            	myDB.DropTable("rank_team_tc_bk");
		            } catch (java.sql.SQLException s) {
		            	log.error("Failed to flip/drop rank_team_tc tables");
		            }
		            
		            // rank_team_rac table
		            try {
		            	myDB.FlipTables("rank_team_rac", "rank_team_rac_bk");
		            	myDB.DropTable("rank_team_rac_bk");
		            } catch (java.sql.SQLException s) {
		            	log.error("Failed to flip/drop rank_team_rac tables");
		            }
		            
		            try {
		            	myDB.FlipTables("cpid_map", "cpid_map_bk");
		            	myDB.DropTable("cpid_map_bk");
		            } catch (java.sql.SQLException s) {
		            	log.error("Failed to flip/drop cpid_map tables");
		            }
	            	
		            //try { // TODO: Uncomment
		            //	// special temporary table
		            //	myDB.DropTable("host_map_bk");
		            //} catch (java.sql.SQLException s) {
		            //	log.error("Failed to flip/drop host_map_bk tables");
		            //}
	            	
		            try {
		            	myDB.FlipTables("cpcs", "cpcs_bk");
		            	myDB.DropTable("cpcs_bk");
		            } catch (java.sql.SQLException s) {
		            	log.error("Failed to flip/drop cpcs tables");
		            }
	            
		            try {
		            	myDB.FlipTables("country_rank", "country_rank_bk");
		            	myDB.DropTable("country_rank_bk");
		            } catch (java.sql.SQLException s) {
		            	log.error("Failed to flip/drop country_rank tables");
		            }
		            
	            }        
			}

            if (cr.GetValueBoolean("import","record_history",true)) {
            	
	            //
	            // Record all the history entries
	            //
				Vector<Project> myProjects = new Vector<Project>();
			    try {
			        myDB.ProjectsRead(myProjects);
			    }
			    catch (java.sql.SQLException s) {}
			
	            for(int i=0;i<myProjects.size();i++) {
	                
	            	Project p;
	            	p = (Project)myProjects.elementAt(i);
	            	log.info("Storing history records for "+p.shortname);
	            	
	            	// Record the user tc history
	            	log.debug("Recording user total credit history for project "+p.shortname);
	            	ProcessHistoryTable("history_user_tc_"+p.shortname,"history_user_tc_"+p.shortname+"_bk","users_"+p.shortname,"user_id","total_credit",ymm, history_day, false, true);
	                
	                // Record the user rac history
	                log.debug("Recording user rac history for project "+p.shortname);
	                ProcessHistoryTable("history_user_rac_"+p.shortname,"history_user_rac_"+p.shortname+"_bk","users_"+p.shortname,"user_id","rac",ymm, history_day, true, true);	                	                
	                
	                // Record the team tc history
	                log.debug("Recording team total credit history for project "+p.shortname);
	                ProcessHistoryTable("history_team_tc_"+p.shortname,"history_team_tc_"+p.shortname+"_bk","teams_"+p.shortname,"team_id","total_credit",ymm, history_day, false, true);
	                
	                // Record the team rac history
	                log.debug("Recording team rac history for project "+p.shortname);
	                ProcessHistoryTable("history_team_rac_"+p.shortname,"history_team_rac_"+p.shortname+"_bk","teams_"+p.shortname,"team_id","rac",ymm, history_day, true, true);
	                
	                // Record the team user count history
	                log.debug("Recording team user count history for project "+p.shortname);
	                ProcessHistoryTable("history_team_uc_"+p.shortname,"history_team_uc_"+p.shortname+"_bk","teams_"+p.shortname,"team_id","nusers",ymm, history_day, false, false);
	                
	            }
	
	            log.info("Storing projects history");
	            // Record the project TC history	            
	            ProcessHistoryTable("history_projects_tc","history_projects_tc_bk","projects","project_id","total_credit",ymm, history_day, false, true);	            
	            
	            // Record the project RAC history
	            ProcessHistoryTable("history_projects_rac","history_projects_rac_bk","projects","project_id","rac",ymm, history_day, true, true);	            

	            // Record the project user,host,team count history 
	            ProcessHistoryTable("history_projects_users","history_projects_users_bk","projects","project_id","user_count",ymm, history_day, false, false);	            
	            ProcessHistoryTable("history_projects_hosts","history_projects_hosts_bk","projects","project_id","host_count",ymm, history_day, false, false);
	            ProcessHistoryTable("history_projects_teams","history_projects_teams_bk","projects","project_id","team_count",ymm, history_day, false, false);
	            
	            // Record the project active user,host/team count history
	            ProcessHistoryTable("history_projects_ausers","history_projects_ausers_bk","projects","project_id","active_users",ymm, history_day, false, false);	            
	            ProcessHistoryTable("history_projects_ahosts","history_projects_ahosts_bk","projects","project_id","active_hosts",ymm, history_day, false, false);
	            ProcessHistoryTable("history_projects_ateams","history_projects_ateams_bk","projects","project_id","active_teams",ymm, history_day, false, false);
	            
	            // Record the cteam TC History
	            ProcessHistoryTable("history_cteam_tc","history_cteam_tc_bk","cteams","table_id","total_credit",ymm, history_day, false, true);
	            
	            // Record the cteam RAC History
	            ProcessHistoryTable("history_cteam_rac","history_cteam_rac_bk","cteams","table_id","rac",ymm, history_day, true, true);
	            
	            // Record the cteam user count
	            ProcessHistoryTable("history_cteam_uc","history_cteam_uc_bk","cteams","table_id","nusers",ymm, history_day, true, true);
	            
	            // Record the cteam tc rank
	            ProcessHistoryTable("history_cteam_tcr","history_cteam_tcr_bk","cteams","table_id","global_credit",ymm, history_day, true, true);
	            
	            // Record the cteam rac rank
	            ProcessHistoryTable("history_cteam_racr","history_cteam_racr_bk","cteams","table_id","global_rac",ymm, history_day, true, true);
            
            
	            //
	            // Update projects table "yesterday" counts
	            //
	            try {
	            	log.info("Updating \"yesterday\" counts");
	            	myDB.RunQuery("update projects set y_host_count=z_host_count,y_user_count=z_user_count,y_team_count=z_team_count,y_active_hosts=z_active_hosts,y_active_users=z_active_users,y_active_teams=z_active_teams,y_total_credit=z_total_credit,y_rac=z_rac,y_country_count=z_country_count");
	            	myDB.RunQuery("update projects set z_host_count=host_count,z_user_count=user_count,z_team_count=team_count,z_active_hosts=active_hosts,z_active_users=active_users,z_active_teams=active_teams,z_total_credit=total_credit,z_rac=rac,z_country_count=country_count");
	            }
	            catch (java.sql.SQLException s) {
	            	log.error("SQL Error",s);
	            }

            }
            
            // set our update time to our racTime
            Double d = new Double(racTime);
            long timestamp = d.longValue() * 1000; 
            Date when = new Date(timestamp);
            sdf = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            sdf.setTimeZone(TimeZone.getDefault());
            String lastImport = sdf.format(when);
            myDB.ProjectUpdateTime(1, lastImport);

            // now set the current day since we are done with the import
            if (cr.GetValueBoolean("import","advance_counters",true)) {
                myDB.SetCurrentDay("history90",history_day);
                Integer i = new Integer(ymm);
                myDB.SetCurrentDay("ymm", i.intValue());
            }

            //
            // TODO: Export data
            //
            String export_path = cr.GetValueString("export", "basepath");
            export_path += File.separatorChar;
                
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

            // Release lock file
            try {
            	// Release the lock
            	lock.release();
            	// close the file
            	channel.close();
            }
            catch (Exception e) {}
            	
            
            log.info("End stats update");
            
        }
        catch (java.sql.SQLException se) {
            log.error(se);
        }
    }    
    
    void ProcessHistoryTable (String oldHistoryTable, String newHistoryTable, String dataTable, String keyColumnName, String dataColumnName, String ymm, int history_day, boolean racTable, boolean bigintTable) {
    	
        try {
            // drop the backup history table (shouldn't exist)
        	try {
        		myDB.DropTable(newHistoryTable);
        	}
        	catch (java.sql.SQLException s) {}
        	
        	// Create history backup table
        	if (racTable== true) {
        		myDB.CreateRacHistoryTable(newHistoryTable, keyColumnName);
        	} else if (bigintTable == true) {
        		myDB.CreateBigIntHistoryTable(newHistoryTable, keyColumnName);
        	} else {
        		myDB.CreateIntHistoryTable(newHistoryTable, keyColumnName);
        	}

        	// Try creating old table, should exist, but in case this
        	// is a new project...
        	try {
            	if (racTable== true) {
            		myDB.CreateRacHistoryTable(oldHistoryTable, keyColumnName);
            	} else if (bigintTable == true) {
            		myDB.CreateBigIntHistoryTable(oldHistoryTable, keyColumnName);
            	} else {
            		myDB.CreateIntHistoryTable(oldHistoryTable, keyColumnName);
            	}        		
            	log.warn("Table "+oldHistoryTable+" didn't exist - new project?");
        	} catch (java.sql.SQLException s) { }
        	
            // Insert records
        	if (racTable==true)
        		myDB.ProcessRACHistory(oldHistoryTable, newHistoryTable, dataTable, keyColumnName, dataColumnName, ymm, history_day);
        	else
        		myDB.ProcessHistory(oldHistoryTable, newHistoryTable, dataTable, keyColumnName, dataColumnName, ymm, history_day);
        	
        	// rename to production
        	try {
            	myDB.FlipTables(oldHistoryTable, newHistoryTable);
            	myDB.DropTable(newHistoryTable);
            } catch (java.sql.SQLException s) {
            	log.error("Failed to flip/drop "+oldHistoryTable+"//"+newHistoryTable+" tables");
            }
        }
        catch (java.sql.SQLException s) {
        	log.error("Failed recording history",s);
        }        
    }
}
