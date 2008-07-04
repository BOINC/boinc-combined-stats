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
import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;
import java.util.*;

import org.apache.log4j.Logger;


public class TeamXMLFileHandler extends DefaultHandler {

  private boolean inTeams = false;
  private boolean inTeam = false;
  private boolean inUser = false;
  private Hashtable<String, User> userHashTable=null;
  private Hashtable<String, Team> teamHashTable=null;
  private Hashtable<String, Integer> countryHashTable=null;
  private Hashtable<String, CTeam> cteamHashTable=null;
  private Database myDB=null;
  private String parseDataValue=null;
  private int project_id;
  private long racComputeTime=0;
  static Logger log = Logger.getLogger(TeamXMLFileHandler.class.getName());  // log4j stuff
  private boolean bAddCrap=false;
  //private Vector users_in_team=null;
  
  private int team_id=0;
  private int type=0;
  private String name="";
  private int founder_id=0;
  private String total_credit="0";
  private String expavg_credit="0";
  private String expavg_time="0";
  private int nusers=0;
  private String founder_name="";
  private String create_time="0";
  private String url="";
  private String description="0";
  private String country="";
  private int country_id=0;

  //private int debug=0;

  private int tc_day=0;
  private int rac_day=0;
  private int week=0;

  
  public int added=0;
  public int updated=0;
  
  
  public TeamXMLFileHandler(Hashtable<String, User> uht,  Hashtable<String, Team> tht, Hashtable<String, Integer> cntry, Hashtable<String, CTeam> cteam, Database db, int pid, long rt, int tcday, int racday) {
      userHashTable = uht;
      countryHashTable = cntry;
      teamHashTable = tht;
      tc_day = tcday;
      rac_day = racday;
      cteamHashTable = cteam;
      
      //users_in_team = new Vector();
      
      myDB = db;
      project_id = pid;
      racComputeTime = rt;
      
      Calendar cal = Calendar.getInstance(TimeZone.getDefault());
      String df = "w";
      java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat(df);
      sdf.setTimeZone(TimeZone.getDefault());
      week = Integer.parseInt(sdf.format(cal.getTime()));
      //df = "dd";
      //sdf = new java.text.SimpleDateFormat(df);
      //sdf.setTimeZone(TimeZone.getDefault());
      //mday = Integer.parseInt(sdf.format(cal.getTime()));
  }
  
  public void startElement(String namespaceURI, String localName,
   String qualifiedName, Attributes atts) throws SAXException {
    
    if (localName.equalsIgnoreCase("team")) inTeam = true;
    if (localName.equalsIgnoreCase("teams")) inTeams = true;
    if (localName.equalsIgnoreCase("user")) inUser = true;
    parseDataValue="";
  }

  private String DecayRac(String current_rac, String rac_time) {
      double M_LN2 = 0.693147180559945309417;
      double credit_half_life = 86400 * 7;
      
      Double d = new Double(rac_time);      
      double avg_time = d.doubleValue();
      d = new Double(current_rac);
      double avg = d.doubleValue();
      
      if (racComputeTime == 0) {
          // get current time
          racComputeTime = System.currentTimeMillis() / 1000;
      }

      double diff = racComputeTime - avg_time;
      double weight = java.lang.Math.exp(-diff * M_LN2/credit_half_life);
      double avgr = avg * weight;
      
      if (avgr < 0.009) avgr = 0.0;
      d = new Double(avgr);
      return d.toString();      

  }
  
  public void endElement(String namespaceURI, String localName,
   String qualifiedName) throws SAXException {

      if (inTeams) {
          if (inUser) {
              nusers++;
//              if (localName.equalsIgnoreCase("id")) {                 
//                  users_in_team.add(new Integer(parseDataValue));
//              }
          } else if (inTeam) {
              if (localName.equalsIgnoreCase("team")) {
                  inTeam = false;

                  //recompute the RAC
                  String rac;
                  rac = DecayRac(expavg_credit,expavg_time);
                  String rac_time = ""+racComputeTime;
                  
                  // get the primary key for our user entry
                  // by looking in the user hashtable
                  int b_users_id=0;
                  Integer u = new Integer(founder_id);
                  String t = u.toString();
                  if (userHashTable.containsKey((Object)t)) {
                      // it's in there
                      User uh = (User) userHashTable.get((Object)t);
                      b_users_id = uh.table_id;     
                      
                  } else {
                      log.debug("Founder id not found");
                  }
                  
                  // got the complete data for a single host entry
                  // now lookup in hashtable to see if they are in there
                  u = new Integer(team_id);
                  t = u.toString();

                  
                  int cteam_table_id=0;
                  if (cteamHashTable.containsKey(name.toLowerCase())) {
                      CTeam ct = (CTeam)cteamHashTable.get(name.toLowerCase());
                      cteam_table_id = ct.table_id;
                  } else {
                      // some unicode or something is messing up the match
                      // so try reading it from the db
                      try {
                          cteam_table_id = myDB.CTeamGetTeamIDFromDB(name);                          
                      }
                      catch (java.sql.SQLException ee) {
                          log.error(ee);
                      }
                      if (cteam_table_id == 0) {
                          // well, we need to add it
                          try {
                              cteam_table_id = myDB.CTeamAddEntry(name);
                              CTeam ct = new CTeam();
                              ct.table_id = cteam_table_id;
                              ct.name=name;
                              cteamHashTable.put(name.toLowerCase(),ct);                          
                          }
                          catch (java.sql.SQLException se) {
                              log.error(se);
                          }
                      }
                  }
                  
                  int table_id=0;
                  if (teamHashTable.containsKey(t)) {
                      // update entry
                      updated++;
                      Team h = (Team) teamHashTable.get(t);
                      
                      try {     
                          int rows;
                          rows = myDB.TeamUpdateEntry(h.table_id, cteam_table_id,type, name, founder_id, b_users_id, total_credit, rac, rac_time, nusers, founder_name, create_time, url, description, country_id);
                          if (rows == 1) {
                              rows = myDB.TeamUpdateTotalCreditHistory(h.table_id,tc_day,week,total_credit);
                              if (rows == 0) myDB.TeamAddTotalCreditHistory(h.table_id, tc_day, week, total_credit);
                              rows = myDB.TeamUpdateRACHistory(h.table_id, rac_day, rac);
                              if (rows == 0) myDB.TeamAddRACHistory(table_id, rac_day, rac);
                          }
                      }
                      catch (java.sql.SQLException se) {
                          log.error(se);
                      }
                      
                      table_id=h.table_id;
                      //now remove entry. Any remaining entries 
                      // after import runs are deleted from the table
                      teamHashTable.remove(t);
                      
                  } else {
                      // add entry
                      added++;
                      try {
                          
                          table_id=myDB.TeamAddEntry(project_id, team_id, cteam_table_id, type, name, founder_id, b_users_id, total_credit, rac, rac_time, nusers, founder_name, create_time, url, description, country_id);
                          myDB.TeamAddTotalCreditHistory(table_id, tc_day, week, total_credit);
                          myDB.TeamAddRACHistory(table_id, rac_day, rac);
                      }
                      catch (java.sql.SQLException se) {
                          log.error(se);
                      }      
                  }
                  
                  // now update our users in the team with our table_id entry
//                  for(int i=0;i<users_in_team.size();i++) {
//                      
//                      try {
//                          Integer U = (Integer)users_in_team.elementAt(i);
//                          
//                          if (userHashTable.containsKey((Object)U.toString())) {
//                              // it's in there
//                              User uh = (User) userHashTable.get((Object)U.toString());
//                              myDB.UserUpdateTeam(uh.table_id, team_id, cteam_table_id);
//                              myDB.TeamAddUserHistEntry(project_id, team_id, cteam_table_id, U.intValue(), uh.table_id);
//                          } else {
//                              log.debug("team member id not found");
//                          }
//                      }
//                      catch (java.sql.SQLException se) {
//                    	  log.error(se);
//                      }
//                      	
//                  }
//                  users_in_team.clear();
                  
                  // clear out data
                  team_id=0;
                  type=0;
                  name="";
                  founder_id=0;
                  total_credit="0";
                  rac="0";
                  rac_time="0";
                  nusers=0;
                  founder_name="";
                  create_time="0";
                  url="";
                  description="0";
                  country="";
                  country_id=0;                 
              }
              else if (localName.equalsIgnoreCase("id")) 
                  team_id = Integer.parseInt(parseDataValue);
              else if (localName.equalsIgnoreCase("type")) 
                  type = Integer.parseInt(parseDataValue);
              else if (localName.equalsIgnoreCase("name"))
                  name = parseDataValue;
              else if (localName.equalsIgnoreCase("userid")) 
                  founder_id = Integer.parseInt(parseDataValue);
              else if (localName.equalsIgnoreCase("total_credit"))
                  total_credit = parseDataValue;
              else if (localName.equalsIgnoreCase("expavg_credit"))
                  expavg_credit = parseDataValue;
              else if (localName.equalsIgnoreCase("expavg_time"))
                  expavg_time = parseDataValue;
              //else if (localName.equalsIgnoreCase("nusers")) 
              //    nusers = Integer.parseInt(parseDataValue);
              else if (localName.equalsIgnoreCase("founder_name"))
                  founder_name = parseDataValue;
              else if (localName.equalsIgnoreCase("create_time"))
                  create_time = parseDataValue;
              else if (localName.equalsIgnoreCase("url"))
                  url = parseDataValue;
              else if (localName.equalsIgnoreCase("description"))
                  description = parseDataValue;
              else if (localName.equalsIgnoreCase("country")) {
                  country = parseDataValue;
                  // lookup in hash table
                  if (countryHashTable.containsKey((Object)country.toLowerCase())) {
                      Integer i = (Integer)countryHashTable.get((Object)country.toLowerCase());
                      country_id = i.intValue();
                  }
                  else {
                      log.warn("Country: [" + country + "] not in table");
                      if (bAddCrap) {
                          int i=0;
                          try {
                              myDB.AddCountry(i, country);
                              Integer cid = new Integer(i);
                              countryHashTable.put(country, cid);
                          } 
                          catch (java.sql.SQLException se) {
                              log.error("Failed to add country entry");
                          }
                      }
                  }
              }
              
          }          
          if (localName.equalsIgnoreCase("teams")) inTeams = false;
          if (localName.equalsIgnoreCase("user"))inUser = false;

      }
      parseDataValue="";
  }

  public void characters(char[] ch, int start, int length)
  throws SAXException {

    parseDataValue = parseDataValue + new String(ch,start,length);
     
  }
  
}