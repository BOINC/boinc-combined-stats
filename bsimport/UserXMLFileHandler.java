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


public class UserXMLFileHandler extends DefaultHandler {

  private boolean inUsers = false;
  private boolean inUser = false;
  private Hashtable<String, User> userHashTable=null;
  private Hashtable<String, Integer> countryHashTable=null;
  private Hashtable<String, CPID> cpidHashTable=null;
  private Hashtable<String, CPIDChange> cpidChangesHashTable=null;
  private Hashtable teamHashTable=null;
  private Database myDB=null;
  private String parseDataValue=null;
  private int project_id;
  private long racComputeTime=0;
  static Logger log = Logger.getLogger(UserXMLFileHandler.class.getName());  // log4j stuff
  private boolean bAddCrap=false;
  private int tc_day=0;
  private int rac_day=0;
  private int week=0;
  //private int mday = 0;
  
  private int user_id=0;
  private String name="";
  private String country="0";
  private int country_id=0;
  private String create_time="0";
  private String total_credit="0";
  private String expavg_credit="0";
  private String expavg_time="0";
  private String user_cpid="";
  private String url="";
  private int teamid=0;
  private int b_team_id=0;
  private int b_cteam_id=0;
  //private boolean has_profile;
  
  int debug=0;
  // stats
  public int updated=0;
  public int added = 0;
  public int cpid_changes = 0;
    
  public UserXMLFileHandler(Hashtable<String, User> uht, Hashtable<String, Integer> cht, Hashtable<String, CPID> cpid, Hashtable<String, CPIDChange> cpidchange, Hashtable team, Database db, int pid, long rt, int tcday, int racday) {
      userHashTable = uht;
      countryHashTable = cht;
      cpidHashTable = cpid;
      cpidChangesHashTable = cpidchange;
      teamHashTable = team;
      
      myDB = db;
      project_id = pid;
      racComputeTime = rt;
      tc_day = tcday;
      rac_day = racday;
      
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
    
    if (localName.equalsIgnoreCase("user")) inUser = true;
    if (localName.equalsIgnoreCase("users")) inUsers = true;
    parseDataValue = "";
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

      if (inUsers) {
          if (inUser) {
              if (localName.equalsIgnoreCase("user")) {
                  inUser = false;
                  // got the complete data for a single user entry
                  // now lookup in hashtable to see if they are in there
                  Integer u = new Integer(user_id);

                  //recompute the RAC
                  String rac;
                  rac = DecayRac(expavg_credit,expavg_time);
                  String rt = ""+racComputeTime;
                  boolean bCPIDFoundInCache=false;
                  
                 
                  int cpid_table_id=0;
                  // check the global cpid hash
                  if (cpidHashTable.containsKey(user_cpid)) {
                      // in there, mark it as found
                      bCPIDFoundInCache=true;
                      CPID cp = (CPID)cpidHashTable.get(user_cpid);
                      cp.bFound = true;
                      cpid_table_id = cp.table_id;
                  } else {
                      // see if it is on our cpid change list
                      if (cpidChangesHashTable.containsKey(user_cpid))
                      {
                          // it is
                          CPIDChange cc= (CPIDChange)cpidChangesHashTable.get(user_cpid);
                          cpid_table_id = cc.table_id; 
                          bCPIDFoundInCache=true;
                      } else {
                          
                          // we need to add it to our tables
                          try {                          
                              cpid_table_id = myDB.CPIDAddEntry(user_cpid, total_credit, rac, rt);
                              myDB.CPIDAddTotalCreditHistory(cpid_table_id, tc_day, week, total_credit);
                              myDB.CPIDAddRACHistory(cpid_table_id, rac_day, rac);
                          }
                          catch (java.sql.SQLException see) {
                              log.error("Failed to add new cpid "+user_cpid+" to global table");
                          }
                          
                          CPID cp = new CPID();
                          cp.bFound = true;
                          cp.table_id = cpid_table_id;
                          cp.user_cpid=user_cpid;
                          if (cpid_table_id > 0)
                              cpidHashTable.put(user_cpid, cp);
                      }
                  }
                  
                  int table_id=0;                  
                  String t = u.toString();
                  if (userHashTable.containsKey((Object)t)) {
                      // it's in there
                      updated++;
                      User uh = (User) userHashTable.get((Object)t);
                      table_id = uh.table_id;
                      // did the user_cpid change? If so add to the change table
                      if (uh.cpid.compareToIgnoreCase(user_cpid) != 0) {
                          cpid_changes++;
                          uh.bCpidChange = true;
                          // had a change. But let's see if we already have
                          // this cpid on another project - if so, we don't
                          // do need to change the cpid table
                          if (bCPIDFoundInCache==false) {

                              CPIDChange cc = new CPIDChange();
                              cc.table_id = cpid_table_id;
                              cc.old_cpid = uh.cpid;
                              cc.new_cpid = user_cpid;
                              cpidChangesHashTable.put(user_cpid, cc);
                          }
                          try {
                              myDB.AddToCPIDHistory(project_id,user_id,uh.cpid, user_cpid);
                              if (bCPIDFoundInCache==false) myDB.CPIDChangeCPID(cpid_table_id, user_cpid);
                          }
                          catch (java.sql.SQLException se) {
                              log.error(se);
                          }
                          
                          uh.cpid = user_cpid;
                      }
                      
                      // store in to the DB
                      try {
                          
                          int rows;
                          rows = myDB.UserUpdateEntry(uh.table_id, name, country_id, user_cpid, url, teamid, b_team_id, b_cteam_id, total_credit, rac,rt,cpid_table_id);
                          if (rows == 1) {
                              rows = myDB.UserUpdateTotalCreditHistory(uh.table_id, tc_day, week, total_credit);
                              if (rows == 0) myDB.UserAddTotalCreditHistory(uh.table_id, tc_day, week, total_credit);
                              rows = myDB.UserUpdateRACHistory(uh.table_id, rac_day, rac);
                              if (rows == 0) myDB.UserAddRACHistory(uh.table_id, rac_day, rac);
                          }
                      }
                      catch (java.sql.SQLException e)
                      {
                          throw new SAXException("DB Failure To Update");
                      }
                      
                  } else {
                      // add to the DB
                      
                      added++;
                      try {                          
                          table_id=myDB.UserAddEntry( project_id, user_id, name, country_id, create_time, user_cpid, url, teamid, b_team_id, b_cteam_id, total_credit, rac,rt,cpid_table_id);                          
                          myDB.UserAddTotalCreditHistory(table_id, tc_day, week, total_credit);
                          myDB.UserAddRACHistory(table_id, rac_day, rac);
                      }
                      catch (java.sql.SQLException e)
                      {
                          throw new SAXException("DB Failure To Add");
                      }
                      
                      // add to the hashtable
                      User uh = new User();
                      uh.table_id = table_id;
                      uh.bCpidChange = false;
                      uh.cpid = user_cpid;
                      uh.user_id = user_id;
                      
                      userHashTable.put(new Integer(uh.user_id).toString(),uh);
                      
                  }
                  
                  if (teamid > 0) {
                      // add to the team history table
                      try {
                          myDB.TeamAddUserHistEntry(project_id, teamid, b_team_id, user_id, table_id);
                      }
                      catch (java.sql.SQLException se) {
                          log.error(se);                          
                      }
                  }
              
                  debug++;
                  if (debug > 1000) {
                      //log.debug(""+System.currentTimeMillis());
                      debug = 0;
                  }
                  // clear out data
                  user_id=0;
                  name="";
                  country="";
                  country_id=0;
                  create_time="0";
                  total_credit="0";
                  expavg_credit="0";
                  expavg_time="0";
                  user_cpid="";
                  url="";
                  teamid=0;
                  b_team_id=0;
                  b_cteam_id=0;
                  //has_profile=false;
              }
              else if (localName.equalsIgnoreCase("id")) 
                  user_id = Integer.parseInt(parseDataValue);
              else if (localName.equalsIgnoreCase("name"))
                  name = parseDataValue;
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
              else if (localName.equalsIgnoreCase("create_time"))
                  create_time = parseDataValue;
              else if (localName.equalsIgnoreCase("total_credit"))
                  total_credit = parseDataValue;
              else if (localName.equalsIgnoreCase("expavg_credit"))
                  expavg_credit = parseDataValue;
              else if (localName.equalsIgnoreCase("expavg_time"))
                  expavg_time = parseDataValue;
              else if (localName.equalsIgnoreCase("cpid"))
                  user_cpid = parseDataValue;
              else if (localName.equalsIgnoreCase("url"))
                  url = parseDataValue;
              else if (localName.equalsIgnoreCase("teamid")) {
                  teamid = Integer.parseInt(parseDataValue);
                  // lookup team id
                  if (teamid > 0) {
                      if (teamHashTable.containsKey(new Integer(teamid).toString())) {
                          Team th = (Team)teamHashTable.get(new Integer(teamid).toString());
                          b_team_id = th.table_id;     
                          b_cteam_id = th.b_cteam_id;
                      }                     
                  }
              }
              //else if (localName.equalsIgnoreCase("has_profile"))
              //    has_profile=true;
          }
          if (localName.equalsIgnoreCase("users")) inUsers = false;

      }
      
      parseDataValue="";
  }

  public void characters(char[] ch, int start, int length)
  throws SAXException {

    parseDataValue = parseDataValue +  new String(ch,start,length);
    
    //if (inUser) {
    //  for (int i = start; i < start+length; i++) {
    //    System.out.print(ch[i]); 
    //  }
    //}   
  }
  
}