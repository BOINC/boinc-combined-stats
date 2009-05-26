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
import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import org.apache.log4j.Logger;


public class UserXMLFileHandler extends DefaultHandler {

  private boolean inUsers = false;
  private boolean inUser = false;
  private Hashtable<String, User> userHashTable=null;
  private Hashtable<String, Country> countryHashTable=null;
  private Hashtable<String, CPID> hCpid=null;
  private Hashtable<String, Team> teamHashTable=null;
  private Database myDB=null;
  private String parseDataValue=null;
  private long racComputeTime=0;
  static Logger log = Logger.getLogger(UserXMLFileHandler.class.getName());  // log4j stuff
  private boolean bAddCrap=false;
  
  private int user_id=0;
  private String name="";
  private String country="0";
  private int country_id=0;
  private double create_time=0;
  private double total_credit=0;
  private double expavg_credit=0;
  private double expavg_time=0;
  private String user_cpid="";
  private String url="";
  private int teamid=0;
  private int b_cteam_id=0;
  //private String tablename=null;
  private int project_id=0;
  private boolean sawStartTag=false;
  private File outFile = null;
  private PrintWriter Out = null;
  int debug=0;
  // stats
  public int added = 0;
  public boolean fileOK=false;
    
  public UserXMLFileHandler(String table, int project_id, Hashtable<String, User> uht, Hashtable<String, Country> countryHash, Hashtable<String, Team> team, Hashtable<String, CPID > cpid, Database db, long rt,String dbFile) 
  throws IOException
  {
      userHashTable = uht;
      countryHashTable = countryHash;
      //cpidChangesHashTable = cpidchange;
      hCpid = cpid;
      teamHashTable = team;
      //tablename = table;
      myDB = db;
      racComputeTime = rt;
      this.project_id = project_id;       
      
      try {
    	  outFile = new File(dbFile);
    	  Out = new PrintWriter(new BufferedWriter(new FileWriter(outFile)));
      } catch (IOException ex){
    	  log.error("Error opening file " + dbFile,ex);
    	  throw new IOException("UserXMLFileHandler",ex);
      }
  }
  
  public void close() {
	  if (Out != null) {
		  Out.close();
	  }
	  Out = null;
	  outFile = null;
	  System.gc();
  }
  
  public void startElement(String namespaceURI, String localName,
   String qualifiedName, Attributes atts) throws SAXException {
    
    if (localName.equalsIgnoreCase("user")) inUser = true;
    if (localName.equalsIgnoreCase("users")) { inUsers = true; sawStartTag = true; }
    parseDataValue = "";
  }

  private double DecayRac(double current_rac, double rac_time) {
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
  
  public void endElement(String namespaceURI, String localName,
   String qualifiedName) throws SAXException {

      if (inUsers) {
          if (inUser) {
              if (localName.equalsIgnoreCase("user")) {
                  inUser = false;
                  // got the complete data for a single user entry

                  //recompute the RAC
                  double rac;
                  rac = DecayRac(expavg_credit,expavg_time);                  

                  
                  long b_cpid_id = 0;
                  // now lookup in user hash table to see if they are in there
                  Integer u = new Integer(user_id);
                  if (userHashTable.containsKey(u.toString())) {
                	  User uMyUser = (User)userHashTable.get(u.toString());
                	  if (uMyUser == null) {
                		  uMyUser = new User();
                		  uMyUser.b_cpid_id = b_cpid_id;
                		  uMyUser.cpid = user_cpid;
                		  uMyUser.user_id = user_id;                		  
                	  }
                	  
                	  // Check for an existing CPID
                	  CPID myCPID = hCpid.get(user_cpid);
                	  Double dTC=null,dR=null;
                	  if (myCPID == null) {
                		  // CPID Not on found, add to DB table
                		  try {
                			  dTC = new Double(total_credit);
                        	  dR = new Double(rac);
                			  b_cpid_id = myDB.CPIDAddEntry(user_cpid, dTC.longValue(), dR.longValue(), racComputeTime);
                		  }
                		  catch (java.sql.SQLException e) {
                			  log.error("SQL Error",e);
                		  }
                		  myCPID = new CPID();
                		  myCPID.b_cpid_id = b_cpid_id;
                		  myCPID.user_cpid = user_cpid;
                		  hCpid.put(user_cpid,myCPID);
                	  } else {
                		  b_cpid_id = myCPID.b_cpid_id;
                	  }
                	                  	  
                	  // Now do some sanity checks.
                	  // TODO: How to decide if we should merge?
                	  if (uMyUser.b_cpid_id != b_cpid_id) {
                		  // b_cpid_id changed!
                		  log.debug("b_cpid_id changed from "+uMyUser.b_cpid_id+" to "+b_cpid_id);
                	  }
                	  
                	  // compare the current cpid with previous cpid
                	  // log an entry if different
                	  if (uMyUser.cpid == null) uMyUser.cpid="";
                	  if (user_cpid.compareToIgnoreCase(uMyUser.cpid)!= 0) {
                		  // changed!
                		  try {
                			  myDB.AddToCPIDHistory(project_id, user_id, uMyUser.cpid, user_cpid);
                		  }
                		  catch (java.sql.SQLException ss) {
                			  log.error("SQL Error",ss);
                		  }
                	  }
                	  
                	  // Record team changes - perhaps we will do 
                	  // something with it.
                	  if (teamid != uMyUser.team_id) {
                		  try {
                			  myDB.AddToTeamHistory(project_id, user_id, uMyUser.team_id, teamid);
                		  }
                		  catch (java.sql.SQLException ss) {
                			  log.error("SQL Error",ss);
                		  }
                	  }
                	  
                  } else {
                	  // We need to lookup the cpid in our master table
                	  CPID c = hCpid.get(user_cpid);
                	  if (c != null) {
                		  b_cpid_id = c.b_cpid_id;
                	  } else {
                		  // not found, new entry
                		  Double dTC = new Double(total_credit);
                    	  Double dR = new Double(rac);
                    	  try {
                    		  b_cpid_id = myDB.CPIDAddEntry(user_cpid, dTC.longValue(), dR.longValue(), racComputeTime);
                    		  if (b_cpid_id != 0) {
                    			  // Add to the cpid hash table
                    			  c = new CPID();
                    			  c.b_cpid_id = b_cpid_id;
                    			  c.user_cpid = user_cpid;
                    			  hCpid.put(user_cpid, c);
                    		  }
                    	  } 
                    	  catch (java.sql.SQLException ss) {
                    		  log.error("SQL Error",ss);
                    	  }
                	  }
                	  // add to our user hash table
                	  User uNew = new User();                
                	  uNew.user_id = user_id;
                	  uNew.b_cpid_id = b_cpid_id;
                	  uNew.cpid = user_cpid;
                      String t = (String) new Long(user_id).toString();
                      userHashTable.put(t,uNew); 
                  }
                  
                  // add to the DB
                  added++;
                  try {    
                	  Double dTC = new Double(total_credit);
                	  Double dR = new Double(rac);
                	  Double dCT = new Double(create_time);
                      //myDB.UserAddEntry( tablename, user_id, name, country_id, dCT.longValue(), user_cpid, url, teamid, b_cteam_id, dTC.longValue(), dR.longValue(),racComputeTime,b_cpid_id);
                	  name = myDB.MakeSafe(name);
                	  user_cpid = myDB.MakeSafe(user_cpid);
                	  url = myDB.MakeSafe(url);
                	  Out.println(user_id+"\t"+b_cpid_id+"\t"+name+"\t"+country_id+"\t"+dCT.longValue()+"\t"+user_cpid+"\t"+url+"\t"+teamid+"\t"+b_cteam_id+"\t"+dTC.longValue()+"\t"+dR.longValue()+"\t"+racComputeTime+"\t0\t0\t0\t0\t0\t0");

                  }
                  catch (Exception e)
                  {
                      throw new SAXException("DB Failure To Add");
                  }
                  
                  // clear out data
                  user_id=0;
                  name="";
                  country="";
                  country_id=0;
                  create_time=0;
                  total_credit=0;
                  expavg_credit=0;
                  expavg_time=0;
                  user_cpid="";
                  url="";
                  teamid=0;
                  b_cteam_id=0;
              }
              else if (localName.equalsIgnoreCase("id")) 
                  user_id = Integer.parseInt(parseDataValue);
              else if (localName.equalsIgnoreCase("name"))
                  name = parseDataValue;
              else if (localName.equalsIgnoreCase("country")) {
                  country = parseDataValue;
                  // lookup in hash table
                  if (countryHashTable.containsKey((Object)country.toLowerCase())) {
                      Country c = countryHashTable.get((Object)country.toLowerCase());
                      if (c.remap_id == 0)
                    	  country_id = c.country_id;
                      else
                    	  country_id = c.remap_id;
                  }
                  else {
                      log.warn("Country: [" + country + "] not in table");
                      if (bAddCrap) {
                          int i=0;
                          try {
                              i = myDB.AddCountry(country);
                              Country nc = new Country();
                              nc.country = country.toLowerCase();
                              nc.country_id = i;
                              nc.remap_id = 0;
                              countryHashTable.put(country, nc);
                          } 
                          catch (java.sql.SQLException se) {
                              log.error("Failed to add country entry");
                          }
                      }
                  }
              }
              else if (localName.equalsIgnoreCase("create_time")) {
            	  create_time = 0;
            	  try {
            		  Double d = new Double(parseDataValue);
            		  create_time = d.doubleValue();
            	  }
            	  catch (Exception e) {
            		  log.error("Failed to parse create_time of "+parseDataValue);
            	  }                  
              } else if (localName.equalsIgnoreCase("total_credit")) {
            	  total_credit = 0;
            	  try {
            		  Double d = new Double(parseDataValue);
            		  total_credit = d.doubleValue();
            	  }
            	  catch (Exception e) {
            		  log.error("Failed to parse total_credit of "+parseDataValue);
            	  }                   
              }
              else if (localName.equalsIgnoreCase("expavg_credit")) {
            	  expavg_credit = 0;
            	  try {
            		  Double d = new Double(parseDataValue);
            		  expavg_credit = d.doubleValue();
            	  }
            	  catch (Exception e) {
            		  log.error("Failed to parse expavg_credit of "+parseDataValue);
            	  }            	  
              }
              else if (localName.equalsIgnoreCase("expavg_time")) {
            	  expavg_time = 0;
            	  try {
            		  Double d = new Double(parseDataValue);
            		  expavg_time = d.doubleValue();
            	  }
            	  catch (Exception e) {
            		  log.error("Failed to parse expavg_time of "+parseDataValue);
            	  } 
              } else if (localName.equalsIgnoreCase("cpid"))
                  user_cpid = parseDataValue;
              else if (localName.equalsIgnoreCase("url")) {
                  url = parseDataValue;
                  if (url.length() > 200 )
                	  url = url.substring(0,199);
              } else if (localName.equalsIgnoreCase("teamid")) {
                  teamid = Integer.parseInt(parseDataValue);
                  // lookup team id
                  if (teamid > 0) {
                      if (teamHashTable.containsKey(new Integer(teamid).toString())) {
                          Team th = (Team)teamHashTable.get(new Integer(teamid).toString());     
                          b_cteam_id = th.b_cteam_id;
                      }                     
                  }
              }
          }
          if (localName.equalsIgnoreCase("users")) { 
        	  inUsers = false; 
        	  if (sawStartTag == true) {
        		  fileOK = true; 
        		  close();
        	  }
          }

      }
      
      parseDataValue="";
  }

  public void characters(char[] ch, int start, int length)
  throws SAXException {

    parseDataValue = parseDataValue +  new String(ch,start,length);
  
  }
  
}