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
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.*;

import org.apache.log4j.Logger;


public class TeamXMLFileHandler extends DefaultHandler {

  private boolean inTeams = false;
  private boolean inTeam = false;
  private boolean inUser = false;
  private Hashtable<String, Team> teamHashTable=null;
  private Hashtable<String, Country> countryHashTable=null;
  private Hashtable<String, CTeam> cteamHashTable=null;
  private Database myDB=null;
  private String parseDataValue=null;
  private long racComputeTime=0;
  static Logger log = Logger.getLogger(TeamXMLFileHandler.class.getName());  // log4j stuff
  private boolean bAddCrap=false;
  
  private int team_id=0;
  private int type=0;
  private String name="";
  private String teamCPID = "";
  private int founder_id=0;
  private double total_credit=0;
  private double expavg_credit=0;
  private double expavg_time=0;
  private int nusers=0;
  private String founder_name="";
  private long create_time=0;
  private String url="";
  private String description="";
  private String country="";
  private int country_id=0;
  //private String tablename=null;
  private boolean sawStartTag=false;
  private File outFile = null;
  private PrintWriter Out = null;
  public int added=0;
  public boolean fileOK=false;
  
  public TeamXMLFileHandler(String table, Hashtable<String, Team> tht, Hashtable<String, Country> countryHash, Hashtable<String, CTeam> cteamHash, Database db, long rt,String dbFile) 
  throws IOException 
  {
      countryHashTable = countryHash;
      teamHashTable = tht;
      //tablename=table;
      myDB = db;
      racComputeTime = rt;
      cteamHashTable = cteamHash;
      
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
    
    if (localName.equalsIgnoreCase("team")) inTeam = true;
    if (localName.equalsIgnoreCase("teams")) { inTeams = true; sawStartTag = true; }
    if (localName.equalsIgnoreCase("user")) inUser = true;
    parseDataValue="";
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

      if (inTeams) {
          if (inUser) {
              nusers++;
          } else if (inTeam) {
              if (localName.equalsIgnoreCase("team")) {
                  inTeam = false;

                  //recompute the RAC
                  double rac;
                  rac = DecayRac(expavg_credit,expavg_time);
                  
                  // got the complete data for a single team entry
                  
                  // Get the CTeam ID from the global hash
                  // If not found, add to CTeam Hash     
                  CTeam ct = cteamHashTable.get(teamCPID);
                  if (ct == null) {
                	  // Currently doesn't exist
                	  ct = new CTeam();
                	  ct.name = name;
                	  ct.team_cpid = teamCPID;      
                	  try {
                		  ct.table_id = myDB.CTeamAddEntry(name, teamCPID);
                	  }
                	  catch (java.sql.SQLException s) {
                		  log.error("SQL Error",s);
                		  ct.table_id=0;
                	  }
                	  // add to global hashtable
                	  cteamHashTable.put(ct.team_cpid, ct);
                  } 
                  
                  Integer iTemp = new Integer(team_id);
                  Team ht = teamHashTable.get(iTemp.toString());
                  if (ht == null) {
                	  ht = new Team();    
                	  ht.b_cteam_id = ct.table_id;
                	  ht.team_id = team_id;
                	  teamHashTable.put(iTemp.toString(),ht);
                  } else {
                	  ht.b_cteam_id = ct.table_id;
                  }

                  // add entry
                  added++;
                  try {
                	  Double dTC = new Double(total_credit);
                	  Double dR = new Double(rac);
                      //myDB.TeamAddEntry(tablename,team_id, ct.table_id, type, name, teamCPID, founder_id, dTC.longValue(), dR.longValue(), racComputeTime, nusers, founder_name, create_time, url, description, country_id);
                	  name = myDB.MakeSafe(name);
                	  founder_name = myDB.MakeSafe(founder_name);
                	  url = myDB.MakeSafe(url);
                	  description = myDB.MakeSafe(description);
                	  //if (country_id == -1) country_id = 0;
                	  //Out.println("\""+team_id+"\",\""+ct.table_id+"\",\""+type+"\",\""+name+"\",\""+teamCPID+"\",\""+founder_id+"\",\""+dTC.longValue()+"\",\""+dR.longValue()+"\",\""+racComputeTime+"\",\""+nusers+"\",\""+founder_name+"\",\""+create_time+"\",\""+url+"\",\""+description+"\",\""+country_id+"\",\"0\",\"0\"");
                	  Out.println(""+team_id+"\t"+ct.table_id+"\t"+type+"\t"+name+"\t"+teamCPID+"\t"+founder_id+"\t"+dTC.longValue()+"\t"+dR.longValue()+"\t"+racComputeTime+"\t"+nusers+"\t"+founder_name+"\t"+create_time+"\t"+url+"\t"+description+"\t"+country_id+"\t0\t0");
                  }
                  catch (Exception se) {
                      log.error("Exception",se);
                  }      
                  
                  // clear out data
                  team_id=0;
                  type=0;
                  name="";
                  teamCPID="";
                  founder_id=0;
                  total_credit=0;
                  rac=0;
                  nusers=0;
                  founder_name="";
                  create_time=0;
                  url="";
                  description="";
                  country="";
                  country_id=0;                 
              }
              else if (localName.equalsIgnoreCase("id")) 
                  team_id = Integer.parseInt(parseDataValue);
              else if (localName.equalsIgnoreCase("type")) 
                  type = Integer.parseInt(parseDataValue);
              else if (localName.equalsIgnoreCase("name")) {
                  name = parseDataValue;
                  // Calculate team CPID
                  // Team CPID = md5(lower(name));  
                  try {
	                  MessageDigest m = MessageDigest.getInstance("MD5");
	                  m.update(parseDataValue.toLowerCase().getBytes(),0,parseDataValue.length());
	                  BigInteger bi = new BigInteger(1,m.digest());
	                  teamCPID = bi.toString(16); 
                  }
                  catch (Exception e) {
                	  log.error("Error Creating MD5 of team name: "+parseDataValue,e);
                  }
              } else if (localName.equalsIgnoreCase("userid")) 
                  founder_id = Integer.parseInt(parseDataValue);
              else if (localName.equalsIgnoreCase("total_credit")) {
            	  total_credit = 0;
            	  try {
            		  Double d = new Double(parseDataValue);
            		  total_credit = d.doubleValue();
            	  }
                  catch (Exception e) {
                	  log.error("Can't parse total_credit string ("+parseDataValue);
                  }                  
              } else if (localName.equalsIgnoreCase("expavg_credit")){
            	  expavg_credit = 0;
            	  try {
            		  Double d = new Double(parseDataValue);
            		  expavg_credit = d.doubleValue();
            	  }
                  catch (Exception e) {
                	  log.error("Can't parse expavg_credit string ("+parseDataValue);
                  }
              } else if (localName.equalsIgnoreCase("expavg_time")) {
            	  expavg_time = 0;
            	  try {
            		  Double d = new Double(parseDataValue);
            		  expavg_time = d.doubleValue();
            	  }
                  catch (Exception e) {
                	  log.error("Can't parse expavg_time string: "+parseDataValue);
                  }
              } else if (localName.equalsIgnoreCase("founder_name")) {
                  founder_name = parseDataValue;
                  if (founder_name.length() > 100) {
                	  founder_name = founder_name.substring(0,99);
                  }
              } else if (localName.equalsIgnoreCase("create_time")) {
                  create_time = 0;
            	  try {
            		  Long l = new Long(parseDataValue);
            		  create_time = l.longValue();
            	  }
            	  catch (Exception e) {
            		  log.error("Can't parse create_time: "+parseDataValue);
            	  }
          	  } else if (localName.equalsIgnoreCase("url")) {
                  url = parseDataValue;
                  if (url.length() > 250) {
                	  log.warn("Warning Team URL Too long: "+url.length()+" : "+url);
                	  url = url.substring(0,250);
                  }
          	  }
              else if (localName.equalsIgnoreCase("description"))
                  description = parseDataValue;
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
                              nc.country_id = i;
                              nc.country = country.toLowerCase();
                              nc.remap_id = 0;
                              
                              countryHashTable.put(country, nc);
                          } 
                          catch (java.sql.SQLException se) {
                              log.error("Failed to add country entry");
                          }
                      }
                  }
              }
              
          }          
          if (localName.equalsIgnoreCase("teams")) {
        	  inTeams = false;
        	  if (sawStartTag == true) {
        		  fileOK = true;
        		  close();
        	  }
          }
          if (localName.equalsIgnoreCase("user"))inUser = false;

      }
      parseDataValue="";
  }

  public void characters(char[] ch, int start, int length)
  throws SAXException {

    parseDataValue = parseDataValue + new String(ch,start,length);
     
  }
  
}