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


public class HostXMLFileHandler extends DefaultHandler {

  private boolean inHosts = false;
  private boolean inHost = false;
  private Hashtable<String, User> userHashTable=null;
  private Hashtable<String, Integer> pvendorHashTable=null;
  private Hashtable<String, Integer> pmodelHashTable=null;
  private Hashtable<String, Integer> osnameHashTable=null;

  private Database myDB=null;
  private String parseDataValue=null;
  private long racComputeTime=0;
  static Logger log = Logger.getLogger(HostXMLFileHandler.class.getName());  // log4j stuff
  private boolean bAddCrap=true;
  //private String tableToInsertInto=null;

  
  private int host_id=0;
  private int user_id=0;
  private String p_vendor="";
  private int p_vendor_id=0;
  private String p_model="";
  private int p_model_id=0;
  private String os_name="";
  private int os_name_id=0;
  private String os_version="";
  private long create_time=0;
  private long rpc_time=0;
  private int timezone=0;
  private int ncpus=0;
  private double p_fpops=0;
  private double p_iops=0;
  private double p_membw=0;
  private double m_nbytes=0;
  private double m_cache=0;
  private double m_swap=0;
  private double d_total=0;
  private double d_free=0;
  private double n_bwup=0;
  private double n_bwdown=0;
  private double avg_turnaround=0;
  private String host_cpid="";
  private double total_credit=0;
  private double expavg_credit=0;
  private double expavg_time=0;
  private double credit_per_cpu_sec=0;
  private boolean sawStartTag = false;
  private File outFile = null;
  private PrintWriter Out = null;
	
  public int added=0;
  public int anonymous=0;
  public int update_skipped=0;
  public boolean fileOK=false;
  
  public HostXMLFileHandler(Hashtable<String, User> uht, Hashtable<String, Integer> pvendor, Hashtable<String, Integer> pmodel, Hashtable<String, Integer> osname,  Database db, long rt, String tablename, boolean addMissingTypes, String dbFile) 
  throws IOException
  {
      userHashTable = uht;
      pvendorHashTable = pvendor;
      pmodelHashTable = pmodel;
      osnameHashTable = osname;      
      bAddCrap = addMissingTypes;
      myDB = db;
      racComputeTime = rt;
      //tableToInsertInto = tablename;
      
      
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
    
    if (localName.equalsIgnoreCase("host")) inHost = true;
    if (localName.equalsIgnoreCase("hosts")) { inHosts = true; sawStartTag = true; }
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

      if (inHosts) {
          if (inHost) {
              if (localName.equalsIgnoreCase("host")) {
                  inHost = false;

                  //recompute the RAC
                  double rac;
                  rac = DecayRac(expavg_credit,expavg_time);
                  

                  // get the primary key for our user entry
                  // by looking in the user hashtable
                  long b_cpid_id=0;
                  if (user_id > 0) {
	                  Integer u = new Integer(user_id);
	                  String t = u.toString();
	                  if (userHashTable.containsKey((Object)t)) {
	                      // it's in there
	                      User uh = (User) userHashTable.get((Object)t);
	                      b_cpid_id = uh.b_cpid_id;
	                  }	                      
                  } else anonymous++; 
                                   
                  // add entry
                  added++;
                  if (added == 1994742)
                	  log.debug("debug");
                  //System.out.println(host_id);
                  try {
                	  Double dTC = new Double(total_credit);
                	  Double dR = new Double(rac);
                      //myDB.HostAddEntry(tableToInsertInto, host_id, user_id, p_vendor_id, p_model_id, os_name_id, os_version, create_time, rpc_time, timezone, ncpus, p_fpops, p_iops, p_membw, m_nbytes, m_cache, m_swap, d_total, d_free, n_bwup, n_bwdown, avg_turnaround, host_cpid, dTC.longValue(), dR.longValue(), racComputeTime,b_cpid_id, credit_per_cpu_sec);
                	  os_version = myDB.MakeSafe(os_version);
                	  host_cpid = myDB.MakeSafe(host_cpid);
                	  Out.println(host_id+"\t"+user_id+"\t"+b_cpid_id+"\t"+p_vendor_id+"\t"+p_model_id+"\t"+os_name_id+"\t"+os_version+"\t"+create_time+"\t"+rpc_time+"\t"+timezone+"\t"+ncpus+"\t"+p_fpops+"\t"+p_iops+"\t"+p_membw+"\t"+m_nbytes+"\t"+m_cache+"\t"+m_swap+"\t"+d_total+"\t"+d_free+"\t"+n_bwup+"\t"+n_bwdown+"\t"+avg_turnaround+"\t"+host_cpid+"\t"+dTC.longValue()+"\t"+dR.longValue()+"\t"+racComputeTime+"\t"+credit_per_cpu_sec+"\t0\t0");
                  }
                                         
                  catch (Exception e) {
                	  log.error("Exception",e);
                  }

                  // clear out data
                  host_id=0;
                  user_id=0;
                  p_vendor="";
                  p_vendor_id=0;
                  p_model="";
                  p_model_id=0;
                  os_name="";
                  os_name_id=0;
                  os_version="";
                  create_time=0;
                  rpc_time=0;
                  timezone=0;
                  ncpus=0;
                  p_fpops=0;
                  p_iops=0;
                  p_membw=0;
                  m_nbytes=0;
                  m_cache=0;
                  m_swap=0;
                  d_total=0;
                  d_free=0;
                  n_bwup=0;
                  n_bwdown=0;
                  avg_turnaround=0;
                  host_cpid="";
                  total_credit=0;
                  expavg_credit=0;
                  expavg_time=0;                  
                  credit_per_cpu_sec=0;
              }
              else if (localName.equalsIgnoreCase("id")) 
                  host_id = Integer.parseInt(parseDataValue);
              else if (localName.equalsIgnoreCase("userid")) 
                  user_id = Integer.parseInt(parseDataValue);
              else if (localName.equalsIgnoreCase("total_credit")) {
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
              } else if (localName.equalsIgnoreCase("credit_per_cpu_sec")) {
                  credit_per_cpu_sec = 0;
            	  try {
            		  Double d = new Double(parseDataValue);
            		  credit_per_cpu_sec = d.doubleValue();
            	  }
            	  catch (Exception e) {
            		  log.error("Failed to parse credit_per_cpu_sec of "+parseDataValue);
            	  }
              } else if (localName.equalsIgnoreCase("p_vendor")) {
                  p_vendor = parseDataValue.trim();
                  // lookup in hash table
                  if (pvendorHashTable.containsKey((Object)p_vendor)) {
                      Integer i = (Integer)pvendorHashTable.get((Object)p_vendor);
                      p_vendor_id = i.intValue();
                  }
                  else {
                      log.debug("p_vendor: [" + p_vendor + "] not in table");
                      if (bAddCrap) {
                          int i=0;
                          try {
                              i= myDB.AddPVendor(p_vendor);
                              Integer cid = new Integer(i);
                              pvendorHashTable.put(p_vendor, cid);
                              p_vendor_id = i;
                          } 
                          catch (java.sql.SQLException se) {
                              log.error("Failed to add p_vendor entry");
                          }
                      }
                  }
              }
              else if (localName.equalsIgnoreCase("p_model")) {
                  p_model = parseDataValue.trim();
                  // lookup in hash table
                  if (pmodelHashTable.containsKey((Object)p_model)) {
                      Integer i = (Integer)pmodelHashTable.get((Object)p_model);
                      p_model_id = i.intValue();
                  }
                  else {
                      log.debug("p_model: [" + p_model + "] not in table");
                      if (bAddCrap) {
                          int i=0;
                          try {
                              i = myDB.AddPModel(p_model);
                              Integer cid = new Integer(i);
                              pmodelHashTable.put(p_model, cid);
                              p_model_id = i;
                          } 
                          catch (java.sql.SQLException se) {
                              log.error("Failed to add p_model entry");
                          }
                      }
                  }
              }
              else if (localName.equalsIgnoreCase("os_name")) {
                  os_name = parseDataValue.trim();
                  // lookup in hash table
                  if (osnameHashTable.containsKey((Object)os_name)) {
                      Integer i = (Integer)osnameHashTable.get((Object)os_name);
                      os_name_id = i.intValue();
                  }
                  else {
                      log.debug("os_name: [" + os_name + "] not in table");
                      int i=0;
                      if (bAddCrap){
                          try {
                              i= myDB.AddOSName(os_name);
                              Integer cid = new Integer(i);
                              osnameHashTable.put(os_name, cid);
                              os_name_id=i;
                          } 
                          catch (java.sql.SQLException se) {
                              log.error("Failed to add os_name entry");
                          }
                      }
                  }
              }
              else if (localName.equalsIgnoreCase("os_version")) {
            	  if (parseDataValue.length() < 100) {
            		  os_version = parseDataValue;
            	  } else {
            		  log.warn("OS Version String truncated: "+parseDataValue);
            		  os_version = parseDataValue.substring(0,99);
            	  }
              } else if (localName.equalsIgnoreCase("create_time")) {
                  create_time = 0;
            	  try {
            		  Long l = new Long(parseDataValue);
            		  create_time = l.longValue();
            	  }
            	  catch (Exception e) {
            		  log.error("Failed to parse create_time of "+parseDataValue);
            	  }
              } else if (localName.equalsIgnoreCase("rpc_time")) {
                  rpc_time = 0;
            	  try {
            		  Long l = new Long(parseDataValue);
            		  rpc_time = l.longValue();
            	  }
            	  catch (Exception e) {
            		  log.error("Failed to parse rpc_time of "+parseDataValue);
            	  }
              } else if (localName.equalsIgnoreCase("timezone")) {
                  timezone = 0;
                  try {
            		  Integer i = new Integer(parseDataValue);
            		  timezone = i.intValue();
            		  if (timezone > 9999999 || timezone < -9999999) {
            			  log.warn("Bad timezone entry: "+timezone+" on host_id: "+ host_id);
            			  timezone = 0;
            		  }
            	  }
            	  catch (Exception e) {
            		  log.error("Failed to parse timezone of "+parseDataValue);
            	  }
              }
              else if (localName.equalsIgnoreCase("ncpus")) {
                  ncpus = Integer.parseInt(parseDataValue);
                  if (ncpus > 500) ncpus = 0;
              }
              else if (localName.equalsIgnoreCase("p_fpops")) {
                  p_fpops = 0;
            	  try {
            		  Double d = new Double(parseDataValue);
            		  p_fpops = d.doubleValue();
            	  }
            	  catch (Exception e) {
            		  log.error("Failed to parse p_fpops of "+parseDataValue);
            	  }
              } else if (localName.equalsIgnoreCase("p_iops")) {
                  p_iops = 0;
            	  try {
            		  Double d = new Double(parseDataValue);
            		  p_iops = d.doubleValue();
            	  }
            	  catch (Exception e) {
            		  log.error("Failed to parse p_iops of "+parseDataValue);
            	  }
              } else if (localName.equalsIgnoreCase("p_membw")) {
                  p_membw = 0;
            	  try {
            		  Double d = new Double(parseDataValue);
            		  p_membw = d.doubleValue();
            	  }
            	  catch (Exception e) {
            		  log.error("Failed to parse p_membw of "+parseDataValue);
            	  }
              } else if (localName.equalsIgnoreCase("m_nbytes")) {
                  m_nbytes = 0;
            	  try {
            		  Double d = new Double(parseDataValue);
            		  m_nbytes = d.doubleValue();
            	  }
            	  catch (Exception e) {
            		  log.error("Failed to parse m_nbytes of "+parseDataValue);
            	  }
              } else if (localName.equalsIgnoreCase("m_cache")) {
                  m_cache = 0;
            	  try {
            		  Double d = new Double(parseDataValue);
            		  m_cache = d.doubleValue();
            	  }
            	  catch (Exception e) {
            		  log.error("Failed to parse m_cache of "+parseDataValue);
            	  }
              } else if (localName.equalsIgnoreCase("m_swap")) {
                  m_swap = 0;
            	  try {
            		  Double d = new Double(parseDataValue);
            		  m_swap = d.doubleValue();
            	  }
            	  catch (Exception e) {
            		  log.error("Failed to parse m_swap of "+parseDataValue);
            	  }
              } else if (localName.equalsIgnoreCase("d_total")) {
                  d_total = 0;
            	  try {
            		  Double d = new Double(parseDataValue);
            		  d_total = d.doubleValue();
            	  }
            	  catch (Exception e) {
            		  log.error("Failed to parse d_total of "+parseDataValue);
            	  }
              } else if (localName.equalsIgnoreCase("d_free")) {
                  d_free = 0;
            	  try {
            		  Double d = new Double(parseDataValue);
            		  d_free = d.doubleValue();
            	  }
            	  catch (Exception e) {
            		  log.error("Failed to parse d_free of "+parseDataValue);
            	  }
              } else if (localName.equalsIgnoreCase("n_bwup")) {
                  n_bwup = 0;
            	  try {
            		  Double d = new Double(parseDataValue);
            		  n_bwup = d.doubleValue();
            	  }
            	  catch (Exception e) {
            		  log.error("Failed to parse n_bwup of "+parseDataValue);
            	  }
              } else if (localName.equalsIgnoreCase("n_bwdown")) {
                  n_bwdown = 0;
            	  try {
            		  Double d = new Double(parseDataValue);
            		  n_bwdown = d.doubleValue();
            	  }
            	  catch (Exception e) {
            		  log.error("Failed to parse n_bwdown of "+parseDataValue);
            	  }
              } else if (localName.equalsIgnoreCase("avg_turnaround")) {
                  avg_turnaround = 0;
            	  try {
            		  Double d = new Double(parseDataValue);
            		  avg_turnaround = d.doubleValue();
            	  }
            	  catch (Exception e) {
            		  log.error("Failed to parse avg_turnaround of "+parseDataValue);
            	  }
              } else if (localName.equalsIgnoreCase("host_cpid")) {
            	  if (parseDataValue.length() <= 32) {
            		  host_cpid = parseDataValue;
            	  } else {
            		  host_cpid = parseDataValue.substring(0,32);
            		  log.error("host_cpid is too long: "+parseDataValue);
            	  }
              }
          }
          if (localName.equalsIgnoreCase("hosts")) {
        	  inHosts = false;
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

	try {
		if (ch != null && length > 0 && (start+length) <= ch.length)
			parseDataValue = parseDataValue + new String(ch,start,length);
	}
	catch (Exception e) {
		log.error("Parse start:"+start+" length:"+length+" other:"+ch.length,e);
	}
  }
  
}