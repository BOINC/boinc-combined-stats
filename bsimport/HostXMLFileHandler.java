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


public class HostXMLFileHandler extends DefaultHandler {

  private boolean inHosts = false;
  private boolean inHost = false;
  private Hashtable userHashTable=null;
  private Hashtable hostHashTable=null;
  private Hashtable<String, Integer> pvendorHashTable=null;
  private Hashtable<String, Integer> pmodelHashTable=null;
  private Hashtable<String, Integer> osnameHashTable=null;
  private Hashtable cpidHashTable=null;
  private Hashtable cpidChangesHashTable=null;
  private Database myDB=null;
  private String parseDataValue=null;
  private int project_id;
  private long racComputeTime=0;
  static Logger log = Logger.getLogger(HostXMLFileHandler.class.getName());  // log4j stuff
  private boolean bAddCrap=false;
  
  private int host_id=0;
  private int user_id=0;
  private String p_vendor="";
  private int p_vendor_id=0;
  private String p_model="";
  private int p_model_id=0;
  private String os_name="";
  private int os_name_id=0;
  private String os_version="";
  private String create_time="0";
  private String rpc_time="0";
  private String timezone="";
  private int ncpus=0;
  private String p_fpops="0";
  private String p_iops="0";
  private String p_membw="0";
  private String m_nbytes="0";
  private String m_cache="0";
  private String m_swap="0";
  private String d_total="0";
  private String d_free="0";
  private String n_bwup="0";
  private String n_bwdown="0";
  private String avg_turnaround="0";
  private String host_cpid="";
  private String total_credit="0";
  private String expavg_credit="0";
  private String expavg_time="0";
  private String credit_per_cpu_sec="0";
 
  private int debug=0;
  
  public int added=0;
  public int updated=0;
  public int anonymous=0;
  public int update_skipped=0;
  
  public HostXMLFileHandler(Hashtable uht, Hashtable host, Hashtable<String, Integer> pvendor, Hashtable<String, Integer> pmodel, Hashtable<String, Integer> osname, Hashtable cpid, Hashtable cpidchanges, Database db, int pid, long rt) {
      userHashTable = uht;
      hostHashTable = host;
      pvendorHashTable = pvendor;
      pmodelHashTable = pmodel;
      osnameHashTable = osname;      
      cpidHashTable = cpid;
      cpidChangesHashTable = cpidchanges;
      myDB = db;
      project_id = pid;
      racComputeTime = rt;
  }
  
  public void startElement(String namespaceURI, String localName,
   String qualifiedName, Attributes atts) throws SAXException {
    
    if (localName.equalsIgnoreCase("host")) inHost = true;
    if (localName.equalsIgnoreCase("hosts")) inHosts = true;
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

      if (inHosts) {
          if (inHost) {
              if (localName.equalsIgnoreCase("host")) {
                  inHost = false;

                  //recompute the RAC
                  String rac;
                  rac = DecayRac(expavg_credit,expavg_time);
                  String rac_time = ""+racComputeTime;
                  

                  // get the primary key for our user entry
                  // by looking in the user hashtable
                  int cpid_table_id=0;
                  int b_users_id=0;
                  boolean bCpidChanged=false;
                  Integer u = new Integer(user_id);
                  String t = u.toString();
                  if (userHashTable.containsKey((Object)t)) {
                      // it's in there
                      User uh = (User) userHashTable.get((Object)t);
                      b_users_id = uh.table_id;  
                      bCpidChanged = uh.bCpidChange;
                      // check the global cpid hash
                      if (cpidChangesHashTable.containsKey(uh.cpid)) {
                          CPIDChange cc = (CPIDChange)cpidChangesHashTable.get(uh.cpid);
                          cpid_table_id = cc.table_id;
                          bCpidChanged=true;
                      } else if (cpidHashTable.containsKey(uh.cpid)) {
                          // in there, mark it as found
                          CPID cp = (CPID)cpidHashTable.get(uh.cpid);
                          cpid_table_id = cp.table_id;
                      } 
                      
                  } else anonymous++; 
                  
                  
                  // got the complete data for a single host entry
                  // now lookup in hashtable to see if they are in there
                  u = new Integer(host_id);
                  t = u.toString();
                  
                  if (hostHashTable.containsKey(t)) {
                      // update entry
                      updated++;
                      Host h = (Host) hostHashTable.get(t);
                      
                      // if rpc time is not in the last few months
                      // skip the update as nothing will have changed
                      Double d = new Double (rpc_time);
                      double drpc = d.doubleValue();
                      
                      if (drpc > (System.currentTimeMillis()/1000) - (3600*24*30*4) || bCpidChanged==true) {
                          try {
                              myDB.HostUpdateEntry(h.table_id, p_vendor_id, p_model_id, os_name_id, os_version, rpc_time, timezone, ncpus, p_fpops, p_iops, p_membw, m_nbytes, m_cache, m_swap, d_total, d_free, n_bwup, n_bwdown, avg_turnaround, host_cpid, total_credit, rac, rac_time,cpid_table_id,b_users_id,credit_per_cpu_sec);
                          }
                          catch (java.sql.SQLException se) {
                              log.error(se);
                          }
                      } else update_skipped++;
                      // now remove entry. Any remaining entries 
                      // after import runs are deleted from the table
                      hostHashTable.remove(t);
                      
                  } else {
                      // add entry
                      added++;
                      try {
                          //int table_id=0;
                          myDB.HostAddEntry(project_id, host_id, user_id, b_users_id, p_vendor_id, p_model_id, os_name_id, os_version, create_time, rpc_time, timezone, ncpus, p_fpops, p_iops, p_membw, m_nbytes, m_cache, m_swap, d_total, d_free, n_bwup, n_bwdown, avg_turnaround, host_cpid, total_credit, rac, rac_time,cpid_table_id, credit_per_cpu_sec);
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
                  host_id=0;
                  user_id=0;
                  p_vendor="";
                  p_vendor_id=0;
                  p_model="";
                  p_model_id=0;
                  os_name="";
                  os_name_id=0;
                  os_version="";
                  create_time="0";
                  rpc_time="0";
                  timezone="0";
                  ncpus=0;
                  p_fpops="0";
                  p_iops="0";
                  p_membw="0";
                  m_nbytes="0";
                  m_cache="0";
                  m_swap="0";
                  d_total="0";
                  d_free="0";
                  n_bwup="0";
                  n_bwdown="0";
                  avg_turnaround="0";
                  host_cpid="";
                  total_credit="0";
                  expavg_credit="0";
                  expavg_time="0";                  
                  credit_per_cpu_sec="0";
              }
              else if (localName.equalsIgnoreCase("id")) 
                  host_id = Integer.parseInt(parseDataValue);
              else if (localName.equalsIgnoreCase("userid")) 
                  user_id = Integer.parseInt(parseDataValue);
              else if (localName.equalsIgnoreCase("total_credit"))
                  total_credit = parseDataValue;
              else if (localName.equalsIgnoreCase("expavg_credit"))
                  expavg_credit = parseDataValue;
              else if (localName.equalsIgnoreCase("expavg_time"))
                  expavg_time = parseDataValue;
              else if (localName.equalsIgnoreCase("credit_per_cpu_sec"))
                  credit_per_cpu_sec = parseDataValue;
              else if (localName.equalsIgnoreCase("p_vendor")) {
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
                              myDB.AddPVendor(i, p_vendor);
                              Integer cid = new Integer(i);
                              pvendorHashTable.put(p_vendor, cid);
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
                              myDB.AddPModel(i, p_model);
                              Integer cid = new Integer(i);
                              pmodelHashTable.put(p_model, cid);
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
                              myDB.AddOSName(i, os_name);
                              Integer cid = new Integer(i);
                              osnameHashTable.put(os_name, cid);
                          } 
                          catch (java.sql.SQLException se) {
                              log.error("Failed to add os_name entry");
                          }
                      }
                  }
              }
              else if (localName.equalsIgnoreCase("os_version"))
                  os_version = parseDataValue;
              else if (localName.equalsIgnoreCase("create_time"))
                  create_time = parseDataValue;
              else if (localName.equalsIgnoreCase("rpc_time"))
                  rpc_time = parseDataValue;
              else if (localName.equalsIgnoreCase("timezone"))
                  timezone = parseDataValue;
              else if (localName.equalsIgnoreCase("ncpus")) {
                  ncpus = Integer.parseInt(parseDataValue);
                  if (ncpus > 500) ncpus = 0;
              }
              else if (localName.equalsIgnoreCase("p_fpops"))
                  p_fpops = parseDataValue;
              else if (localName.equalsIgnoreCase("p_iops"))
                  p_iops = parseDataValue;
              else if (localName.equalsIgnoreCase("p_membw"))
                  p_membw = parseDataValue;
              else if (localName.equalsIgnoreCase("m_nbytes"))
                  m_nbytes = parseDataValue;
              else if (localName.equalsIgnoreCase("m_cache"))
                  m_cache = parseDataValue;
              else if (localName.equalsIgnoreCase("m_swap"))
                  m_swap = parseDataValue;
              else if (localName.equalsIgnoreCase("d_total"))
                  d_total = parseDataValue;
              else if (localName.equalsIgnoreCase("d_free"))
                  d_free = parseDataValue;
              else if (localName.equalsIgnoreCase("n_bwup"))
                  n_bwup = parseDataValue;
              else if (localName.equalsIgnoreCase("n_bwdown"))
                  n_bwdown = parseDataValue;
              else if (localName.equalsIgnoreCase("avg_turnaround"))
                  avg_turnaround = parseDataValue;
              else if (localName.equalsIgnoreCase("host_cpid"))
                  host_cpid = parseDataValue;
          }
          if (localName.equalsIgnoreCase("hosts")) inHosts = false;

      }
      parseDataValue="";
  }

  public void characters(char[] ch, int start, int length)
  throws SAXException {

    parseDataValue = parseDataValue + new String(ch,start,length);
     
  }
  
}