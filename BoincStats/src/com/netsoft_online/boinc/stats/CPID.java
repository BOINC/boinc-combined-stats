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
public class CPID implements Comparable<Object> {
    long b_cpid_id;
    String user_cpid;
    String name;
    int create_time=0;
    int country_id=0;
    int project_count=0;
    int active_project_count=0;
    long total_credit=0;
    long rac=0;
    long rac_time=0;
    String hosts_visible;
    long global_credit=0;
    long global_rac=0;
    long global_new30days_credit=0;
    long global_new30days_rac=0;
    long global_new90days_credit=0;
    long global_new90days_rac=0;
    long global_new365days_credit=0;
    long global_new365days_rac=0;
    long global_1project_credit=0;
    long global_1project_rac=0;
    long global_5project_credit=0;
    long global_5project_rac=0;
    long global_10project_credit=0;
    long global_10project_rac=0;
    long global_20project_credit=0;
    long global_20project_rac=0;
    long global_country_credit=0;
    long global_country_rac=0;
    long global_joinyear_credit=0;
    long global_joinyear_rac=0;
    
    int join_year=0;
    public boolean bCompareRac=false;

    
    public int compareTo(Object o) {
    	
    	CPID co = (CPID) o;
    	Long l,l2;
    	if (bCompareRac) {
    		l = new Long(co.rac);
    		l2 = new Long(this.rac);
    	} else {
    		l = new Long(co.total_credit);
    		l2 = new Long(this.total_credit);    		
    	}
    	return l.compareTo(l2);
    }
}
