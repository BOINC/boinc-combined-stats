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
import java.lang.String;
import java.util.Vector;


public class SectionObj {

	//data members
	public String SectionName;
	public Vector<ItemObj> ItemNames = new Vector<ItemObj>(5);
	
	public SectionObj(String Name) {
		this.SectionName = Name;
	}
	
	public void insert(String Name, String Val) {
		ItemObj obj = new ItemObj(Name, Val);
		ItemNames.addElement(obj);
	}

	public void remove(String Name) {
		for (int i = 0; i < ItemNames.size(); i++) {
			ItemObj temp = (ItemObj)ItemNames.elementAt(i);
			if (((String)temp.getName()).equalsIgnoreCase(Name)) {
				ItemNames.removeElementAt(i);
			}
		}
	}
	
	public String getValue(String Name) {
		for (int i = 0; i < ItemNames.size(); i++) {
			ItemObj temp = (ItemObj)ItemNames.elementAt(i);
			if (((String)temp.getName()).equalsIgnoreCase(Name)) {
				return (String)temp.getValue();
			}
		}
		return null;
	}

	public void modify(String Name, String Val) {
		for (int i = 0; i < ItemNames.size(); i++) {
			ItemObj temp = (ItemObj)ItemNames.elementAt(i);
			if (((String)temp.getName()).equalsIgnoreCase(Name)) {
				temp.modify(Val);
			}
		}
	}
}