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

public class ItemObj {

	//data members
	private String ItemName;
	private String Value;
	
	//constructor
	public ItemObj(String Name, String Val) {
		ItemName = Name;
		Value = Val;
	}
	
	//compares to ItemObj objects for equality
	public boolean equals(ItemObj obj) {
		if (ItemName.equalsIgnoreCase((String)obj.getName())) {
			if (Value.equalsIgnoreCase((String)obj.getValue())) {
				return true;
			}
			return false;
		} else {
			return false;
		}
	}
	
	//returns Item's Value
	public String getValue() {
		return Value;
	}
	
	//returns Item's Name
	public String getName() {
		return ItemName;
	}

	public void modify(String Val) {
		Value = Val;
	}
}