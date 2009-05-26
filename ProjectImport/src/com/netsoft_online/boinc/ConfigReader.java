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
import java.io.*;
import java.util.*;


public class ConfigReader {

	//data members
	Hashtable<String,SectionObj> iniFile;
	Vector<String> printableFile;
	
	public ConfigReader(){
		iniFile = new Hashtable<String,SectionObj>();
		printableFile = new Vector<String>();
	}
	
	public void addData(String Section, String Data, String Value) {
		Section = Section.toLowerCase();
		boolean validSection = false;
		SectionObj obj = (SectionObj)iniFile.get(Section);
		if (obj == null) {
			obj = new SectionObj(Section);
			obj.insert(Data, Value);
			iniFile.put(Section, obj);
			printableFile.add(new String(""));
			printableFile.add(new String("[" + Section + "]"));
			printableFile.add(new String(Data + "=" + Value));
			printableFile.add(new String(""));
		} else {
			obj.insert(Data, Value);
			for (int i = 0; i < printableFile.size(); i++) {
				String line = (String)printableFile.get(i);
				line.trim();
				if (validSection == true) {
					if ((line.length() == 0) || (line.startsWith("[") && line.endsWith("]"))) {
						printableFile.insertElementAt(new String(Data + "=" + Value),i);
						validSection = false;
					}
				}
				else if (line.startsWith("[") && line.endsWith("]")) {
					int stringSize = line.length();
					String Name = line.substring(1, (stringSize - 1));
					if (Name.equalsIgnoreCase(Section)) {
						validSection = true;
					}
				}
			}
		}	
	}

	// Changes the value of a data item within a specific section object.
	// If the section or data objects don't exist the method does nothing.

	public void modifyData(String section, String data, String value) {
		section = section.toLowerCase();
		boolean validSection = false;
		SectionObj obj = (SectionObj)iniFile.get(section);
		if (obj == null) {
			System.err.println("No such section");
		} else {
			obj.modify(data, value);
			for (int i = 0; i < printableFile.size(); i++) {
				String line = (String)printableFile.get(i);
				line.trim();
				if (validSection == true) {
					int equalsIndex = line.indexOf("=");
					if (!(equalsIndex == -1)) {
						String newLine = line.substring(0,equalsIndex);
						if (newLine.equalsIgnoreCase(data)) {
							printableFile.setElementAt((String)newLine + "=" + value, i);
							validSection = false;
						}
					}
				}
				else if (line.startsWith("[") && line.endsWith("]")) {
					int stringSize = line.length();
					String Name = line.substring(1, (stringSize - 1));
					validSection = false;
					if (Name.equalsIgnoreCase(section)) {
						validSection = true;
					}
				}
			}
		}
	}

	public void removeItem(String section, String data) {
		section = section.toLowerCase();
		boolean validSection = false;
		SectionObj obj = (SectionObj)iniFile.get(section);
		if (obj == null) {
			System.err.println("No such section");
		} else {
			obj.remove(data);
			for (int i = 0; i < printableFile.size(); i++) {
				String line = (String)printableFile.get(i);
				line.trim();
				if (validSection == true) {
					int equalsIndex = line.indexOf("=");
					line.trim();
					if (!(equalsIndex == -1)) {
						String newLine = line.substring(0,equalsIndex);
						if (newLine.equalsIgnoreCase(data)) {
							printableFile.removeElementAt(i);
							validSection = false;
						}
					}
				}
				else if (line.startsWith("[") && line.endsWith("]")) {
					int stringSize = line.length();
					String Name = line.substring(1, (stringSize - 1));
					validSection = false;
					if (Name.equalsIgnoreCase(section)) {
						validSection = true;
					}
				}
			}
		}
	}

	public void removeSection(String section) {
		section = section.toLowerCase();
		boolean validSection = false;
		int startIndex = 0;
		int endIndex = 0;
		SectionObj obj = (SectionObj)iniFile.get(section);
		if (obj == null) {
			System.err.println("No such Section");
		} else {
			iniFile.remove(section);
			for (int i = 0; i < printableFile.size(); i++) {
				String line = (String)printableFile.get(i);
				line.trim();
				if (line.startsWith("[") && line.endsWith("]")) {
					if (validSection == true) {
						validSection = false;
						endIndex = i;
					} else {
						int stringSize = line.length();
						String Name = line.substring(1, (stringSize - 1));
						validSection = false;
						if (Name.equalsIgnoreCase(section)) {
							validSection = true;
							startIndex = i;
						}
					}
				}
			}
			for (int j = startIndex; j < endIndex; j++) {
				printableFile.removeElementAt(startIndex);
			}
		}
	}

	public void printToFile(String outFileName) {
		PrintWriter Out = null;
		try {
			File outFile = new File(outFileName);
			Out = new PrintWriter(new BufferedWriter(new FileWriter(outFile)));
		} catch (IOException ex){
			System.err.println("Error opening file " + outFileName + " failed so stopping. The error was:");
			System.err.println(ex);
		}
		for (int i = 0; i < printableFile.size(); i++) {
			Out.println((String)printableFile.get(i));
		}
		Out.close();
	}
	
	public boolean ReadConfigFile(String filename) throws 
									IOException {
		//create the object to read from a file
		BufferedReader fin = new BufferedReader(new 
									FileReader(filename));
		String LastSectionName = "No Talent Ass Clown";
		SectionObj obj = new SectionObj(LastSectionName);
		while(fin.ready()) {
			String line = fin.readLine();
			printableFile.add(line);
			line.trim();
			if ((line.indexOf(";") >= 0) || (line.indexOf("#") >= 
															0)) {
				int startComment = line.indexOf(";");
				if ((line.indexOf("#") >= 0) && (line.indexOf("#") 
											> startComment)) {
					startComment = line.indexOf("#");
				}
				line = line.substring(0, (startComment));
				line.trim();
			}
			if (line.startsWith("[") && line.endsWith("]")) {
				int stringSize = line.length();
				String Name = line.substring(1, (stringSize - 1));
				Name = Name.toLowerCase();
				obj = new SectionObj(Name);
				iniFile.put(Name, obj);
				LastSectionName = Name;
			} else if (LastSectionName != "No Talent Ass Clown") {
				int equalsIndex = line.indexOf("=");
				if (equalsIndex != -1) {
					String name = line.substring(0, (equalsIndex));
					String value = line.substring(equalsIndex + 1);
					obj.insert(name, value);
				}
			} else if (line.length() == 0) {
			}			
			else {
				throw new IOException();
			}
		}
		return true;
	}
				
	public void ResetConfig() {
		iniFile.clear();
		printableFile.clear();	
	}
	
	public int GetValueInt(String section, String item, 
											int Default) {
		section = section.toLowerCase();
		if (iniFile.get(section) == null) {
			return Default;
		} else {
			String s = 
				((SectionObj)iniFile.get(section)).getValue(item);
			int i;
			try {
				i = Integer.parseInt(s);
			} catch (NumberFormatException ex) {
				return Default;
			}
			return i;
		}
	}
	
	public int GetValueInt(String section, String item) throws 
										NumberFormatException {
		section = section.toLowerCase();
		String s = 
				((SectionObj)iniFile.get(section)).getValue(item);
		return Integer.parseInt(s);	
	}
	
	public String GetValueString(String section, String item, 
												String Default) {
		section = section.toLowerCase();
		if (iniFile.get(section) == null) {
			return Default;
		} else {
			try {
				String s = 
				((SectionObj)iniFile.get(section)).getValue(item);
				return s;
			} catch (Throwable ex) {
				return Default;
			}
		}
	}
	
	public String GetValueString(String section, String item) 
									throws NullPointerException {
		section = section.toLowerCase();
		if (iniFile.get(section) == null) {
			throw new NullPointerException();
		} else {
			String s = 
				((SectionObj)iniFile.get(section)).getValue(item);
			return s;
		}
	}
	
	public boolean GetValueBoolean(String section, 
									String item, boolean Default) {
		section = section.toLowerCase();
		if (iniFile.get(section) == null) {
			return Default;
		}
		String s = 
				((SectionObj)iniFile.get(section)).getValue(item);
		if (s == null) return Default;
		
		if (s.equalsIgnoreCase("yes") || s.equalsIgnoreCase("y") 
											|| s.equals("1")) {
			return true;
		} else if (s.equalsIgnoreCase("no") || 
						s.equalsIgnoreCase("n") || s.equals("0")) {
			return false;
		} else {
			return Default;
		}		
	}
	
	public boolean GetValueBoolean(String section, String item) 
									throws NullPointerException {
		section = section.toLowerCase();
		if (iniFile.get(section) == null) {
			throw new NullPointerException();
		}
		String s = 
				((SectionObj)iniFile.get(section)).getValue(item);
		if (s.equalsIgnoreCase("yes") || s.equalsIgnoreCase("y") 
											|| s.equals("1")) {
			return true;
		} else if (s.equalsIgnoreCase("no") || 
						s.equalsIgnoreCase("n") || s.equals("0")) {
			return false;
		} else {
			throw new NullPointerException();
		}	
	}
}