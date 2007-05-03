This SVN repository contains the source code for the BOINC Combined Statistics web site
(http://boinc.netsoft-online.com/). 

Here is a quick guide to the components of the repository:


bsimport - This directory contains the java source code of the import and 
           statistic calculation program. This is the program that is run
           daily to import the statistics from the various BOINC projects
           into the stats database.

database - This directory contains the MySQL table creation and update scripts
           to create the underlying database for the statistics.

e107     - This directory contains the theme and plugin modules for the e107
           web site (http://www.e107.org). When these are added and activated
           to an e107 system, will produce the viewable results of the stats.
           The site currently uses version 0.7.8 of e107.

scripts  - This directory contains other various scripts used by BCS.




The general flow of how BOINC Combined Statistics runs is:

1) The fetch_files.sh script runs twice a day to grab the export files
   from various projects. The scheduled time of the run is set so it
   should finish fetching the files before the import process runs.
   It utilizes wget, and only downloads the file if it has changed from
   the previous time the script ran.  The idea behind running the fetch
   multiple times is to avoid temporary connection failures to projects.

2) The bsimport java application runs. It runs once a day. This application
   will take the data files from each project and import the data into
   the mysql database. After a sucessful import, it creates a backup file
   of the data. If any of the three import files (team,host,user) has a
   problem, the program will go through and decay the RAC values so all 
   values in the DB are calculated to the same time.

   There are a number of optimizations in the import process. For example,
   if the host hasn't been heard from in some time, and its RAC is already
   decayed to zero, then the import process just skips past it - not doing
   any database transactions for that particular host.

   Once all the data has been imported, bsimport creates the stats. It runs
   through and calculates things like world rank, etc. Often these are 
   done via SQL statments, letting the DB do the work. Other times - in 
   particular the credit per cpu second analysis, the program does the
   full calculation as it can be done far faster by reading all the host
   data in order of host cpid than the database can do it.

   After the stats are calculated, bsimport exports a sorted list of the
   combined user data (currently only user data, not host or team data) 
   for use by others to import. The file is a gzip'd xml file that is
   made available for download on the web site.

  
3) The statistics are made available via the e107 plugins and other various
   web RPC pages. These web pages access the data in the mysql database for
   display via XML, web pages and graphs.

