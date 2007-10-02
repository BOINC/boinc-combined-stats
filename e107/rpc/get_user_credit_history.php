<?
header('Content-type: text/xml');

$project = $_GET["projectid"];

print "<?xml version=\"1.0\" encoding=\"iso-8859-1\"?>\n";
print "<user_credit_history>\n";
print "<title>User Credit History</title>\n";

include ('/home/virtual/netsoft-online.com/home/boinc/dbconnect.php');

$connect=mysql_connect($dbhost,$dblogin,$dbpassword);
mysql_select_db($dbname);

if ($connect != 0)
{

   $query = "select key_item,value from b_currentday where key_item='tc' or key_item='week'";
   $res = mysql_query($query);
   if (!$res) {
     exit();
   }

   $startday = 0;
   $startweek = 0;

   while ($row = mysql_fetch_array($res)) {
       if ($row['key_item']=="tc")
             $startday = $row["value"];
       
       if ($row['key_item']=="week")
             $startweek = $row["value"];
   }

   if ($startday == 0 || $startweek == 0) {
      print "<ERROR>Failed to get date stuff</ERROR>\n";
      exit();
   }

   mysql_free_result($res);
  
   $fd = "";
   $ld = ""; 
   $min = "";
   $max = "";

   $query = "select * from b_cpid a, b_cpid_total_credit_hist b where a.table_id=b.b_cpid_id and a.user_cpid='$cpid'";

   $res = mysql_query($query);
   if (!$res) {
     exit();
   }
   
   print "   <cpid>$cpid</cpid>\n";

   while ($row = mysql_fetch_array($res)) 
   {

        $project_count = $row['project_count'];
        print "   <project_count>$project_count</project_count>\n";

        $project_count = $row['active_project_count'];
        print "   <active_project_count>$project_count</active_project_count>\n";

        $tc = $row['total_credit'];
        print "   <total_credit>$tc</total_credit>\n";

        $rac = $row['rac'];
        print "   <rac>$rac</rac>\n";
        $rac_time = $row['rac_time'];
        print "   <rac_time>$rac_time</rac_time>\n";

        $cc = $row['computer_count'];
        print "   <computer_count>$cc</computer_count>\n";

        $cc = $row['active_computer_count'];
        print "   <active_computer_count>$cc</active_computer_count>\n";

        $count = 1;
	$start = $startday + 1;
   	if ($start > 91) $start = 1;
	$end = $startday;

        print "   <total_credit_history_last_91_days>\n";

	while ($start != $end) {
		$rstring = "d_$start";
		$c = $row[$rstring];
		print "      <day_$count>$c</day_$count>\n";

		$start = $start + 1;
		if ($start > 91) $start = 1;
                $count++;
	}      

	$rstring = "d_$start";
	$c = $row[$rstring];
	print "      <day_$count>$c</day_$count>\n";

        print "   </total_credit_history_last_91_days>\n";

        $start = $startweek + 1;
        if ($start > 52) $start = 1;
	$end = $startweek;

        print "   <total_credit_history_last_year>\n";

        $count = 1;
	while ($start != $end) {
		$rstring = "w_$start";
		$c = $row[$rstring];
		print "      <week_$count>$c</week_$count>\n";

		$start = $start + 1;
		if ($start > 52) $start = 1;
                $count++;
	}      

	$rstring = "w_$start";
	$c = $row[$rstring];
	print "      <week_$count>$c</week_$count>\n";

        print "   </total_credit_history_last_year>\n";


   }
   
   mysql_free_result($res);

} else {
    print "   <database_error/>\n";
}

print "</user_credit_history>\n";
?>
