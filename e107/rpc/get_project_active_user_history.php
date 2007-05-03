<?
header('Content-type: text/xml');

$project = $_GET["projectid"];

print "<?xml version=\"1.0\" encoding=\"iso-8859-1\"?>\n";
print "<project_active_user_count_history>\n";

include ('/home/virtual/netsoft-online.com/home/boinc/dbconnect.php');

$connect=mysql_connect($dbhost,$dblogin,$dbpassword);
mysql_select_db($dbname);

if ($connect != 0)
{

   $query = "select name from b_projects where project_id=$project";
   $res = mysql_query($query);
   if (!$res) {
     exit();
   }

   $row = mysql_fetch_array($res);
   $project_name = $row["name"];
   mysql_free_result($res);
 
   print "   <project_name>$project_name</project_name>\n";
 
   $query = "select value from b_currentday where key_item='year'";
   $res = mysql_query($query);
   if (!$res) {
     exit();
   }

   $row = mysql_fetch_array($res);
   $startday = $row["value"];
   mysql_free_result($res);
  
   $fd = "";
   $ld = ""; 
   $min = "";
   $max = "";

   $query = "select * from b_project_active_users_hist where project_id=$project ";

   $res = mysql_query($query);
   if (!$res) {
     exit();
   }
   
   $count = 1;

   while ($row = mysql_fetch_array($res)) 
   {

	$start = $startday + 1;
   	if ($start > 365) $start = 1;
	$end = $startday;

	while ($start != $end) {
		$rstring = "d_$start";
		$c = $row[$rstring];
		print "   <day_$count>$c</day_$count>\n";

		$start = $start + 1;
		if ($start > 365) $start = 1;
                $count++;
	}      

	$rstring = "d_$start";
	$c = $row[$rstring];
	print "   <day_$count>$c</day_$count>\n";

   }
   
   mysql_free_result($res);

} else {
    print "   <database_error/>\n";
}

print "</project_active_user_count_history>\n";
?>
