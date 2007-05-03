<?
header('Content-type: text/xml');

print "<?xml version=\"1.0\" encoding=\"iso-8859-1\"?>\n";
print "<projects>\n";

include ('/home/virtual/netsoft-online.com/home/boinc/dbconnect.php');

$connect=mysql_connect($dbhost,$dblogin,$dbpassword);
mysql_select_db($dbname);
if ($connect != 0)
{

   $query = "select project_id,name from b_projects";
   $res = mysql_query($query);
   if (!$res) {
     exit();
   }

   while ($row = mysql_fetch_array($res)) 
   {

	$project_id = $row["project_id"];
        $project_name = $row["name"];
	print "   <project>\n";
	print "     <project_id>$project_id</project_id>\n";
	print "     <project_name>$project_name</project_name>\n";
	print "   </project>\n";
   }
   
   mysql_free_result($res);

} else {
    print "   </database_error>\n";
}

print "</projects>\n";
?>
