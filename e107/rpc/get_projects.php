<?
header('Content-type: text/xml');

print "<?xml version=\"1.0\" encoding=\"iso-8859-1\"?>\n";
print "<projects>\n";

include ('/home/virtual/netsoft-online.com/home/boinc/dbconnect.php');

$connect=mysql_connect($dbhost,$dblogin,$dbpassword);
mysql_select_db($dbname);
if ($connect != 0)
{

   $query = "select project_id,name,user_count,host_count,team_count,shown,url,retired,total_credit,rac,country_count from b_projects where shown='Y' or retired='Y'";
   $res = mysql_query($query);
   if (!$res) {
     exit();
   }

   while ($row = mysql_fetch_array($res)) 
   {

	$project_id = $row["project_id"];
        $project_name = $row["name"];
        $user_count = $row["user_count"];
        $host_count = $row["host_count"];
        $team_count = $row["team_count"];
        $shown = $row["shown"];
        $url = $row["url"];
        $retired = $row["retired"];
        $total_credit = $row["total_credit"];
        $rac = $row["rac"];
        $country_count = $row["country_count"];
        
	print "   <project>\n";
	print "     <project_id>$project_id</project_id>\n";
	print "     <project_name>$project_name</project_name>\n";
        print "     <total_credit>$total_credit</total_credit>\n";
        print "     <rac>$rac</rac>\n";
        print "     <user_count>$user_count</user_count>\n";
        print "     <host_count>$host_count</host_count>\n";
        print "     <team_count>$team_count</team_count>\n";
        print "     <country_count>$country_count</country_count>\n";
        if ($shown=="Y" && $retired=="N" && $url != "")
           print "     <url>$url</url>\n";
     
	print "   </project>\n";
   }
   
   mysql_free_result($res);

} else {
    print "   </database_error>\n";
}

print "</projects>\n";
?>
