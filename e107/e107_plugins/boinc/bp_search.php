<?php
// Remember that we must include class2.php
require_once ("../../class2.php");

require_once ("bp_functions.php");
include_lan ( e_PLUGIN . 'boinc/languages/' . e_LANGUAGE . '/lan_boinc.php' );

$searchname = $_POST ["searchname"];
$exact = $_POST ["exact"];
$submit = $_POST ["submit"];

$mytext = "<table class='fborder'>\n";

if ($searchname == "") {
	$mytext .= "<tr><th class='forumheader'>" . LAN_270 . "</th></tr>";
	$mytext .= "<tr><td class='nforumthread'>" . LAN_271 . "</td></tr></table>";
	require_once (HEADERF);
	$ns->tablerender ( LAN_272, $mytext );
	require_once (FOOTERF);
	return;
}

$query = "SELECT a.user_name,a.user_cpid,b.name as pname,a.total_credit from cpid_map a, projects b where a.project_id=b.project_id and ";

if ($exact == "y")
	$query .= "a.user_name = '$searchname'";
else
	$query .= "a.user_name like '%$searchname%'";

$query .= " order by user_cpid";
$dbhandle = statsdb_connect ();
if ($dbhandle) 
{
	$res = mysqli_query ( $dbhandle, $query );
	
	if (! $res) {
		echo "<b>" . LAN_232 . ": " . mysqli_error ($dbhandle) . "</b>" ;
		exit ();
	}
	
	$mytext .= "<tr><th class='forumheader2'>" . LAN_273 . "</th><th class='forumheader2'>" . LAN_274 . "</th><th class='forumheader2'>" . LAN_275 . "</th><th class='forumheader2'>" . LAN_276 . "</th></tr>\n";
	
	$count = 0;
	
	while ( $row = mysqli_fetch_array ( $res ) ) {
		$name = $row ["user_name"];
		$user_cpid = $row ["user_cpid"];
		$project = $row ["pname"];
		$pr = $row ["project_rank_credit"];
		
		if ($count % 2 == 0)
			$cc = "nforumthread";
		else
			$cc = "nforumthread2";
		$count ++;
		
		$mytext .= "<tr><td class='$cc'><a href=\"get_user.php?cpid=$user_cpid\">$name</a>";
		$mytext .= "</td><td class='$cc'>$project</td><td class='$cc'>$pr</td><td class='$cc'>$user_cpid</td></tr>\n";
	}
}

$mytext .= "</table>";

require_once (HEADERF);
$ns->tablerender ( LAN_277, $mytext );
require_once (FOOTERF);
?>
