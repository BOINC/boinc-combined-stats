<?php
$cpid = $_GET ["cpid"];
$html = isset ( $_GET ["html"] ) ? $_GET ["html"] : '';

function escape_xml_special_chars($text) {
	// first unescape them in the case where they may 
	// already be escaped - don't want to do it twice
	$text = str_replace ( "&lt;", "<", $text );
	$text = str_replace ( "&gt;", ">", $text );
	$text = str_replace ( "&apos;", "'", $text );
	$text = str_replace ( "&amp;", "&", $text );
	
	// then escape them
	$text = str_replace ( "&", "&amp;", $text );
	$text = str_replace ( "<", "&lt;", $text );
	$text = str_replace ( ">", "&gt;", $text );
	$text = str_replace ( "'", "&apos;", $text );
	return $text;
}

$rc = 0;

if ($cpid == "") {
	print "<error>cpid not specified</error>\n";
	exit ();
}

if ($html == "") {
	header ( 'Content-type: text/xml' );
	print "<?xml version=\"1.0\" encoding=\"iso-8859-1\"?>\n";
	print "<user>\n";
} else {
	$head = "Location: http://boinc.netsoft-online.com/e107_plugins/boinc/get_user.php?cpid=$cpid";
	header ( $head );
	
	print "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0 Transitional//EN\" \"http://www.w3.org/TR/REC-html40/loose.dtd\">\n";
	print "<html><head><title>User Stats Detail</title>\n";
	print "</head>\n<body>\n";
	print "This page has moved to <a href=\"http://boinc.netsoft-online.com/e107_plugins/boinc/get_user.php?cpid=$cpid\">here</a>";
	print "</body></html>";
	exit ();
}

global $dbhost;
global $dblogin;
global $dbpassword;
global $dbname;

// connect to dtabase
include ('../dbconnect.php');

$dbhandle = mysqli_connect ( $dbhost, $dblogin, $dbpassword );

$printcount = 0;

if ($dbhandle) {
	mysqli_select_db ( $dbhandle, $dbname );
	$global_info = "";
	$project_info = "";
	
	// Get the global info for the user
	$query = "select global_credit, global_rac, user_name from cpid where user_cpid='" . $cpid . "'";
	
	$res = mysqli_query ( $dbhandle, $query );
	if (! $res) {
		echo "<b>Error performing query: " . mysqli_error ($dbhandle) . "</b>";
		exit ();
	}
	
	while ( ($row = mysqli_fetch_array ( $res )) ) {
		$global_rank_credit = $row ["global_credit"];
		$global_rank_rac = $row ["global_rac"];
		$global_user_name = $row ["user_name"];
	}
	
	$global_total_credit = 0;
	$global_rac = 0;
	
	// Now get the list of projects they are participating in
	$query = "select a.project_id, b.shortname, b.name, a.user_id, a.team_id, b.url from cpid_map a, projects b where a.project_id=b.project_id and a.user_cpid='" . $cpid . "' and b.shown='Y' and b.retired='N'";
	$res = mysqli_query ( $dbhandle, $query );
	if (! $res) {
		echo "<b>Error performing query: " . mysqli_error ( $dbhandle ) . "</b>";
		exit ();
	}
	while ( ($row = mysqli_fetch_array ( $res )) ) {
		
		$project_id = $row ["project_id"];
		$project_name = $row ["name"];
		$project_shortname = $row ["shortname"];
		$project_userid = $row ["user_id"];
		$project_teamid = $row ["team_id"];
		$project_url = $row ["url"];
		// Now fetch the information from the project tables
		

		$project_info .= "  <project>\n    <name>" . $project_name . "</name>\n    <project_id>" . $project_id . "</project_id>\n";
		$query = "select a.name, b.country, a.create_time, a.url, a.total_credit, a.rac, a.rac_time, a.project_rank_credit, a.project_rank_rac, a.team_rank_credit, a.team_rank_rac, a.computer_count,a.active_computer_count from users_" . $project_shortname . " a, country b where a.country_id=b.country_id and a.user_id=" . $project_userid;
		$res_user = mysqli_query ( $dbhandle, $query );
		if (! $res_user) {
			echo "<b>Error performing query: " . mysqli_error ( $dbhandle ) . "</b>";
			exit ();
		}
		while ( ($row_user = mysqli_fetch_array ( $res_user )) ) {
			$project_username = $row_user ["name"];
			$project_country = $row_user ["country"];
			$project_createtime = $row_user ["create_time"];
			$project_userurl = $row_user ["url"];
			$project_usertc = $row_user ["total_credit"];
			$project_userrac = $row_user ["rac"];
			$project_userractime = $row_user ["rac_time"];
			$project_userrank_credit = $row_user ["project_rank_credit"];
			$project_userrank_rac = $row_user ["project_rank_rac"];
			$project_userrank_teamcredit = $row_user ["team_rank_credit"];
			$project_userrank_teamrac = $row_user ["team_rank_rac"];
			$project_computercount = $row_user ["computer_count"];
			$project_activecomputercount = $row_user ["active_computer_count"];
		}
		$global_total_credit += $project_usertc;
		$global_rac += $project_userrac; // TODO: Decay
		

		$project_info .= "    <url>" . escape_xml_special_chars ( $project_url ) . "</url>\n";
		$project_info .= "    <total_credit>" . $project_usertc . "</total_credit>\n";
		$project_info .= "    <expavg_credit>" . $project_userrac . "</expavg_credit>\n";
		$project_info .= "    <expavg_time>" . $project_userractime . "</expavg_time>\n";
		$project_info .= "    <project_rank_total_credit>" . $project_userrank_credit . "</project_rank_total_credit>\n";
		$project_info .= "    <project_rank_expavg_credit>" . $project_userrank_rac . "</project_rank_expavg_credit>\n";
		$project_info .= "    <id>" . $project_userid . "</id>\n";
		$project_info .= "    <create_time>" . $project_createtime . "</create_time>\n";
		$project_info .= "    <country>" . $project_country . "</country>\n";
		$project_info .= "    <user_name>" . escape_xml_special_chars ( $project_username ) . "</user_name>\n";
		$project_info .= "    <user_url>" . escape_xml_special_chars ( $project_userurl ) . "</user_url>\n";
		
		if ($project_teamid && $project_teamid != 0) {
			// is a member of a team, get team info
			$query = "select name, url, nusers from teams_" . $project_shortname . " where team_id=" . $project_teamid;
			$res_team = mysqli_query ( $dbhandle, $query );
			if (! $res_team) {
				echo "<b>Error performing query: " . mysqli_error ( $dbhandle ) . "</b>";
				exit ();
			}
			while ( ($row_team = mysqli_fetch_array ( $res_team )) ) {
				$team_name = $row_team ["name"];
				$team_url = $row_team ["url"];
				$team_usercount = $row_team ["nusers"];
			}
			
			$project_info .= "    <team_id>" . $project_teamid . "</team_id>\n";
			$project_info .= "    <team_name>" . escape_xml_special_chars ( $team_name ) . "</team_name>\n";
			$project_info .= "    <team_url>" . escape_xml_special_chars ( $team_url ) . "</team_url>\n";
			$project_info .= "    <team_rank_total_credit>" . $project_userrank_teamcredit . "</team_rank_total_credit>\n";
			$project_info .= "    <team_rank_expavg_credit>" . $project_userrank_teamrac . "</team_rank_expavg_credit>\n";
			$project_info .= "    <team_member_count>" . $team_usercount . "</team_member_count>\n";
		}
		
		$project_info .= "    <computer_count>" . $project_computercount . "</computer_count>\n";
		$project_info .= "    <active_computer_count>" . $project_activecomputercount . "</active_computer_count>\n";
		$project_info .= "  </project>\n";
	}
	
	$global_info .= "  <total_credit>" . $global_total_credit . "</total_credit>\n";
	$global_info .= "  <expavg_credit>" . $global_rac . "</expavg_credit>\n";
	$global_info .= "  <expavg_time>" . $project_userractime . "</expavg_time>\n"; // TODO: Fix me
	$global_info .= "  <name>" . escape_xml_special_chars ( $global_user_name ) . "</name>\n";
	$global_info .= "  <world_rank_total_credit>" . $global_rank_credit . "</world_rank_total_credit>\n";
	$global_info .= "  <world_rank_expavg_credit>" . $global_rank_rac . "</world_rank_expavg_credit>\n";
	
	print $global_info;
	print $project_info;
	print "</user>\n";

} else {
	print "<?xml version=\"1.0\" encoding=\"iso-8859-1\"?>\n";
	print "<user>\n";
	print "  <error>Can't connect to database</error>\n";
	print "</user>\n";
}

?>
