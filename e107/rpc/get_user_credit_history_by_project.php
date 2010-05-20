<?
header ( 'Content-type: text/xml' );

$project = $_GET ["projectid"];
$cpid = $_GET ["cpid"];

print "<?xml version=\"1.0\" encoding=\"iso-8859-1\"?>\n";
print "<user_credit_history_by_project>\n";
print "<title>User Credit History by Project</title>\n";

global $dbhandle;
global $dbhost;
global $dblogin;
global $dbpassword;
global $dbname;

include ('../dbconnect.php');

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

$dbhandle = mysqli_connect ( $dbhost, $dblogin, $dbpassword );

if ($dbhandle) {
	mysqli_select_db ( $dbhandle, $dbname );
	// get the pointer to the day
	$query = "select value from currentday where key_item='history90'";
	$res = mysqli_query ( $dbhandle, $query );
	if (! $res) {
		exit ();
	}
	
	$startday = 0;
	while ( ($row = mysqli_fetch_array ( $res )) ) {
		$startday = $row ["value"];
	}
	if (! $startday)
		$startday = 1;
	mysqli_free_result ( $res );
	
	// get the project info
	$query = "select name, shortname from projects where project_id=$project";
	$res = mysqli_query ( $dbhandle, $query );
	if (! $res) {
		exit ();
	}
	while ( ($row = mysqli_fetch_array ( $res )) ) {
		$project_name = $row ["name"];
		$project_shortname = $row ["shortname"];
	}
	
	// get the user info
	$query = "select a.name, b.country, a.user_id, a.url, a.total_credit, a.rac, a.rac_time, a.computer_count,a.active_computer_count from users_" . $project_shortname . " a, country b where a.country_id=b.country_id and a.user_cpid='" . $cpid . "'";
	$res_user = mysqli_query ( $dbhandle, $query );
	if (! $res_user) {
		echo "<b>Error performing query: " . mysqli_error ( $dbhandle ) . "</b>";
		exit ();
	}
	while ( ($row_user = mysqli_fetch_array ( $res_user )) ) {
		$project_username = $row_user ["name"];
		$project_country = $row_user ["country"];
		$project_userid = $row_user ["user_id"];
		$project_usertc = $row_user ["total_credit"];
		$project_userrac = $row_user ["rac"];
		$project_userractime = $row_user ["rac_time"];
		$project_computercount = $row_user ["computer_count"];
		$project_activecomputercount = $row_user ["active_computer_count"];
	}
	
	print "<cpid>$cpid</cpid>";
	print "<project_id>$project</project_id>\n";
	print "<project_name>" . escape_xml_special_chars ( $project_name ) . "</project_name>\n";
	print "<user_name>" . escape_xml_special_chars ( $project_username ) . "</user_name>\n";
	print "<user_country>$project_country</user_country>\n";
	print "<total_credit>$project_usertc</total_credit>\n";
	print "<expavg_credit>$project_userrac</expavg_credit>\n";
	print "<expavg_time>$project_userractime</expavg_time>\n";
	print "<computer_count>$project_computercount</computer_count>\n";
	print "<active_computer_count>$project_activecomputercount</active_computer_count>\n";
	print "<total_credit_history_last_91_days>\n";
	
	$query = "select * from history_user_tc_$project_shortname where user_id=$project_userid ";
	$res = mysqli_query ( $dbhandle, $query );
	if (! $res) {
		exit ();
	}
	
	while ( ($row = mysqli_fetch_array ( $res )) ) {
		
		$start = $startday + 1;
		if ($start > 91)
			$start = 1;
		
		for($count = 1; $count < 91; $count ++) {
			$rstring = "d_$start";
			$c = $row [$rstring];
			print "   <day_$count>$c</day_$count>\n";
			
			$start = $start + 1;
			if ($start > 91)
				$start = 1;
		}
		
		print "   <day_$count>$project_usertc</day_$count>\n";
	
	}
	mysqli_free_result ( $res );
	print "</total_credit_history_last_91_days>\n";

} else {
	print "   <database_error/>\n";
}

print "</user_credit_history_by_project>\n";
?>
