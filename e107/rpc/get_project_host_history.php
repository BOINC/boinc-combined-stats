<?
header ( 'Content-type: text/xml' );

$project = $_GET ["projectid"];

print "<?xml version=\"1.0\" encoding=\"iso-8859-1\"?>\n";
print "<project_host_count_history>\n";
print "<title>Project Host Count History</title>\n";

global $dbhost;
global $dblogin;
global $dbpassword;
global $dbname;

include ('../dbconnect.php');

$dbhandle = mysqli_connect ( $dbhost, $dblogin, $dbpassword );
if ($dbhandle) {
	mysqli_select_db ( $dbhandle, $dbname );
	$query = "select name, host_count from projects where project_id=$project";
	$res = mysqli_query ( $dbhandle, $query );
	if (! $res) {
		exit ();
	}
	
	$row = mysqli_fetch_array ( $res );
	$project_name = $row ["name"];
	$last_point = $row ["host_count"];
	mysqli_free_result ( $res );
	
	print "    <project_name>$project_name</project_name>\n";
	
	$query = "select value from currentday where key_item='history90'";
	$res = mysqli_query ( $dbhandle,  $query );
	if (! $res) {
		exit ();
	}
	
	$startday = 1;
	$row = mysqli_fetch_array ( $res );
	$startday = $row ["value"];
	mysqli_free_result ( $res );
	if (! $startday)
		$startday = 1;
	
	$query = "select * from history_projects_hosts where project_id=$project ";
	
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
		
		print "   <day_$count>$last_point</day_$count>\n";
	
	}
	
	mysqli_free_result ( $res );

} else {
	print "   </database_error>\n";
}

print "</project_host_count_history>\n";
?>
