<?php
if (! defined ( 'e107_INIT' )) {
	exit ();
}

include ("bp_functions.php");
include_lan ( e_PLUGIN . 'boinc/languages/' . e_LANGUAGE . '/lan_boinc.php' );

$bp_title = LAN_140;
$text = LAN_141;

if (defined ( "BULLET" )) {
	$bullet = "<img src='" . THEME_ABS . "images/" . BULLET . "' alt='' style='vertical-align: middle;' />";
} elseif (file_exists ( THEME . "images/bullet2.gif" )) {
	$bullet = "<img src='" . THEME_ABS . "images/bullet2.gif' alt='bullet' style='vertical-align: middle;' />";
} else {
	$bullet = "";
}

$dbhandle = statsdb_connect ();
if ($dbhandle) {
	
	$query = "select name,project_id from projects where shown='Y' and project_id <> 19 order by name";
	
	$res = mysqli_query ( $dbhandle, $query );
	if (! $res) {
		$text = LAN_142;
	} else {
		
		$text = "$bullet <a href=\"" . e_PLUGIN . "boinc/bp.php?project=19\">BOINC Combined</a>";
		
		while ( $row = mysqli_fetch_array ( $res ) ) {
			$name = $row ["name"];
			$project_id = $row ["project_id"];
			
			$text .= "<br/>$bullet <a href=\"" . e_PLUGIN . "boinc/bp.php?project=$project_id\">$name</a>";
		}
	}
}
$ns->tablerender ( $bp_title, $text );

?>

