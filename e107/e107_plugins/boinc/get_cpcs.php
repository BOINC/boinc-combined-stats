<?php
// Remember that we must include class2.php
require_once ("../../class2.php");

require_once ("bp_functions.php");
include_lan ( e_PLUGIN . 'boinc/languages/' . e_LANGUAGE . '/lan_boinc.php' );

$dbhandle = statsdb_connect ();
if ($dbhandle) {
	$query = "select distinct a.project_id1, b.name from cpcs a, projects b where a.project_id1=b.project_id order by project_id1";
	
	$res = mysqli_query ( $dbhandle, $query );
	
	if (! $res) {
		echo "<b>" . LAN_232 . ": " . mysqli_error ($dbhandle) . "</b>";
		exit ();
	}
	
	$data = "<table ><tr><th>&nbsp;</th>\n";
	
	$cnt = 0;
	while ( ($row = mysqli_fetch_array ( $res )) ) {
		$pid = $row ["project_id1"];
		$pname = $row ["name"];
		$pid_array [$cnt] = $pid;
		$cnt ++;
		
		$chars = preg_split ( '//', $pname, - 1, PREG_SPLIT_NO_EMPTY );
		$vname = "";
		
		for($i = 0; $i < strlen ( $pname ); $i ++)
			$vname .= $chars [$i] . "<br/>";
		
		$data .= "  <td class='forumheader2a' valign=\"bottom\">$vname</td>\n";
	}
	
	$data .= "</tr>\n";
	
	$query = "select a.project_id1,a.project_id2,a.count,a.r_over_c,b.name,b.active_hosts from cpcs a, projects b where a.project_id1=b.project_id order by project_id1,project_id2";
	
	$res = mysqli_query ( $dbhandle, $query );
	
	if (! $res) {
		echo "<b>" . LAN_232 . ": " . mysqli_error ($dbhandle) . "</b>";
		exit ();
	}
	
	$last = 0;
	$sk = 0;
	$cc = 0;
	
	while ( ($row = mysqli_fetch_array ( $res )) ) {
		$pid1 = $row ["project_id1"];
		$pid2 = $row ["project_id2"];
		$count = $row ["count"];
		$r_over_c = $row ["r_over_c"];
		$pname = $row ["name"];
		$ah = $row ["active_hosts"];
		
		if ($last == 0) {
			$last = $pid1;
			$data .= "<tr><td class='forumheader2'>$pname</td>";
		}
		
		if ($last == $pid1) {
			while ( $pid_array [$cc] != $pid2 && $cc < $cnt ) {
				if ($pid_array [$cc] == $pid1)
					$data .= "<td class='nforumthread'>1.000<br/>($ah)</td>";
				else
					$data .= "<td class='nforumthread'>&nbsp;</td>";
				$cc ++;
			}
			$data .= sprintf ( "<td class='nforumthread'>%.3f<br/> ($count)</td>", $r_over_c );
		} else {
			for($r = $cc; $r < $cnt; $r ++) {
				$data .= "<td class='nforumthread'>&nbsp;</td>";
			}
			$cc = 0;
			$sk ++;
			$data .= "</tr>\n<tr><td class='forumheader2'>$pname</td>";
			while ( $pid_array [$cc] != $pid2 && $cc < $cnt ) {
				$data .= "<td class='nforumthread'>&nbsp;</td>";
				$cc ++;
			}
			$data .= sprintf ( "<td class='nforumthread'>%.3f<br/> ($count)</td>", $r_over_c );
		}
		$cc ++;
		$last = $pid1;
	}
}
$data .= "<td class='nforumthread'>1.000<br/>($ah)</td></tr></table>";

// Kick out the display to e107 engine
require_once (HEADERF);
?>

<div class='dcaption'>
<div class='left'></div>
<div class='right'></div>
<div class='center'><?php
echo LAN_260;
?></div>
</div>
<div class='dbody'>
<div class='leftwrapper'>
<div class='rightwrapper'>
<div class='leftcontent'></div>
<div class='rightcontent'></div>
<div class='dcenter'>
<div class='dinner'><?php
echo LAN_261;
?>
<br />
</div>
</div>
</div>
</div>
</div>
<div class='dbottom'>
<div class='left'></div>
<div class='right'></div>
<div class='center'></div>
</div>

<?php

$ns->tablerender ( LAN_262, $data );
require_once (FOOTERF);

?>
