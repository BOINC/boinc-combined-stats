<?php
require_once ("../../class2.php");

global $dbname;
global $dbe107;
global $NEWSFEED_LIST_START;
global $NEWSFEED_LIST_END;
require_once ("bp_functions.php");
include_lan ( e_PLUGIN . 'boinc/languages/' . e_LANGUAGE . '/lan_boinc.php' );

$cpid = $_GET ["cpid"];

$rc = 0;

if ($cpid == "") {
	$data = LAN_230 . "\n";
	$tname = LAN_231;
	require_once (HEADERF);
	$text = $NEWSFEED_LIST_START . $data . $NEWSFEED_LIST_END;
	$ns->tablerender ( $tname, $text );
	require_once (FOOTERF);
	return;
}

$data = "";

$dbhandle = statsdb_connect ();
if ($dbhandle) {
	
	// get the overal boinc totals
	$query = "SELECT user_count,total_credit from projects where project_id=1";
	
	$res = mysqli_query ( $dbhandle, $query );
	
	if (! $res) {
		echo "<b>" . LAN_232 . ": " . mysqli_error ($dbhandle) . "</b>";
		exit ();
	}
	
	while ( ($row = mysqli_fetch_array ( $res )) ) {
		$boinc_user_count = $row ["user_count"];
		$boinc_total_credit = $row ["total_credit"];
	}
	
	// Get the global info for the user
	$query = "select a.*, b.country from cpid a, country b where a.country_id=b.country_id and user_cpid='" . $cpid . "'";
	
	$res = mysqli_query ( $dbhandle, $query );
	if (! $res) {
		echo "<b>Error performing query: " . mysqli_error ($dbhandle) . "</b>";
		exit ();
	}
	
	while ( ($row = mysqli_fetch_array ( $res )) ) {
		$global_user_name = $row ["user_name"];
		$global_join_date = $row ["join_date"];
		$global_country = $row ["country"];
		$global_countryid = $row ["country_id"];
		//$global_active_project_count = $row["active_project_count"];
		//$global_project_count = $row["project_count"];
		$global_total_credit = $row ["total_credit"];
		$global_rac = $row ["rac"];
		$global_rank_credit = $row ["global_credit"];
		$global_rank_rac = $row ["global_rac"];
		$global_rank_new30_credit = $row ["global_new30days_credit"];
		$global_rank_new30_rac = $row ["global_new30days_rac"];
		$global_rank_new90_credit = $row ["global_new90days_credit"];
		$global_rank_new90_rac = $row ["global_new90days_rac"];
		$global_rank_new365_credit = $row ["global_new365days_credit"];
		$global_rank_new365_rac = $row ["global_new365days_rac"];
		$global_rank_1project_credit = $row ["global_1project_credit"];
		$global_rank_1project_rac = $row ["global_1project_rac"];
		$global_rank_5project_credit = $row ["global_5project_credit"];
		$global_rank_5project_rac = $row ["global_5project_rac"];
		$global_rank_10project_credit = $row ["global_10project_credit"];
		$global_rank_10project_rac = $row ["global_10project_rac"];
		$global_rank_20project_credit = $row ["global_20project_credit"];
		$global_rank_20project_rac = $row ["global_20project_rac"];
		$global_rank_country_credit = $row ["global_country_credit"];
		$global_rank_country_rac = $row ["global_country_rac"];
		$global_rank_joinyear_credit = $row ["global_joinyear_credit"];
		$global_rank_joinyear_rac = $row ["global_joinyear_rac"];
	}
	
	// Print out user summary
	$global_total_credit = number_format ( $global_total_credit );
	$global_rac = number_format ( $global_rac );
	$data .= "<table style='width:100%' class='nforumholder' cellpadding='0' cellspacing='0'>\n";
	$data .= "<tr><td class='forumheader3' colspan=\"3\">" . LAN_233 . "</td></tr>\n";
	$data .= "<tr><td class='nforumthread'>" . LAN_234 . "</td><td colspan=\"2\" class='nforumthread'>$global_user_name</td></tr>\n";
	$data .= "<tr><td class='nforumthread'>" . LAN_235 . "</td><td colspan=\"2\" class='nforumthread'>$cpid</td></tr>\n";
	$data .= "<tr><td class='nforumthread'>" . LAN_236 . "</td><td colspan=\"2\" class='nforumthread'>$global_total_credit</td></tr>\n";
	$data .= "<tr><td class='nforumthread'>" . LAN_237 . "</td><td colspan=\"2\" class='nforumthread'>$global_rac</td></tr>\n";
	$data .= "<tr><td class='nforumthread'>" . LAN_203 . "</td><td colspan=\"2\" class='nforumthread'>$global_country</td></tr>\n";
	$dstr = date ( 'F d, Y', strtotime ( $global_join_date ) );
	$data .= "<tr><td class='nforumthread'>" . LAN_407 . "</td><td colspan=\"2\" class='nforumthread'>$dstr</td></tr>\n";
	$data .= "<tr><td colspan=\"3\" class='forumheader3'>&nbsp;</td></tr>";
	$data .= "<tr><td class='forumheader3' colspan=\"3\">" . LAN_238 . "</td></tr>\n";
	
	$fboinc_user_count = number_format ( $boinc_user_count );
	
	if ($global_rank_credit > 0) {
		$data .= "<tr><td class='nforumthread' >&nbsp;</td><td class='nforumthread'>" . LAN_239 . "</td><td class='nforumthread'>" . LAN_240 . "</td></tr>\n";
		$percent = "??";
		if ($boinc_user_count > 0) {
			$percent = 100 - (($global_rank_credit / $boinc_user_count) * 100);
			$percent = number_format ( $percent, 4 );
		}
		$start = $global_rank_credit - 10;
		if ($start < 1)
			$start = 1;
		
		$fwr_t = number_format ( $global_rank_credit );
		$data .= "<tr><td class='nforumthread'>" . LAN_241 . "</td><td class='nforumthread'><a href=\"utc_rank.php?list=global_credit&amp;start=$start&amp;count=20&amp;highlight=$global_rank_credit\">$fwr_t " . LAN_242 . " $fboinc_user_count</a><br/> ($percent " . LAN_243 . ")</td>\n";
		
		if ($global_rank_rac != 0) {
			$percent = "??";
			if ($boinc_user_count > 0) {
				$percent = 100 - (($global_rank_rac / $boinc_user_count) * 100);
				$percent = number_format ( $percent, 4 );
			}
			$fwr_e = number_format ( $global_rank_rac );
			$start = $global_rank_rac - 10;
			if ($start < 1)
				$start = 1;
			$data .= "<td class='nforumthread'><a href=\"urac_rank.php?list=global_rac&amp;start=$start&amp;count=20&amp;highlight=$global_rank_rac\">$fwr_e " . LAN_242 . " $fboinc_user_count</a><br/> ($percent " . LAN_243 . ")</td>";
		} else {
			$data .= "<td class='nforumthread'>&nbsp;</td>";
		}
		$data .= "</tr>\n";
	}
	if ($global_rank_country_credit > 0) {
		$start = $global_rank_country_credit - 10;
		if ($start < 1)
			$start = 1;
		$data .= "<tr><td class='nforumthread'>" . LAN_203 . " $global_country:</td><td class='nforumthread'><a href=\"utc_rank.php?list=global_country_credit&amp;opt1=country_id&amp;opt2=$global_countryid&amp;start=$start&amp;count=20&amp;highlight=$global_rank_country_credit\">" . number_format ( $global_rank_country_credit ) . "</a></td>";
		if ($global_rank_country_rac > 0) {
			$start = $global_rank_country_rac - 10;
			if ($start < 1)
				$start = 1;
			$data .= "<td class='nforumthread'><a href=\"urac_rank.php?list=global_country_rac&amp;start=$start&amp;count=20&amp;highlight=$global_rank_country_rac\">" . number_format ( $global_rank_country_rac ) . "</a></td></tr>";
		} else {
			$data .= "<td class='nforumthread'>&nbsp;</td></tr>";
		}
	}
	
	if ($global_rank_new30_credit > 0) {
		$start = $global_rank_new30_credit - 10;
		if ($start < 1)
			$start = 1;
		$data .= "<tr><td class='nforumthread'>" . LAN_400 . "</td><td class='nforumthread'><a href=\"utc_rank.php?list=global_new30days_credit&amp;start=$start&amp;count=20&amp;highlight=$global_rank_new30_credit\">" . number_format ( $global_rank_new30_credit ) . "</a></td>";
		if ($global_rank_new30_rac > 0) {
			$start = $global_rank_new30_rac - 10;
			if ($start < 1)
				$start = 1;
			$data .= "<td class='nforumthread'><a href=\"urac_rank.php?list=global_new30days_rac&amp;start=$start&amp;count=20&amp;highlight=$global_rank_new30_rac\">" . number_format ( $global_rank_new30_rac ) . "</a></td></tr>";
		} else {
			$data .= "<td class='nforumthread'>&nbsp;</td></tr>";
		}
	}
	
	if ($global_rank_new90_credit > 0) {
		$start = $global_rank_new90_credit - 10;
		if ($start < 1)
			$start = 1;
		$data .= "<tr><td class='nforumthread'>" . LAN_401 . "</td><td class='nforumthread'><a href=\"utc_rank.php?list=global_new90days_credit&amp;start=$start&amp;count=20&amp;highlight=$global_rank_credit\">" . number_format ( $global_rank_new90_credit ) . "</a></td>";
		if ($global_rank_new90_rac > 0) {
			$start = $global_rank_new90_rac - 10;
			if ($start < 1)
				$start = 1;
			$data .= "<td class='nforumthread'><a href=\"urac_rank.php?list=global_new90days_rac&amp;start=$start&amp;count=20&amp;highlight=$global_rank_new90_rac\">" . number_format ( $global_rank_new90_rac ) . "</a></td></tr>";
		} else {
			$data .= "<td class='nforumthread'>&nbsp;</td></tr>";
		}
	}
	if ($global_rank_new365_credit > 0) {
		$start = $global_rank_new365_credit - 10;
		if ($start < 1)
			$start = 1;
		$data .= "<tr><td class='nforumthread'>" . LAN_402 . "</td><td class='nforumthread'><a href=\"utc_rank.php?list=global_new365days_credit&amp;start=$start&amp;count=20&amp;highlight=$global_rank_new365_credit\">" . number_format ( $global_rank_new365_credit ) . "</a></td>";
		if ($global_rank_new365_rac > 0) {
			$start = $global_rank_new365_rac - 10;
			if ($start < 1)
				$start = 1;
			$data .= "<td class='nforumthread'><a href=\"urac_rank.php?list=global_new365days_rac&amp;start=$start&amp;count=20&amp;highlight=$global_rank_new365_rac\">" . number_format ( $global_rank_new365_rac ) . "</a></td></tr>";
		} else {
			$data .= "<td class='nforumthread'>&nbsp;</td></tr>";
		}
	}
	if ($global_rank_1project_credit > 0) {
		$start = $global_rank_1project_credit - 10;
		if ($start < 1)
			$start = 1;
		$data .= "<tr><td class='nforumthread'>" . LAN_403 . "</td><td class='nforumthread'><a href=\"utc_rank.php?list=global_1project_credit&amp;start=$start&amp;count=20&amp;highlight=$global_rank_1project_credit\">" . number_format ( $global_rank_1project_credit ) . "</a></td>";
		if ($global_rank_1project_rac > 0) {
			$start = $global_rank_1project_rac - 10;
			if ($start < 1)
				$start = 1;
			$data .= "<td class='nforumthread'><a href=\"urac_rank.php?list=global_1project_rac&amp;start=$start&amp;count=20&amp;highlight=$global_rank_1project_rac\">" . number_format ( $global_rank_1project_rac ) . "</a></td></tr>";
		} else {
			$data .= "<td class='nforumthread'>&nbsp;</td></tr>";
		}
	}
	if ($global_rank_5project_credit > 0) {
		$start = $global_rank_5project_credit - 10;
		if ($start < 1)
			$start = 1;
		$data .= "<tr><td class='nforumthread'>" . LAN_404 . "</td><td class='nforumthread'><a href=\"utc_rank.php?list=global_5project_credit&amp;start=$start&amp;count=20&amp;highlight=$global_rank_5project_credit\">" . number_format ( $global_rank_5project_credit ) . "</a></td>";
		if ($global_rank_5project_rac > 0) {
			$start = $global_rank_5project_rac - 10;
			if ($start < 1)
				$start = 1;
			$data .= "<td class='nforumthread'><a href=\"urac_rank.php?list=global_5project_rac&amp;start=$start&amp;count=20&amp;highlight=$global_rank_5project_rac\">" . number_format ( $global_rank_5project_rac ) . "</a></td></tr>";
		} else {
			$data .= "<td class='nforumthread'>&nbsp;</td></tr>";
		}
	}
	if ($global_rank_10project_credit > 0) {
		$start = $global_rank_10project_credit - 10;
		if ($start < 1)
			$start = 1;
		$data .= "<tr><td class='nforumthread'>" . LAN_405 . "</td><td class='nforumthread'><a href=\"utc_rank.php?list=global_10project_credit&amp;start=$start&amp;count=20&amp;highlight=$global_rank_10project_credit\">" . number_format ( $global_rank_10project_credit ) . "</a></td>";
		if ($global_rank_10project_rac > 0) {
			$start = $global_rank_10project_rac - 10;
			if ($start < 1)
				$start = 1;
			$data .= "<td class='nforumthread'><a href=\"urac_rank.php?list=global_10project_rac&amp;start=$start&amp;count=20&amp;highlight=$global_rank_10project_rac\">" . number_format ( $global_rank_10project_rac ) . "</a></td></tr>";
		} else {
			$data .= "<td class='nforumthread'>&nbsp;</td></tr>";
		}
	}
	if ($global_rank_20project_credit > 0) {
		$start = $global_rank_20project_credit - 10;
		if ($start < 1)
			$start = 1;
		$data .= "<tr><td class='nforumthread'>" . LAN_406 . "</td><td class='nforumthread'><a href=\"utc_rank.php?list=global_20project_credit&amp;start=$start&amp;count=20&amp;highlight=$global_rank_20project_credit\">" . number_format ( $global_rank_20project_credit ) . "</a></td>";
		if ($global_rank_20project_rac > 0) {
			$start = $global_rank_20project_rac - 10;
			if ($start < 1)
				$start = 1;
			$data .= "<td class='nforumthread'><a href=\"urac_rank.php?list=global_20project_rac&amp;start=$start&amp;count=20&amp;highlight=$global_rank_20project_rac\">" . number_format ( $global_rank_20project_rac ) . "</a></td></tr>";
		} else {
			$data .= "<td class='nforumthread'>&nbsp;</td></tr>";
		}
	}
	
	// show global graphs
	$data .= "<tr><td colspan=\"3\" class='forumheader3'>&nbsp;</td></tr>";
	$data .= "<tr><td colspan=\"3\" class='forumheader3'>" . LAN_244 . "</td></tr>\n";
	$data .= "<tr><td class='nforumthread'><img src=\"user_graph.php?cpid=$cpid&amp;type=1\" alt=\"" . LAN_245 . "\" /></td>\n";
	if ($global_rac > 0) {
		$data .= "<td class='nforumthread' colspan=\"2\"><img src=\"user_graph.php?cpid=$cpid&amp;type=2\" alt=\"" . LAN_245 . "\" /></td></tr>\n";
	} else {
		$data .= "<td class='nforumthread' colspan=\"2\">&nbsp;</td></tr>\n";
	}
	$data .= "<tr><td class='nforumthread' colspan=\"3\" style='text-align:center'><img src=\"user_graph.php?cpid=$cpid&amp;type=3\" alt=\"" . LAN_245 . "\" /></td></tr>\n";
	//$data .= "<tr><td class='nforumthread' colspan=\"3\" style='text-align:center'><img src=\"user_graph.php?cpid=$cpid&amp;type=7\" alt=\"".LAN_245."\" /></td></tr>\n";
	

	// show project details
	$data .= "<tr><td colspan=\"3\" class='forumheader3'>" . LAN_246 . "</td></tr>\n";
	
	$query = "select a.project_id, b.shortname, b.name, a.user_id, a.team_id, b.url, b.user_count from cpid_map a, projects b where a.project_id=b.project_id and a.user_cpid='" . $cpid . "' and b.shown='Y' and b.retired='N'";
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
		$project_usercount = $row ["user_count"];
		
		// fetch the information from the project tables
		$project_info .= "  <project>\n    <name>" . $project_name . "</name>\n    <project_id>" . $project_id . "</project_id>\n";
		$query = "select a.name, b.country, a.create_time, a.url, a.total_credit, a.rac, a.rac_time, a.project_rank_credit, a.project_rank_rac, a.team_rank_credit, a.team_rank_rac, a.computer_count from users_" . $project_shortname . " a, country b where a.country_id=b.country_id and a.user_id=" . $project_userid;
		$res_user = mysqli_query ( $dbhandle, $query );
		if (! $res_user) {
			echo "<b>Error performing query: " . mysqli_error ($dbhandle) . "</b>";
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
		}
		$data .= "<tr><td colspan=\"3\" class='forumheader3'>&nbsp;</td></tr>";
		$data .= "<tr><td colspan=\"2\"><b><a href=\"$project_url\">$project_name</a></b></td></tr>\n";
		if ($project_userurl == "") {
			$data .= " <tr><td class='nforumthread'>" . LAN_234 . "</td><td class='nforumthread' colspan=\"2\">$project_username</td></tr>\n";
		} else {
			$data .= " <tr><td class='nforumthread'>" . LAN_234 . "</td><td class='nforumthread' colspan=\"2\"><a href=\"http://$project_userurl\">$project_username</a></td></tr>\n";
		}
		$ptotal_credit = number_format ( $project_usertc );
		$pexpavg_credit = number_format ( $project_userrac );
		$data .= " <tr><td class='nforumthread'>" . LAN_236 . "</td><td class='nforumthread' colspan=\"2\">$ptotal_credit</td></tr>\n";
		$data .= " <tr><td class='nforumthread'>" . LAN_237 . "</td><td class='nforumthread' colspan=\"2\">$pexpavg_credit</td></tr>\n";
		
		if ($project_userrank_credit > 0) {
			$percent = "??";
			if ($project_usercount > 0) {
				$percent = 100 - (($project_userrank_credit / $project_usercount) * 100);
				$percent = number_format ( $percent, 4 );
			}
			if ($project_userrank_credit == 1)
				$percent = "100.0";
			$data .= " <tr><td class='nforumthread'>" . LAN_248 . "</td><td class='nforumthread' colspan=\"2\">$project_userrank_credit " . LAN_242 . " $project_usercount ($percent " . LAN_243 . ")</td></tr>\n";
		}
		if ($project_userrank_rac > 0) {
			$percent = "??";
			if ($project_usercount > 0) {
				$percent = 100 - (($project_userrank_rac / $project_usercount) * 100);
				$percent = number_format ( $percent, 4 );
			}
			if ($project_userrank_rac == 1)
				$percent = "100.0";
			$data .= " <tr><td class='nforumthread'>" . LAN_249 . "</td><td class='nforumthread' colspan=\"2\">$project_userrank_rac " . LAN_242 . " $project_usercount ($percent " . LAN_243 . ")</td></tr>\n";
		}
		$usrurl = $project_url . "show_user.php?userid=" . $project_userid;
		$data .= " <tr><td class='nforumthread'><a href=\"$usrurl\">" . LAN_250 . "</a></td><td class='nforumthread' colspan=\"2\"><a href=\"$usrurl\">$project_userid</a></td></tr>\n";
		$data .= " <tr><td class='nforumthread'>" . LAN_203 . ":</td><td class='nforumthread' colspan=\"2\">$project_country</td></tr>\n";
		
		if ($project_teamid && $project_teamid != 0) {
			// is a member of a team, get team info
			$query = "select team_id, name, url, nusers from teams_" . $project_shortname . " where team_id=" . $project_teamid;
			$res_team = mysqli_query ( $dbhandle, $query );
			if (! $res_team) {
				echo "<b>Error performing query: " . mysqli_error ( $dbhandle ) . "</b>";
				exit ();
			}
			while ( ($row_team = mysqli_fetch_array ( $res_team )) ) {
				$team_id = $row_team ["team_id"];
				$team_name = $row_team ["name"];
				$team_url = $row_team ["url"];
				$team_usercount = $row_team ["nusers"];
			}
			
			$data .= " <tr><td class='nforumthread'>" . LAN_202 . ":</td><td class='nforumthread' colspan=\"2\">$team_name ($team_id)</td></tr>\n";
			if ($project_userrank_teamcredit > 0) {
				$percent = "??";
				if ($team_usercount > 0) {
					$percent = 100 - (($project_userrank_teamcredit / $team_usercount) * 100);
					$percent = number_format ( $percent, 4 );
				}
				if ($project_userrank_teamcredit == 1)
					$percent = "100.0";
				$data .= " <tr><td class='nforumthread'>" . LAN_408 . " " . LAN_239 . "</td><td class='nforumthread' colspan=\"2\">$project_userrank_teamcredit " . LAN_242 . " $team_usercount ($percent " . LAN_243 . ")</td></tr>\n";
			}
			if ($project_userrank_teamrac > 0) {
				$percent = "??";
				if ($team_usercount > 0) {
					$percent = 100 - (($project_userrank_teamrac / $team_usercount) * 100);
					$percent = number_format ( $percent, 4 );
				}
				if ($project_userrank_teamrac == 1)
					$percent = "100.0";
				$data .= " <tr><td class='nforumthread'>" . LAN_408 . " " . LAN_240 . "</td><td class='nforumthread' colspan=\"2\">$project_userrank_teamrac " . LAN_242 . " $team_usercount ($percent " . LAN_243 . ")</td></tr>\n";
			}
		}
		if ($project_computercount > 0) {
			$data .= "<tr><td class='nforumthread'>" . LAN_251 . "</td><td class='nforumthread' colspan=\"2\">$project_computercount</td></tr>\n";
		} else {
			$data .= "<tr><td class='nforumthread'>" . LAN_251 . "</td><td class='nforumthread' colspan=\"2\">Unknown</td></tr>\n";
		}
		$dstr = date ( 'F d, Y', $project_createtime );
		$data .= "<tr><td class='nforumthread'>Member since:</td><td class='nforumthread' colspan=\"2\">$dstr</td></tr>\n";
		$data .= "<tr><td class='nforumthread' colspan=\"3\">" . LAN_252 . "<br/><img src=\"user_graph.php?cpid=$cpid&amp;type=4&amp;project=$project_id\" alt=\"" . LAN_252 . "\" /></td></tr>\n";
		//$data .= "<tr><td class='nforumthread' colspan=\"3\">".LAN_253."<br/><img src=\"user_graph.php?cpid=$cpid&amp;type=8&amp;project=$project_id\" alt=\"".LAN_253."\" /></td></tr>\n";
	}
}
$data .= "</table>\n";

// Kick out the display to e107 engine
require_once (HEADERF);
$ns->tablerender ( $tname, $data );
require_once (FOOTERF);

?>
