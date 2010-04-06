<?php
require_once("../../class2.php");
require_once("bp_functions.php");
include_lan(e_PLUGIN.'boinc/languages/'.e_LANGUAGE.'/lan_boinc.php');

$list = $_GET['list'];
$start = $_GET['start'];
$count = $_GET['count'];
$opt1 = $_GET['opt1'];
$opt2 = $_GET['opt2'];
$highlight = $_GET['highlight'];
if (!isset($highlight))
$highlight = 0;
 
require_once(HEADERF);

if ($count == "") $count=100;
if ($start == "") $start=1;

if ($list == "")
{
   // display list of what we can do
   $data = display_rankings();
} else  {
	if ($list == "global_rac") 
	   $tname = LAN_160;
	if ($list == "global_credit")
	   $tname = LAN_161;
	if ($list == "global_new30days_credit")
	   $tname = LAN_199a." ".LAN_190;
	if ($list == "global_new30days_rac")
	   $tname = LAN_199b." ".LAN_190;
	if ($list == "global_new90days_credit")
	   $tname = LAN_199a." ".LAN_191;
	if ($list == "global_new_90days_rac")
	   $tname = LAN_199b." ".LAN_191;
	if ($list == "global_new365days_credit")
	   $tname = LAN_199a." ".LAN_192;
	if ($list == "global_new365days_rac")
	   $tname = LAN_199b." ".LAN_192;
	if ($list == "global_1project_credit")
	   $tname = LAN_199a." ".LAN_193;
	if ($list == "global_1project_rac")
	   $tname = LAN_199b." ".LAN_193;
	if ($list == "global_5project_credit")
	   $tname = LAN_199a." ".LAN_194;
	if ($list == "global_5project_rac")
	   $tname = LAN_199b." ".LAN_194;
	if ($list == "global_10project_credit")
	   $tname = LAN_199a." ".LAN_195;
	if ($list == "global_10project_rac")
	   $tname = LAN_199b." ".LAN_195;
	if ($list == "global_20project_credit")
	   $tname = LAN_199a." ".LAN_196;
	if ($list == "global_20project_rac")
	   $tname = LAN_199b." ".LAN_196;
	if ($list == "global_joinyear_credit")
	   $tname = LAN_199a." ".LAN_197 ." ". $opt2;
	if ($list == "global_joinyear_rac")
	   $tname = LAN_197 ." ". $opt2;
	if ($list == "global_country_credit")
	   $tname = LAN_199a." ".LAN_198. " ".get_country_name($opt2);
	if ($list == "global_country_rac")
	   $tname = LAN_198 . " ".get_country_name($opt2);
	
	
	$data = display_users_ranking("rank_user_rac",$start,$start+$count,$list,$opt1,$opt2,$highlight,false);
}

global $dbe107;
mysql_select_db($dbe107);

require_once(HEADERF);
$ns->tablerender($tname, $data);
require_once(FOOTERF);

?>
