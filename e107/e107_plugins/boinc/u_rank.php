<?php
require_once("../../class2.php");
require_once("bp_functions.php");
include_lan(e_PLUGIN.'boinc/languages/'.e_LANGUAGE.'/lan_boinc.php');

$list = $_GET['list'];
$start = $_GET['start'];
$count = $_GET['count'];

require_once(HEADERF);

  if ($count == "") $count=100;
  if ($start == "") $start=1;

  if ($list == "")
  {
     // display list of what we can do
     $data = display_rankings();
  }
  else 
  {
     if ($list == "rac_p0c0") 
        $tname = LAN_160;
     if ($list == "tc_p0c0")
        $tname = LAN_161;
     if ($list == "tc_p1c1")
        $tname = LAN_162;
     if ($list == "rac_p1c1")
        $tname = LAN_163;
     if ($list == "tc_p1c5")
        $tname = LAN_164;
     if ($list == "rac_p1c5")
        $tname = LAN_165;
     if ($list == "tc_p1c10")
        $tname = LAN_166;
     if ($list == "rac_p1c10")
        $tname = LAN_167;
     if ($list == "tc_p1c20")
        $tname = LAN_168;
     if ($list == "rac_p1c20")
        $tname = LAN_169;
     if ($list == "tc_p2c0")
        $tname = LAN_170;
     if ($list == "rac_p2c0")
        $tname = LAN_171;
     if ($list == "tc_p2c1")
        $tname = LAN_172;
     if ($list == "tc_p2c5")
        $tname = LAN_173;
     if ($list == "tc_p2c10")
        $tname = LAN_174;
     if ($list == "tc_p2c20")
        $tname = LAN_175;
     if ($list == "tc_p5c0")
        $tname = LAN_176;
     if ($list == "tc_p5c1")
        $tname = LAN_177;
     if ($list == "tc_p5c5")
        $tname = LAN_178;
     if ($list == "tc_p5c10")
        $tname = LAN_179;
     if ($list == "tc_p5c20")
        $tname = LAN_180;
     if ($list == "rac_p2c1")
        $tname = LAN_181;
     if ($list == "rac_p2c5")
        $tname = LAN_182;
     if ($list == "rac_p2c10")
        $tname = LAN_183;
     if ($list == "rac_p2c20")
        $tname = LAN_184;
     if ($list == "rac_p5c0")
        $tname = LAN_185;
     if ($list == "rac_p5c1")
        $tname = LAN_186;
     if ($list == "rac_p5c5")
        $tname = LAN_187;
     if ($list == "rac_p5c10")
        $tname = LAN_188;
     if ($list == "rac_p5c20")
        $tname = LAN_189;


     $data = display_users($start,$start+$count,$list);
  }

  mysql_select_db($db_e107);

  require_once(HEADERF);
  $ns->tablerender($tname, $data);
  require_once(FOOTERF);

?>

