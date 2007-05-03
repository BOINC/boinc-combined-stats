<?php
require_once("../../class2.php");
require_once("bp_functions.php");

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
        $tname = "Users currently doing the most work";
     if ($list == "tc_p0c0")
        $tname = "Users who have done the most work";
     if ($list == "tc_p1c1")
        $tname = "Users who have done the most work on a single computer";
     if ($list == "rac_p1c1")
        $tname = "Users who are currently doing the most work on a single computer";
     if ($list == "tc_p1c5")
        $tname = "Users who have done the most work on a 5 or fewer computers";
     if ($list == "rac_p1c5")
        $tname = "Users who are currently doing the most work on 5 or fewer computers";
     if ($list == "tc_p1c10")
        $tname = "Users who have done the most work on a 10 or fewer computers";
     if ($list == "rac_p1c10")
        $tname = "Users who are currently doing the most work on 10 or fewer computers";
     if ($list == "tc_p1c20")
        $tname = "Users who have done the most work on 20 or fewer computers";
     if ($list == "rac_p1c20")
        $tname = "Users who are currently doing the most work on 20 or fewer computers";
     if ($list == "tc_p2c0")
        $tname = "Users who have done the most work and actively participating in 2 or more projects";
     if ($list == "rac_p2c0")
        $tname = "Users who are currently doing the most work and are actively participating in 2 or more projects";

     $data = display_users($start,$start+$count,$list);
  }

  mysql_select_db($db_e107);

  require_once(HEADERF);
  $ns->tablerender($tname, $data);
  require_once(FOOTERF);

?>

