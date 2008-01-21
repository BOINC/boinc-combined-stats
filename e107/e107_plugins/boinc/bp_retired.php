<?php

// Remember that we must include class2.php
require_once("../../class2.php");

require_once("bp_functions.php");
include_lan(e_PLUGIN.'boinc/languages/'.e_LANGUAGE.'/lan_boinc.php');

require_once(HEADERF);

$bp_title = LAN_120;
 
$text = display_project_stats(0);

$ns->tablerender($bp_title,$text);

require_once(FOOTERF);

?>

