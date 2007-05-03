<?php

// Remember that we must include class2.php
require_once("../../class2.php");

require_once("bp_functions.php");
//@include_once(e_PLUGIN."bp/languages/".e_LANGUAGE.".php");
//@include_once(e_PLUGIN."bp/languages/English.php");

require_once(HEADERF);

$bp_title = "Summary of projects";
 
$text = display_project_stats(1);

$ns->tablerender($bp_title,$text);

require_once(FOOTERF);

?>

