<?php
/*
+ ----------------------------------------------------------------------------+
|     e107 website system
|
|     �Steve Dunstan 2001-2002
|     http://e107.org
|     jalist@e107.org
|
|     Released under the terms and conditions of the
|     GNU General Public License (http://gnu.org).
|
|     $Source: /cvsroot/e107/e107_0.7/e107_themes/khatru/forum_template.php,v $
|     $Revision: 1.3 $
|     $Date: 2005/12/14 19:28:52 $
|     $Author: sweetas $
+----------------------------------------------------------------------------+
*/
if (!defined('e107_INIT')) { exit; }

$FORUM_MAIN_START = "";

$FORUM_MAIN_PARENT = 
BOXOPEN."<b>{PARENTNAME} {PARENTSTATUS}</b>".BOXMAIN."
<table style='width:100%' class='nforumholder' cellpadding='0' cellspacing='0'>
<tr>
<td colspan='2' style='width:60%; text-align:center' class='nforumcaption2'>{FORUMTITLE}</td>
<td style='width:10%; text-align:center' class='nforumcaption2'>{THREADTITLE}</td>
<td style='width:10%; text-align:center' class='nforumcaption2'>{REPLYTITLE}</td>
<td style='width:20%; text-align:center' class='nforumcaption2'>{LASTPOSTITLE}</td>
</tr>
";


$FORUM_MAIN_PARENT_END = "
</table>".BOXCLOSE;

$FORUM_MAIN_FORUM = "<tr>\n<td style='width:5%; text-align:center' class='nforumcaption3'>{NEWFLAG}</td>\n<td style='width:55%' class='nforumcaption3'>{FORUMNAME}<br /><span class='smallblacktext'>{FORUMDESCRIPTION}</span>{FORUMSUBFORUMS}</td>\n<td style='width:10%; text-align:center' class='nforumthread'>{THREADS}</td>\n<td style='width:10%; text-align:center' class='nforumthread'>{REPLIES}</td>\n<td style='width:20%; text-align:center' class='nforumthread'><span class='smallblacktext'>{LASTPOST}</span></td>\n</tr>";


$FORUM_MAIN_END = 
BOXOPEN."<b>{INFOTITLE}</b>".BOXMAIN."

<table style='width:100%' class='nforumholder' cellpadding='0' cellspacing='0'>\n<tr>\n<td rowspan='4' style='width:5%; text-align:center' class='nforumthread'>{LOGO}</td>\n<td style='width:auto' class='nforumthread'>{USERINFO}</td>\n</tr>\n<tr>\n<td style='width:95%' class='nforumthread'>{INFO}</td>\n</tr><tr>\n<td style='width:95%' class='nforumthread'>{FORUMINFO}</td>\n</tr>\n<tr>\n<td style='width:95%' class='nforumthread'>{USERLIST}<br />{STATLINK}</td>\n</tr>\n</table>\n</div>\n<div class='spacer'>\n<table class='nforumholder' style='width:98%' cellpadding='0' cellspacing='0'>\n<tr>\n<td class='nforumthread' style='text-align:center; width:33%'>{ICONKEY}</td>\n<td style='text-align:center; width:33%' class='nforumthread'>{SEARCH}</td>\n<td style='width:33%; text-align:center; vertical-align:middle' class='nforumthread'><span class='smallblacktext'>{PERMS}</span>\n</td>\n</tr>\n</table><div class='nforumdisclaimer' style='text-align:center'>Powered by <b>e107 Forum System</b></div>".BOXCLOSE;


$FORUM_NEWPOSTS_START = "<div style='text-align:center'>\n<div class='spacer'>\n<table style='width:95%' class='fborder'>\n<tr>\n<td style='width:3%' class='fcaption'>&nbsp;</td>\n<td style='width:60%' class='fcaption'>{NEWTHREADTITLE}</td>\n<td style='width:27%; text-align:center' class='fcaption'>{POSTEDTITLE}</td>\n</tr>";



$FORUM_NEWPOSTS_MAIN .= "<tr>\n<td style='width:3%' class='forumheader3'>{NEWIMAGE}</td>\n<td style='width:60%' class='forumheader3'>{NEWSPOSTNAME}</td>\n<td style='width:27%; text-align:center' class='forumheader3'>{STARTERTITLE}</td>\n</tr>";
 


$FORUM_NEWPOSTS_END .= "</table></div></div>";
 


$FORUM_TRACK_START = "<div style='text-align:center'>\n<div class='spacer'>\n<table style='width:95%' class='fborder'>\n<tr>\n<td colspan='3' style='width:60%' class='fcaption'>{TRACKTITLE}</td>\n</tr>\n";


$FORUM_TRACK_MAIN = "<tr>
<td style='text-align:center; vertical-align:middle; width:6%'  class='forumheader3'>{NEWIMAGE}</td>
<td style='vertical-align:middle; text-align:left; width:70%'  class='forumheader3'><span class='mediumtext'>{TRACKPOSTNAME}</span></td>
<td style='vertical-align:middle; text-align:center; width:24%'  class='forumheader3'><span class='mediumtext'>{UNTRACK}</td>
</tr>";
                


$FORUM_TRACK_END = "</table>\n</div>\n</div>";



?>