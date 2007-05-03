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
|     $Source: /cvsroot/e107/e107_0.7/e107_themes/khatru/forum_viewtopic_template.php,v $
|     $Revision: 1.3 $
|     $Date: 2005/12/14 19:28:52 $
|     $Author: sweetas $
+----------------------------------------------------------------------------+
*/

if (!defined('e107_INIT')) { exit; }

$icon = (file_exists(THEME."forum/e.png") ? THEME."forum/e.png" : e_PLUGIN."forum/images/lite/e.png");


$FORUMSTART =
BOXOPEN."{BACKLINK}".BOXMAIN."

<table cellspacing='0' cellpadding='0' style='width:100%'>
<tr>
<td class='smalltext'>{NEXTPREV}</td>
<td style='text-align:right'>&nbsp;{TRACK}&nbsp;</td>
</tr>
</table>

<table style='width:100%'>
<tr>
<td style='width:60%; vertical-align:middle;'><div class='newheadline'><img src='".$icon."' style='vertical-align:middle' alt=''/> <b>{THREADNAME}</b></div><br />{GOTOPAGES}</td>
<td style='width:40%; vertical-align:middle; text-align:right;'>{BUTTONS}</td>
</tr>
</table>


<table style='width:100%' class='nforumholder' cellpadding='0' cellspacing='0'>
<tr>
<td style='width:20%; text-align:center' class='nforumcaption2'>\n".LAN_402."\n</td>\n<td style='width:80%; text-align:center' class='nforumcaption2'>\n".LAN_403."\n</td>
</tr>
</table>


";

$FORUMEND = "

<div class='spacer'>
{GOTOPAGES}
</div>
<table style='width:100%' cellpadding='0' cellspacing='0'>
<tr>
<td style='width:50%; text-align:left; vertical-align:top'><b>{MODERATORS}</b><br />{FORUMJUMP}</td>
<td style='width:50%; text-align:right; vertical-align:top'>{BUTTONS}</td>
</tr>
</table>
</div>
<div class='spacer'>

<div style='text-align:center'>{QUICKREPLY}</div>


<div style='text-align:center;'>
<a href='".e_PLUGIN."rss_menu/rss.php?8.1.".e_QUERY."'><img src='".e_PLUGIN."rss_menu/images/rss1.png' alt='".LAN_431."' style='vertical-align: middle; border: 0;' /></a> 
<a href='".e_PLUGIN."rss_menu/rss.php?8.2.".e_QUERY."'><img src='".e_PLUGIN."rss_menu/images/rss2.png' alt='".LAN_432."' style='vertical-align: middle; border: 0;' /></a> 
<a href='".e_PLUGIN."rss_menu/rss.php?8.3.".e_QUERY."'><img src='".e_PLUGIN."rss_menu/images/rss3.png' alt='".LAN_433."' style='vertical-align: middle; border: 0;' /></a>
</div>

<div class='nforumdisclaimer' style='text-align:center'>Powered by <b>e107 Forum System</b></div>
".BOXCLOSE;









$FORUMTHREADSTYLE = "
<div class='spacer'>
<table style='width:100%' class='nforumholder' cellpadding='0' cellspacing='0'>
<tr>
<td class='nforumreplycaption' style='vertical-align:middle; width:20%;'>\n{NEWFLAG}\n{POSTER}\n</td>
<td class='nforumreplycaption' style='vertical-align:middle; width:80%;'>
<table cellspacing='0' cellpadding='0' style='width:100%'>
<tr>
<td class='smallblacktext'>\n{THREADDATESTAMP}\n</td>
<td style='text-align:right'>\n{REPORTIMG}\n{EDITIMG}\n{QUOTEIMG}\n</td>
</tr>
</table>
</td>
</tr>	
<tr>
<td class='nforumreply2' style='vertical-align:top'>\n{AVATAR}\n<span class='smalltext'>\n{LEVEL}\n{MEMBERID}\n{JOINED}\n{POSTS}\n</span>\n</td>
<td class='nforumreply2' style='vertical-align:top'>{POLL}\n{POST}\n{SIGNATURE}\n</td>
</tr>		
<tr>
<td class='nforumreplycaption'>\n<span class='smallblacktext'>\n{TOP}\n</span>\n</td>
<td class='nforumreplycaption' style='vertical-align:top'>
<table cellspacing='0' cellpadding='0' style='width:100%'>
<tr>
<td>\n{PROFILEIMG}\n {EMAILIMG}\n {WEBSITEIMG}\n {PRIVMESSAGE}\n</td>
<td style='text-align:right'>\n{MODOPTIONS}\n</td>
</tr>
</table>
</td>
</tr>	
</table>
</div>";


$FORUMREPLYSTYLE = "
<div class='spacer'>
<table style='width:100%' class='nforumholder' cellpadding='0' cellspacing='0'>
<tr>
<td class='nforumreplycaption' style='vertical-align:middle; width:20%;'>\n{NEWFLAG}\n{POSTER}\n</td>
<td class='nforumreplycaption' style='vertical-align:middle; width:80%;'>
<table cellspacing='0' cellpadding='0' style='width:100%'>
<tr>
<td class='smallblacktext'>\n{THREADDATESTAMP}\n</td>
<td style='text-align:right'>\n{REPORTIMG}\n{EDITIMG}\n{QUOTEIMG}\n</td>
</tr>
</table>
</td>
</tr>	
<tr>
<td class='nforumreply2' style='vertical-align:top'>\n{AVATAR}\n<span class='smalltext'>\n{LEVEL}\n{MEMBERID}\n{JOINED}\n{POSTS}\n</span>\n</td>
<td class='nforumreply2' style='vertical-align:top'>{POST}\n{SIGNATURE}</td>
</tr>		
<tr>
<td class='nforumreplycaption'>\n<span class='smallblacktext'>\n{TOP}\n</span>\n</td>
<td class='nforumreplycaption' style='vertical-align:top'>
<table cellspacing='0' cellpadding='0' style='width:100%'>
<tr>
<td>\n{PROFILEIMG}\n {EMAILIMG}\n {WEBSITEIMG}\n {PRIVMESSAGE}\n</td>
<td style='text-align:right'>\n{MODOPTIONS}\n</td>
</tr>
</table>
</td>
</tr>		
</table>
</div>";

$FORUMREPLYSTYLE = "
<div class='spacer'>
<table style='width:100%' class='nforumholder' cellpadding='0' cellspacing='0'>
<tr>
<td class='nforumcaption3' style='vertical-align:middle; width:20%;'>\n{NEWFLAG}\n{POSTER}\n{ANON_IP}</td>
<td class='nforumcaption3' style='vertical-align:middle; width:80%;'>
<table cellspacing='0' cellpadding='0' style='width:100%'>
<tr>
<td class='smallblacktext'>\n{THREADDATESTAMP}\n</td>
<td style='text-align:right'>\n{EMAILITEM} {PRINTITEM} {REPORTIMG}\n{EDITIMG}\n{QUOTEIMG}\n</td>
</tr>
</table>
</td>
</tr>
<tr>
<td class='nforumthread' style='vertical-align:top'>\n{AVATAR}\n<span class='smalltext'>\n{CUSTOMTITLE}\n{LEVEL}\n{MEMBERID}\n{JOINED}\n{USER_EXTENDED=location.text_value}\n{POSTS}\n</span>\n</td>
<td class='nforumthread' style='vertical-align:top'>\n{POST}\n{SIGNATURE}\n</td>
</tr>
<tr>
<td class='nforumthread2'>\n<span class='smallblacktext'>\n{TOP}\n</span>\n</td>
<td class='nforumthread2' style='vertical-align:top'>
<table cellspacing='0' cellpadding='0' style='width:100%'>
<tr>
<td>\n{PROFILEIMG}\n {EMAILIMG}\n {WEBSITEIMG}\n {PRIVMESSAGE}\n</td>
<td style='text-align:right'>\n{MODOPTIONS}\n</td>
</tr>
</table>
</td>
</tr>
</table>
</div>";

?>
