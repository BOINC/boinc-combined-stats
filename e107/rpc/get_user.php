<?php
$cpid = $_GET["cpid"];
$html = isset($_GET["html"]) ? $_GET["html"] : '';

function escape_xml_special_chars ($text)
{
	// first unescape them in the case where they may 
	// already be escaped - don't want to do it twice
        $text = str_replace("&lt;", "<", $text);
        $text = str_replace("&gt;", ">", $text);
        $text = str_replace("&apos;", "\'", $text); 
	$text = str_replace("&amp;", "&", $text);

	// then escape them
	$text = str_replace("&", "&amp;", $text);
        $text = str_replace("<", "&lt;", $text);
        $text = str_replace(">", "&gt;", $text);
        $text = str_replace("\'", "&apos;", $text); 
        return $text;
}

$rc = 0;

if ($cpid=="")
{
   print "<error>cpid not specified</error>\n";
   exit;
}

if ($html=="")
{
   header('Content-type: text/xml');
   print "<?xml version=\"1.0\" encoding=\"iso-8859-1\"?>\n";
} else {
   $head = "Location: http://boinc.netsoft-online.com/e107/e107_plugins/boinc/get_user.php?cpid=$cpid";
   header($head);

   print "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0 Transitional//EN\" \"http://www.w3.org/TR/REC-html40/loose.dtd\">\n";
   print "<html><head><title>User Stats Detail</title>\n";
   print "</head>\n<body>\n";
   print "This page has moved to <a href=\"http://boinc.netsoft-online.com/e107/e107_plugins/boinc/get_user.php?cpid=$cpid\">here</a>";
   print "</body></html>";
   exit;
}

// connect to dtabase
include ('/home/virtual/netsoft-online.com/home/boinc/dbconnect.php');

$connect=mysql_connect($dbhost,$dblogin,$dbpassword);
mysql_select_db($dbname);

if ($connect != 0)
{

   // get the overal boinc totals (now that I have them easily)
   $query = "SELECT user_count,total_credit from b_projects where project_id=19";

   $res = mysql_query($query);

   if (!$res) {
     echo "<b>Error performing query: " . mysql_error() . "</b>";
     exit();
   }

   while ($row = mysql_fetch_array($res) ) 
   {
      $boinc_user_count=$row["user_count"];
      $boinc_total_credit=$row["total_credit"];

   }


   $query = "SELECT a.user_cpid, a.total_credit, a.rac,a.rac_time, b.name, b.project_id, ".
         "b.user_id, b_country.country, b.create_time, b.url, b.teamid, b.project_rank_credit,b.project_rank_rac, ".
         "b.total_credit as ptotal_credit, b.rac as pexpavg_credit, b.rac_time as pexpavg_time, ".
         "d.name as pname, d.url as purl, b_teams.name as tname, b.computer_count as count, ".
         "d.user_count,d.total_credit as ptotal, b_rank_user_tc_p0c0.rank as r_total, ".
         "b_rank_user_rac_p0c0.rank as r_expavg ".
         "FROM b_cpid a, b_users b, b_projects d ".
         "LEFT JOIN b_teams ON b.b_team_id=b_teams.table_id ".
         "LEFT JOIN b_country ON b.country_id=b_country.country_id ".
         "LEFT JOIN b_rank_user_tc_p0c0 ON a.table_id=b_rank_user_tc_p0c0.b_cpid_id ".
         "LEFT JOIN b_rank_user_rac_p0c0 ON a.table_id=b_rank_user_rac_p0c0.b_cpid_id ".
         "WHERE a.user_cpid = '$cpid' and a.table_id=b.b_cpid_id  ".
         "AND b.project_id=d.project_id";

   $res = mysql_query($query);

   if (!$res) {
     echo "<b>Error performing query: " . mysql_error() . "</b>";
     exit();
   }

   $i=0;

   while ($row = mysql_fetch_array($res) ) 
   {
      $user_cpid=$row["user_cpid"];
      $total_credit=$row["total_credit"];
      $expavg_credit=$row["rac"];
      $expavg_time=$row["rac_time"];
      $name=$row["name"];
      $project_id=$row["project_id"];
      $user_id=$row["user_id"];
      $country=$row["country"];
      $create_time=$row["create_time"];
      $url=$row["url"];
      $teamid=$row["teamid"];
      $ptotal_credit=$row["ptotal_credit"];
      $pexpavg_credit=$row["pexpavg_credit"];
      $pexpavg_time=$row["pexpavg_time"];
      $pname=$row["pname"];
      $purl=$row["purl"];
      $tname=$row["tname"];
      $count=$row["count"];
      $wr_t=$row["r_total"];
      $wr_e=$row["r_expavg"];
      $pr_t=$row["project_rank_credit"];
      $pr_e=$row["project_rank_rac"];
      $puser_count=$row["user_count"];
      $ptotal=$row["ptotal"];
      //$r_one_t=$row["r_one_t"];
      //$r_one_e=$row["r_one_e"];
      //$r_five_t=$row["r_five_t"];
      //$r_five_e=$row["r_five_e"];
      //$r_ten_t=$row["r_ten_t"];
      //$r_ten_e=$row["r_ten_e"];
      //$r_twenty_t=$row["r_twenty_t"];
      //$r_twenty_e=$row["r_twenty_e"];
      //$r_p_t=$row["r_p_t"];
      //$r_p_e=$row["r_p_e"];
      //$r_p5_t=$row["r_p5_t"];
      //$r_p5_e=$row["r_p5_e"];
      //$r_p5_1_t=$row["r_p5_1_t"];
      //$r_p5_1_e=$row["r_p5_1_e"];
      //$r_p5_5_t=$row["r_p5_5_t"];
      //$r_p5_5_e=$row["r_p5_5_e"];

      if ($i==0) {
         if ($html=="") {
            print "<user>\n";
            print "  <total_credit>$total_credit</total_credit>\n";
            print "  <expavg_credit>$expavg_credit</expavg_credit>\n";
            print "  <expavg_time>$expavg_time</expavg_time>\n";
            print "  <cpid>$user_cpid</cpid>\n";
	    $name = escape_xml_special_chars($name);
            print "  <name>$name</name>\n";
            if ($wr_t <> 0)
            {
               print "  <world_rank_total_credit>$wr_t</world_rank_total_credit>\n";
            }
            if ($wr_e <> 0)
            {
               print "  <world_rank_expavg_credit>$wr_e</world_rank_expavg_credit>\n";
            } 
         } else {
            $total_credit = number_format($total_credit,2);
            $expavg_credit = number_format($expavg_credit,2);
            //print "<h2>User Detailed Information</h2>\n";
            print "<table class=\"forum\">\n";
            print "<tr><th colspan=2>User Summary</th></tr>\n";
            print "<tr><td class=\"unsorted\">Name:</td><td class=\"unsorted\">$name</td></tr>\n";
            print "<tr><td class=\"unsorted\">User Cross Project ID:</td><td class=\"unsorted\">$user_cpid</td></tr>\n";
            print "<tr><td class=\"unsorted\">Total Credit:</td><td class=\"unsorted\">$total_credit</td></tr>\n";
            print "<tr><td class=\"unsorted\">Recent Average Credit:</td><td class=\"unsorted\">$expavg_credit</td</tr>\n";
            print "</table>\n<p>\n<table class=\"forum\"><tr><th colspan=3>Rankings</th></tr>\n";
            print "<tr><th>&nbsp;</th><th>by Total Credit</th><th>by Recent Average Credit</th>\n";
            $fboinc_user_count = number_format($boinc_user_count);
            if ($wr_t <> 0)
            {
              $percent = "??";
              if ($boinc_user_count > 0) { 
                $percent = 100 - (($wr_t / $boinc_user_count) * 100);
                $percent = number_format($percent,4); 
              }
              $fwr_t = number_format($wr_t);
               print "<tr><td class=\"unsorted\">World Rank</td><td class=\"unsorted\"><a href=\"rankings.php?list=r_total&amp;start=$wr_t\">$fwr_t out of $fboinc_user_count</a><br> ($percent percentile)</td>\n";
             
              if ($wr_e <> 0)
              {
                 $percent = "??";
                 if ($boinc_user_count > 0) { 
                  $percent = 100 - (($wr_e / $boinc_user_count) * 100);
                  $percent = number_format($percent,4); 
                 }
                 $fwr_e = number_format($wr_e);
                 print "<td class=\"unsorted\"><a href=\"rankings.php?list=r_expavg&amp;start=$wr_e\">$fwr_e out of $fboinc_user_count</a><br> ($percent percentile)</td>";
              }
              else print "<td class=\"unsorted\">&nbsp;</td>";
       	    
              print "</tr>\n";
            }

            //if ($r_one_t <> 0)
            //{
            //   $fr_one_t = number_format($r_one_t);
            //   print "<tr><td class=\"unsorted\">Using a single computer</td><td class=\"unsorted\"><a href=\"rankings.php?list=r_one_t&amp;start=$r_one_t\">$fr_one_t</a></td>\n";
            // 
            //
            //   if ($r_one_e <> 0)
            //   {
            //      $fr_one_e = number_format($r_one_e);
            //      print "<td class=\"unsorted\"><a href=\"rankings.php?list=r_one_e&amp;start=$r_one_e\">$fr_one_e</a></td>\n";
            //   }
            //   else print "<td class=\"unsorted\">&nbsp;</td>";
            //   print "</tr>\n";
            //}

            //if ($r_five_t <> 0)
            //{
            //   $fr_five_t = number_format($r_five_t);
            //   print "<tr><td class=\"unsorted\">Using five or fewer computers</td><td class=\"unsorted\"><a href=\"rankings.php?list=r_five_t&amp;start=$r_five_t\">$fr_five_t</a></td>\n";
            //
            //   if ($r_five_e <> 0)
            //   {
            //      $fr_five_e = number_format($r_five_e);
            //      print "<td class=\"unsorted\"><a href=\"rankings.php?list=r_five_e&amp;start=$r_five_e\">$fr_five_e</a></td>\n";
            //   }
            //   else print "<td class=\"unsorted\">&nbsp;</td>";
            //   print "</tr>\n";
            //}
            //print "</table>\n";
            print "<p><table class=\"forum\">\n";
            print "<tr><th colspan=2>Total Credit Graphs</th></tr>\n";
            print "<tr><td><img src=\"user_graph.php?cpid=$cpid&amp;type=1\" alt=\"stats graph\"></td>\n";
            if ($expavg_credit > 0) {
                print "<td><img src=\"user_graph.php?cpid=$cpid&amp;type=2\" alt=\"stats graph\"></td></tr>\n";
            } else {
                print "<td>&nbsp;</td></tr>\n";
            }
            print "<tr><td colspan=2><center><img src=\"user_graph.php?cpid=$cpid&amp;type=3\" alt=\"stats graph\"></center></td></tr>\n";
            print "</table>\n";
            
            //print "<tr><td colspan=2><h3>Participating in Projects</h3></td></tr>\n";
            print "<h3>User Projects</h3>\n";
         }

         $i++;
      }  
      if ($html=="") 
      {
         print "      <project>\n";
	 $pname = escape_xml_special_chars($pname);
         print "         <name>$pname</name>\n";
         print "         <project_id>$project_id</project_id>\n";
         print "         <url>$purl</url>\n";
         print "         <total_credit>$ptotal_credit</total_credit>\n";
         print "         <expavg_credit>$pexpavg_credit</expavg_credit>\n";
         if ($pr_t <> "") { print "         <project_rank_total_credit>$pr_t</project_rank_total_credit>\n"; }
         if ($pr_e <> "") { print "         <project_rank_expavg_credit>$pr_e</project_rank_expavg_credit>\n"; }
         print "         <id>$user_id</id>\n";
         print "         <create_time>$create_time</create_time>\n";
         print "         <country>$country</country>\n";
         print "         <user_name>$name</user_name>\n";
         if ($url <> "") {
             print "         <user_url>$url</user_url>\n";
         }
         if ($teamid > 0) {
            print "         <team_id>$teamid</team_id>\n";
	    $tname = escape_xml_special_chars($tname);
            print "         <team_name>$tname</team_name>\n";
         }
         if ($count > 0) {
            print "        <computer_count>$count</computer_count>\n";
         }
         print "      </project>\n";
      } else {
         print "<table class=\"forum\">\n";
         print "<tr><th colspan=2><b><a href=\"$purl\" style=\"color: #FFFFFF\">$pname</a></b</th></tr>\n";
         if ($url=="") 
         {
            print " <tr><td class=\"unsorted\">Name:</td><td class=\"unsorted\">$name</td></tr>\n";
         } else {
            print " <tr><td class=\"unsorted\">Name:</td><td class=\"unsorted\"><a href=\"http://$url\">$name</a></td></tr>\n";
         }
         $ptotal_credit = number_format($ptotal_credit,2);
         $pexpavg_credit = number_format($pexpavg_credit,2);
         print " <tr><td class=\"unsorted\">Total Credit:</td><td class=\"unsorted\">$ptotal_credit</td></tr>\n";
         print " <tr><td class=\"unsorted\">Recent Average Credit:</td><td class=\"unsorted\">$pexpavg_credit</td></tr>\n";
         if ($pr_t <> "") {
             $percent = "??";
             if ($puser_count > 0) { 
                $percent = 100 - (($pr_t / $puser_count) * 100);
                $percent = number_format($percent,4); 
             }
             print " <tr><td class=\"unsorted\">Project Rank Total Credit:</td><td class=\"unsorted\">$pr_t out of $puser_count ($percent percentile)</td></tr>\n"; 
         }
         if ($pr_e <> "") {
             $percent = "??";
             if ($puser_count > 0) { 
                $percent = 100 - (($pr_e / $puser_count) * 100);
                $percent = number_format($percent,4); 
             }
             print " <tr><td class=\"unsorted\">Project Rank Recent Average Credit:</td><td class=\"unsorted\">$pr_e out of $puser_count ($percent percentile)</td></tr>\n"; 
         }
         $usrurl = $purl . "show_user.php?userid=" . $user_id;
         print " <tr><td class=\"unsorted\"><a href=\"$usrurl\">Project User ID:</a></td><td class=\"unsorted\"><a href=\"$usrurl\">$user_id</a></td></tr>\n";
         print " <tr><td class=\"unsorted\">Country:</td><td class=\"unsorted\">$country</td></tr>\n";
         if ($teamid > 0) {
            print " <tr><td>Team:</td><td>$tname ($teamid)</td></tr>\n";
         }
         else {
            print " <tr><td>Team:</td><td>None</td></tr>\n";
         }
         if ($count > 0) {
            print "<tr><td class=\"unsorted\">Number of Computers:</td><td class=\"unsorted\">$count</td</tr>\n";
         }
         else {
            print "<tr><td class=\"unsorted\">Number of Computers:</td><td class=\"unsorted\">Unknown</td></tr>\n";
         }
         $dstr = date('F d, Y',$create_time);
         print "<tr><td>Member since:</td><td>$dstr</td></tr>\n";

         print "<tr><td class=\"unsorted\" colspan=2>90 day History<br><img src=\"user_graph.php?cpid=$cpid&amp;type=4&amp;project=$project_id\" alt=\"30 day hist graph\"></td></tr>\n";
         print "</table><p>\n";      
      }
   }

   if ($html=="") {
     print "</user>\n";
   } else {
?>
<script type="text/javascript"><!--
google_ad_client = "pub-0469295967534544";
google_ad_width = 180;
google_ad_height = 60;
google_ad_format = "180x60_as_rimg";
google_cpa_choice = "CAAQoMakgwIaCJ57wjARt0UUKMzD4IEB";
google_ad_channel = "";
//--></script>
<script type="text/javascript" src="http://pagead2.googlesyndication.com/pagead/show_ads.js">
</script>

<script type="text/javascript"><!--
google_ad_client = "pub-0469295967534544";
google_ad_width = 120;
google_ad_height = 60;
google_ad_format = "120x60_as_rimg";
google_cpa_choice = "CAAQueKXhAIaCKrCZAD7Zb_VKI-293M";
google_ad_channel = "";
//--></script>
<script type="text/javascript" src="http://pagead2.googlesyndication.com/pagead/show_ads.js">
</script>

<script type="text/javascript"><!--
google_ad_client = "pub-0469295967534544";
google_ad_width = 180;
google_ad_height = 60;
google_ad_format = "180x60_as_rimg";
google_cpa_choice = "CAAQ7IClgwIaCIrC0ettWplVKNjMrIMB";
google_ad_channel = "";
//--></script>
<script type="text/javascript" src="http://pagead2.googlesyndication.com/pagead/show_ads.js">
</script>
<script type="text/javascript"><!--
google_ad_client = "pub-0469295967534544";
google_ad_width = 120;
google_ad_height = 60;
google_ad_format = "120x60_as_rimg";
google_cpa_choice = "CAAQgdOWhAIaCD95EptSe9kNKIHD93M";
google_ad_channel = "";
//--></script>
<script type="text/javascript" src="http://pagead2.googlesyndication.com/pagead/show_ads.js">
</script>

<p>
<script type="text/javascript"><!--
google_ad_client = "pub-0469295967534544";
google_ad_width = 728;
google_ad_height = 90;
google_ad_format = "728x90_as";
google_ad_type = "text_image";
google_ad_channel ="";
google_color_border = "CCCCCC";
google_color_bg = "CCCCCC";
google_color_link = "000000";
google_color_text = "333333";
google_color_url = "666666";
//--></script>
<script type="text/javascript"
  src="http://pagead2.googlesyndication.com/pagead/show_ads.js">
</script>

<?php
     print "<p align=\"center\">\nCopyright 2006 Netsoft<br>\n";
     print "<p align=\"center\"><a href=\"http://validator.w3.org/check?uri=referer\"><img border=\"0\"\n";
     print "     src=\"http://www.w3.org/Icons/valid-html40\" alt=\"Valid HTML 4.0!\" height=\"31\" width=\"88\"></a>\n</p>";
     print "</body></html>\n"; 
   }
}
else
{
   print "<?xml version=\"1.0\" encoding=\"iso-8859-1\"?>\n";
   print "<user>\n";
   print "  <error>Can't connect to database</error>\n";
   print "</user>\n";
}

?>
