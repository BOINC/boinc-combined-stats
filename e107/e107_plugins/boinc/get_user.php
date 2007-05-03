<?php
require_once("../../class2.php");

require_once("bp_functions.php");

$cpid = $_GET["cpid"];

$rc = 0;

if ($cpid=="")
{
   $data =  "<error>cpid not specified</error>\n";
   $tname = "Boinc User Statistics";
   require_once(HEADERF);
   $text = $NEWSFEED_LIST_START . $data . $NEWSFEED_LIST_END;
   $ns->tablerender($tname, $text);
   require_once(FOOTERF);
   return;
}

   $data = "";

   mysql_select_db($db_db);

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
            $total_credit = number_format($total_credit,2);
            $expavg_credit = number_format($expavg_credit,2);
            $data .= "<table style='width:100%' class='nforumholder' cellpadding='0' cellspacing='0'>\n";
            $data .= "<tr><td class='forumheader3' colspan=\"3\">User Summary</td></tr>\n";
            $data .= "<tr><td class='nforumthread'>Name:</td><td colspan=\"2\" class='nforumthread'>$name</td></tr>\n";
            $data .= "<tr><td class='nforumthread'>User Cross Project ID:</td><td colspan=\"2\" class='nforumthread'>$user_cpid</td></tr>\n";
            $data .= "<tr><td class='nforumthread'>Total Credit:</td><td colspan=\"2\" class='nforumthread'>$total_credit</td></tr>\n";
            $data .= "<tr><td class='nforumthread'>Recent Average Credit:</td><td colspan=\"2\" class='nforumthread'>$expavg_credit</td></tr>\n";
            $data .= "<tr><td class='forumheader3' colspan=\"3\">Rankings</td></tr>\n";
            $data .= "<tr><td class='nforumthread' >&nbsp;</td><td class='nforumthread'>by Total Credit</td><td class='nforumthread'>by Recent Average Credit</td></tr>\n";
            $fboinc_user_count = number_format($boinc_user_count);
            if ($wr_t <> 0)
            {
              $percent = "??";
              if ($boinc_user_count > 0) { 
                $percent = 100 - (($wr_t / $boinc_user_count) * 100);
                $percent = number_format($percent,4); 
              }
              $fwr_t = number_format($wr_t);
               $data .= "<tr><td class='nforumthread'>World Rank</td><td class='nforumthread'><a href=\"u_rank.php?list=tc_p0c0&amp;start=$wr_t\">$fwr_t out of $fboinc_user_count</a><br/> ($percent percentile)</td>\n";
             
              if ($wr_e <> 0)
              {
                 $percent = "??";
                 if ($boinc_user_count > 0) { 
                  $percent = 100 - (($wr_e / $boinc_user_count) * 100);
                  $percent = number_format($percent,4); 
                 }
                 $fwr_e = number_format($wr_e);
                 $data .= "<td class='nforumthread'><a href=\"u_rank.php?list=rac_p0c0&amp;start=$wr_e\">$fwr_e out of $fboinc_user_count</a><br/> ($percent percentile)</td>";
              }
              else $data .= "<td class='nforumthread'>&nbsp;</td>";
       	    
              $data .= "</tr>\n";
            }

            //if ($r_one_t <> 0)
            //{
            //   $fr_one_t = number_format($r_one_t);
            //   $data .= "<tr><td class='nforumthread'>Using a single computer</td><td class='nforumthread'><a href=\"rankings.php?list=r_one_t&amp;start=$r_one_t\">$fr_one_t</a></td>\n";
            // 
            //
            //   if ($r_one_e <> 0)
            //   {
            //      $fr_one_e = number_format($r_one_e);
            //      $data .= "<td class='nforumthread'><a href=\"rankings.php?list=r_one_e&amp;start=$r_one_e\">$fr_one_e</a></td>\n";
            //   }
            //   else $data .= "<td class='nforumthread'>&nbsp;</td>";
            //   $data .= "</tr>\n";
            //}

            //if ($r_five_t <> 0)
            //{
            //   $fr_five_t = number_format($r_five_t);
            //   $data .= "<tr><td class='nforumthread'>Using five or fewer computers</td><td class='nforumthread'><a href=\"rankings.php?list=r_five_t&amp;start=$r_five_t\">$fr_five_t</a></td>\n";
            //
            //   if ($r_five_e <> 0)
            //   {
            //      $fr_five_e = number_format($r_five_e);
            //      $data .= "<td class='nforumthread'><a href=\"rankings.php?list=r_five_e&amp;start=$r_five_e\">$fr_five_e</a></td>\n";
            //   }
            //   else $data .= "<td class='nforumthread'>&nbsp;</td>";
            //   $data .= "</tr>\n";
            //}
            //$data .= "</table>\n";
            //$data .= "<p><table class=\"forum\">\n";
            $data .= "<tr><td colspan=\"3\" class='forumheader3'>Total Credit Graphs</td></tr>\n";
            $data .= "<tr><td class='nforumthread'><img src=\"user_graph.php?cpid=$cpid&amp;type=1\" alt=\"stats graph\" /></td>\n";
            if ($expavg_credit > 0) {
                $data .= "<td class='nforumthread' colspan=\"2\"><img src=\"user_graph.php?cpid=$cpid&amp;type=2\" alt=\"stats graph\" /></td></tr>\n";
            } else {
                $data .= "<td class='nforumthread' colspan=\"2\">&nbsp;</td></tr>\n";
            }
            $data .= "<tr><td class='nforumthread' colspan=\"3\"><center><img src=\"user_graph.php?cpid=$cpid&amp;type=3\" alt=\"stats graph\" /></center></td></tr>\n";
            $data .= "<tr><td class='nforumthread' colspan=\"3\"><center><img src=\"user_graph.php?cpid=$cpid&amp;type=7\" alt=\"stats graph\" /></center></td></tr>\n";
            //$data .= "</table>\n";
            
            //$data .= "<tr><td colspan=\"3\" class='nforumthread'><h3>Participating in Projects</h3></td></tr>\n";
            $data .= "<tr><td colspan=\"3\" class='forumheader3'>User Projects</td></tr>\n";
         }

         $i++;
         //$data .= "<table class=\"forum\">\n";
         $data .= "<tr><td colspan=\"2\"><b><a href=\"$purl\" style=\"color: #FFFFFF\">$pname</a></b></td></tr>\n";
         if ($url=="") 
         {
            $data .= " <tr><td class='nforumthread'>Name:</td><td class='nforumthread' colspan=\"2\">$name</td></tr>\n";
         } else {
            $data .= " <tr><td class='nforumthread'>Name:</td><td class='nforumthread' colspan=\"2\"><a href=\"http://$url\">$name</a></td></tr>\n";
         }
         $ptotal_credit = number_format($ptotal_credit,2);
         $pexpavg_credit = number_format($pexpavg_credit,2);
         $data .= " <tr><td class='nforumthread'>Total Credit:</td><td class='nforumthread' colspan=\"2\">$ptotal_credit</td></tr>\n";
         $data .= " <tr><td class='nforumthread'>Recent Average Credit:</td><td class='nforumthread' colspan=\"2\">$pexpavg_credit</td></tr>\n";
         if ($pr_t <> "") {
             $percent = "??";
             if ($puser_count > 0) { 
                $percent = 100 - (($pr_t / $puser_count) * 100);
                $percent = number_format($percent,4); 
             }
             $data .= " <tr><td class='nforumthread'>Project Rank Total Credit:</td><td class='nforumthread' colspan=\"2\">$pr_t out of $puser_count ($percent percentile)</td></tr>\n"; 
         }
         if ($pr_e <> "") {
             $percent = "??";
             if ($puser_count > 0) { 
                $percent = 100 - (($pr_e / $puser_count) * 100);
                $percent = number_format($percent,4); 
             }
             $data .= " <tr><td class='nforumthread'>Project Rank Recent Average Credit:</td><td class='nforumthread' colspan=\"2\">$pr_e out of $puser_count ($percent percentile)</td></tr>\n"; 
         }
         $usrurl = $purl . "show_user.php?userid=" . $user_id;
         $data .= " <tr><td class='nforumthread'><a href=\"$usrurl\">Project User ID:</a></td><td class='nforumthread' colspan=\"2\"><a href=\"$usrurl\">$user_id</a></td></tr>\n";
         $data .= " <tr><td class='nforumthread'>Country:</td><td class='nforumthread' colspan=\"2\">$country</td></tr>\n";
         if ($teamid > 0) {
            $data .= " <tr><td class='nforumthread'>Team:</td><td class='nforumthread' colspan=\"2\">$tname ($teamid)</td></tr>\n";
         }
         else {
            $data .= " <tr><td class='nforumthread'>Team:</td><td class='nforumthread' colspan=\"2\">None</td></tr>\n";
         }
         if ($count > 0) {
            $data .= "<tr><td class='nforumthread'>Number of Computers:</td><td class='nforumthread' colspan=\"2\">$count</td></tr>\n";
         }
         else {
            $data .= "<tr><td class='nforumthread'>Number of Computers:</td><td class='nforumthread' colspan=\"2\">Unknown</td></tr>\n";
         }
         $dstr = date('F d, Y',$create_time);
         $data .= "<tr><td class='nforumthread'>Member since:</td><td class='nforumthread' colspan=\"2\">$dstr</td></tr>\n";

         $data .= "<tr><td class='nforumthread' colspan=\"3\">90 day History<br/><img src=\"user_graph.php?cpid=$cpid&amp;type=4&amp;project=$project_id\" alt=\"30 day hist graph\" /></td></tr>\n";
         $data .= "<tr><td class='nforumthread' colspan=\"3\">52 Week History<br/><img src=\"user_graph.php?cpid=$cpid&amp;type=8&amp;project=$project_id\" alt=\"52 week hist graph\" /></td></tr>\n";
      }

   $data .= "</table>\n";      
   
   mysql_select_db($db_e107);
   // Kick out the display to e107 engine
   require_once(HEADERF);
   $ns->tablerender($tname, $data);
   require_once(FOOTERF);

?>
