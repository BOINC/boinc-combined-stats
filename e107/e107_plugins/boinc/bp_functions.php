<?php

include('/home/virtual/netsoft-online.com/home/boinc/db.php');

if(!function_exists("project_color")) {
function project_color($p) {
    switch ($p) {
    case 'climateprediction.net': return 'springgreen4'; // (0,139,69)
    case 'Predictor@Home': return 'skyblue'; // (135,206,235)
    case 'SETI@home': return 'royalblue'; // (65,105,225)
    case 'Einstein@Home': return 'orange'; // (255,165,0)
    case 'Rosetta@home': return 'violet'; // (238,130,238)
    case 'PrimeGrid': return 'seashell3'; // (205,197,191)
    case 'LHC@home': return 'coral'; // (255,127,80)
    case 'World Community Grid': return 'salmon'; // (250,128,114)
    case 'BURP': return 'yellow1'; return 'springgreen'; // (0,255,127)
    case 'SZTAKI Desktop Grid': return 'tomato3'; // (205,79,57)
    case 'uFluids': return 'black'; // (0,0,0)
    case 'SIMAP': return 'darkseagreen'; // (143,188,143)
    case 'Folding@Home': return 'darkorchid'; //(153,50,204)
    case 'MalariaControl': return 'dodgerblue'; // (30,144,255)
    case 'The Lattice Project': return 'darkgreen'; // (0,100,0)
    case 'Pirates@Home': return 'chartreuse'; // (127,255,0)
    case 'BBC Climate Change Experiment': return 'gold3'; // (205,173,0)
    case 'Leiden Classical': return 'mediumred'; // (140,34,34)
    case 'SETI@home Beta': return 'cadetblue1'; // (152,245,255)
    case 'RALPH@Home': return 'linen'; // (250,240,230)
    case 'QMC@HOME': return 'lightgreen'; // (144,238,144)
    case 'XtremLab': return 'gray5'; // (130,130,130)
    case 'HashClash': return 'hotpink'; // (255,105,180)
    case 'cpdn seasonal': return 'white'; // (255,255,255)
    case 'Chess960@Home Alpha': return 'brown'; // (165,42,42)
    case 'vtu@home': return 'red'; // (255,0,0)
    case 'LHC@home alpha': return 'peru'; // (205,133,63)
    case 'TANPAKU': return 'darkkhaki'; // (189,183,107)
    case 'other': return 'goldenrod1'; // (255,193,37)
    case 'Rectilinear Crossing Number': return 'cadetblue4'; // (83,134,139)
    case 'Nano-Hive@Home': return 'honeydew'; // (193,205,193)
    case 'Spinhenge@home': return 'lavenderblush'; // (255,240,245)
    case 'RieselSieve': return 'bisque3'; // (205,183,158)
    case 'Project Neuron': return 'hotpink4'; // (139,58,98)
    case 'Docking@Home': return 'lightblue2'; // (178,223,238)
    case 'proteins@home': return 'blue'; // (178,223,238)
    case 'DepSpid': return 'tan4'; // (178,223,238)
    case 'ABC@home': return 'burlywood'; // (222,184,135)
    case 'BOINC alpha test': return 'beige'; // (245,245,220)
    case 'ABC@home': return 'burlywood'; // (222,184,135)
    case 'BOINC alpha test': return 'beige'; // (245,245,220)
    case 'WEP-M+2': return 'mediumspringgreen'; // (0,250,154)
    case 'Zivis Superordenador Ciudadano': return 'AntiqueWhite1'; // (255,239,219)
    case 'SciLINC': return 'aliceblue'; // (240,248,255)
    case 'APS@Home': return 'coral3'; // (205,91,69)
    case 'PS3GRID': return 'darkcyan'; // (i0,139,139)
    case 'Superlink@Technion': return 'darkolivegreen1'; // (202,255,112)
    case 'BRaTS@Home': return 'indianred1'; // (255,106,106)
    case 'Cosmology@Home': return 'khaki'; // (240,230,140)
    case 'SHA 1 Collision Search': return 'lemonchiffon'; // (255,250,205)

    }
    return 'gray4';
}
}

if(!function_exists("get_max_count")) {
function get_max_count ($table)
{

     $query = "select count(*) as cnt from b_rank_user_".$table;
     $res = mysql_query($query);
     if (!$res) {
       echo "<b>Error performing query: " . mysql_error() . "</b>";
       exit();
     }
     $count = 0;

     while ($row = mysql_fetch_array($res) )
     {
        $count=$row["cnt"];
     }

     return $count;
}
}

if(!function_exists("get_project_name")) {
function get_project_name($project_id) {
   global $db_db, $db_e107;
   $name = "DB Connect Error";

   mysql_select_db($db_db);

   $query = "select name from b_projects where project_id = $project_id";

   $res = mysql_query($query);
   if (!$res) {
          $name = "DB Error $db_db " ;
   } else {

          while ($row = mysql_fetch_array($res) )
          {
             $name = $row["name"];
          }
   }
   mysql_select_db($db_e107);
   return $name;
}
}

if(!function_exists("get_project_info")) {
function get_project_info($project_id) {
   global $db_db, $db_e107;
   
   mysql_select_db($db_db);

   $query = "select name, url, user_count, host_count, team_count, active_users, active_hosts, active_teams, total_credit, rac, country_count,project_status, project_for_profit, shown from b_projects where project_id=$project_id";


   $data = "<table style='width:100%' class='nforumholder' cellpadding='0' cellspacing='0'>\n<tr><th class='forumheader3' colspan=3>".LAN_300."</th></tr>\n";

   $res = mysql_query($query);
   if (!$res) {
          $name = "DB Error $db_db " ;
   } else {

          while ($row = mysql_fetch_array($res) )
          {
             $name = $row["name"];
             $url = $row["url"];
             $user_count = $row["user_count"];
             $host_count = $row["host_count"];
             $team_count = $row["team_count"];
             $auser_count = $row["active_users"];
             $ahost_count = $row["active_hosts"];
             $ateam_count = $row["active_teams"];
             $country_count = $row["country_count"];
             $total_credit = $row["total_credit"];
             $rac = $row["rac"];
             $pstatus = $row["project_status"];
             $pprofit = $row["project_for_profit"];
             $shown = $row["shown"];
          }
   }

   $total_credit = number_format($total_credit,0);
   $rac  = number_format($rac,0);
   $user_count = number_format($user_count,0);
   $host_count = number_format($host_count,0);
   $team_count = number_format($team_count,0);
   $auser_count = number_format($auser_count,0);
   $ahost_count = number_format($ahost_count,0);
   $ateam_count = number_format($ateam_count,0);

   $data .= "<tr><td class='nforumthread'>".LAN_301."</td><td class='nforumthread' colspan=2>$name</td></tr>\n";
   if ($url != "" && $shown=="Y") {
   $data .= "<tr><td class='nforumthread'>".LAN_302."</td><td class='nforumthread' colspan=2><a href=\"$url\">$url</a></td></tr>\n";

   } 
   $data .= "<tr><td class='nforumthread' >".LAN_303."</td><td class='nforumthread' colspan=2>$pstatus</td></tr>\n"; 
   $data .= "<tr><td class='nforumthread' >".LAN_304."</td><td class='nforumthread' colspan=2>$pprofit</td></tr>\n"; 
   $data .= "<tr><td class='nforumthread' >".LAN_305."</td><td class='nforumthread' colspan=2>$total_credit</td></tr>\n";
   $data .= "<tr><td class='nforumthread' >".LAN_306."</td><td class='nforumthread' colspan=2>$rac</td></tr>\n";

   $data .= "<tr><td class='nforumthread' >&nbsp;</td><td class='nforumthread2'>".LAN_307."</td><td class='nforumthread2'>".LAN_308."</td></tr>\n";
   $data .= "<tr><td class='nforumthread'>".LAN_309."</td><td class='nforumthread'>$user_count</td><td class='nforumthread'>$auser_count</td></tr>\n";
   $data .= "<tr><td class='nforumthread'>".LAN_310."</td><td class='nforumthread'>$team_count</td><td class='nforumthread'>$ateam_count</td></tr>\n";
   $data .= "<tr><td class='nforumthread'>".LAN_311."</td><td class='nforumthread'>$host_count</td><td class='nforumthread'>$ahost_count</td></tr>\n";

   $data .= "<tr><td class='forumheader3' colspan=3>".LAN_312."</td></tr>\n";
   $data .= "<tr><td class='nforumthread' colspan=3><center><img alt=\"".LAN_312."\" src=project_graph.php?projectid=$project_id&amp;type=5 /></center></td></tr>\n";

   $data .= "<tr><td class='forumheader3' colspan=3>".LAN_313."</td></tr>\n";
   $data .= "<tr><td class='nforumthread' colspan=3><center><img alt=\"".LAN_313."\" src=project_graph.php?projectid=$project_id&amp;type=1 /></center></td></tr>\n";
   $data .= "<tr><td class='nforumthread' colspan=3><center><img alt=\"".LAN_313."\" src=project_graph.php?projectid=$project_id&amp;type=6 /></center></td></tr>\n";
   $data .= "<tr><td class='forumheader3' colspan=3>".LAN_314."</td></tr>\n";
   $data .= "<tr><td class='nforumthread' colspan=3><center><img alt=\"".LAN_314."\" src=project_graph.php?projectid=$project_id&amp;type=3 /></center></td></tr>\n";
   $data .= "<tr><td class='nforunthread' colspan=3><center><img alt=\"".LAN_314."\" src=project_graph.php?projectid=$project_id&amp;type=8 /></center></td></tr>\n";
   $data .= "<tr><td class='forumheader3' colspan=3>".LAN_315."</td></tr>\n";
   $data .= "<tr><td class='nforumthread' colspan=3><center><img alt=\"".LAN_315."\" src=project_graph.php?projectid=$project_id&amp;type=2 /></center></td></tr>\n";
   $data .= "<tr><td class='nforumthread' colspan=3><center><img alt=\"".LAN_315."\" src=project_graph.php?projectid=$project_id&amp;type=7 /></center></td></tr>\n";


   $data .="</table>\n";
   mysql_select_db($db_e107);

   return $data;

}
}

if(!function_exists("display_project_stats")) {
  function display_project_stats($active)
  {
      global $db_db, $db_e107;
      // connect to dtabase
      mysql_select_db($db_db);

      if ($active > 0) {
      $query = "select a.name,a.url,a.user_count,a.host_count,a.team_count,".
          "a.country_count,a.total_credit,a.project_id,a.y_user_count,".
          "a.y_host_count,a.y_team_count,a.y_country_count,a.y_total_credit,".
          "a.last_update,a.active_hosts,a.active_users,a.active_teams,".
          "a.y_active_users,a.y_active_teams,a.y_active_hosts,a.shown, a.retired from b_projects a ".
          "where (a.retired='N' and a.shown='Y') or a.project_id=19 order by a.total_credit desc";
      } else {
      $query = "select a.name,a.url,a.user_count,a.host_count,a.team_count,".
          "a.country_count,a.total_credit,a.project_id,a.y_user_count,".
          "a.y_host_count,a.y_team_count,a.y_country_count,a.y_total_credit,".
          "a.last_update,a.active_hosts,a.active_users,a.active_teams,".
          "a.y_active_users,a.y_active_teams,a.y_active_hosts, a.shown, a.retired from b_projects a ".
          "where a.retired='Y' and a.shown='Y' order by a.total_credit desc";
      }
      $res = mysql_query($query);
      if (!$res) {
        mysql_select_db($db_e107);
        $DATA =  "<b>Error performing query: " . mysql_error() . "</b>";
        return $DATA;
      }

   if ($active > 0) {

   $DATA = "<table style='width: 100%;' class='fborder'>
     <tr>
     <th class='forumheader'>".LAN_316."</th>
     <th colspan=2 class='forumheader'>".LAN_317."</th>
     <th colspan=2 class='forumheader'>".LAN_318."</th>
     <th colspan=2 class='forumheader'>".LAN_319."</th>
     <th colspan=1 class='forumheader'>".LAN_320."</th>
     <th colspan=1 class='forumheader'>".LAN_321."</th>
     <th colspan=2 class='forumheader'><font size =-1>".LAN_322."</font></th>
     </tr>";
  } else {

   $DATA = "<table style='width: 100%;' class='fborder'>
     <tr>
     <th class='forumheader'>".LAN_316."</th>
     <th colspan=1 class='forumheader'>".LAN_323."</th>
     <th colspan=1 class='forumheader'>".LAN_324."</th>
     <th colspan=1 class='forumheader'>".LAN_325."</th>
     <th colspan=1 class='forumheader'>".LAN_320."</th>
     <th colspan=1 class='forumheader'>".LAN_321."</th>
     <th colspan=1 class='forumheader'><font size =-1>".LAN_322."</font></th>
     </tr>";

   }
   $i=0;
   $wt = 0;
   while ($row = mysql_fetch_array($res) )
   {
      $name=$row["name"];
      $url=$row["url"];
      $user_count=$row["user_count"];
      $host_count=$row["host_count"];
      $team_count=$row["team_count"];
      $country_count=$row["country_count"];
      $total_credit=$row["total_credit"];
      $pid=$row["project_id"];
      $yuser_count=$row["y_user_count"];
      $yhost_count=$row["y_host_count"];
      $yteam_count=$row["y_team_count"];
      $ycountry_count=$row["y_country_count"];
      $ytotal_credit=$row["y_total_credit"];
      $last_update=$row["last_update"];
      $auser_count=$row["active_users"];
      $ahost_count=$row["active_hosts"];
      $ateam_count=$row["active_teams"];
      $yauser_count=$row["y_active_users"];
      $yahost_count=$row["y_active_hosts"];
      $yateam_count=$row["y_active_teams"];
      $shown = $row["shown"];
      $retired = $row["retired"];

      if ($i % 2 == 0) $bc = "nforumthread";
      else $bc = "nforumthread2";
      $i++;
      $wt += $total_credit;

      $uc = $user_count - $yuser_count;
      $hc = $host_count - $yhost_count;
      $tc = $team_count - $yteam_count;
      $auc = $auser_count - $yauser_count;
      $ahc = $ahost_count - $yahost_count;
      $atc = $ateam_count - $yateam_count;
      $cc = $country_count - $ycountry_count;
      $tcc = $total_credit - $ytotal_credit;

      $user_count = number_format($user_count);
      $host_count = number_format($host_count);
      $team_count = number_format($team_count);
      $auser_count = number_format($auser_count);
      $ahost_count = number_format($ahost_count);
      $ateam_count = number_format($ateam_count);
      $country_count = number_format($country_count);
      $total_credit = number_format($total_credit);

      if ($uc >= 0) {
         $cuc = "#005000";
         $suc = "+";
      } else {
         $cuc = "#500000";
         $suc = "";
      }

      if ($hc >= 0) {
         $chc = "#005000";
         $shc = "+";
      } else {
         $chc = "#500000";
         $shc = "";
      }

      if ($tc >= 0) {
         $ctc = "#005000";
         $stc = "+";
      } else {
         $ctc = "#500000";
         $stc = "";
      }

      if ($auc >= 0) {
         $acuc = "#005000";
         $asuc = "+";
      } else {
         $acuc = "#500000";
         $asuc = "";
      }

      if ($ahc >= 0) {
         $achc = "#005000";
         $ashc = "+";
      } else {
         $achc = "#500000";
         $ashc = "";
      }

      if ($atc >= 0) {
         $actc = "#005000";
         $astc = "+";
      } else {
         $actc = "#500000";
         $astc = "";
      }

      if ($cc >= 0) {
         $ccc = "#005000";
         $scc = "+";
      } else {
         $ccc = "#500000";
         $scc = "";
      }

      if ($tcc >= 0) {
         $ctcc = "#005000";
         $stcc = "+";
      } else {
         $ctcc = "#500000";
         $stcc = "";
      }

      $uc = number_format($uc);
      $hc = number_format($hc);
      $tc = number_format($tc);
      $auc = number_format($auc);
      $ahc = number_format($ahc);
      $atc = number_format($atc);
      $cc = number_format($cc);
      $tcc = number_format($tcc);

      $DATA .= "<tr>\n";
      if ($active > 0)
         $DATA .= "<td rowspan=2 class='$bc'><a href=\"bp.php?project=$pid\">$name</a>";
      else
         $DATA .= "<td class='$bc'><a href=\"bp.php?project=$pid\">$name</a>";
         if ($url != "" && $pid != 19 && $shown=="Y" && $retired=="N") 
           $DATA .= "&nbsp;<a href=\"$url\">(site)</a>";

         $DATA .= "</td>\n<td align=right class='$bc'>$user_count</td>\n";
         if ($active > 0) $DATA .= "<td align=right class='$bc'><font size=-1 color=\"$cuc\">($suc$uc)</font></td>\n";
         $DATA .= "<td align=right class='$bc'>$host_count</td>\n";
         if ($active > 0) $DATA .= "<td align=right class='$bc'><font size=-1 color=\"$chc\">($shc$hc)</font></td>\n";
         $DATA .= "<td align=right class='$bc'>$team_count</td>\n";
         if ($active > 0) $DATA .= "<td align=right class='$bc'><font size=-1 color=\"$ctc\">($stc$tc)</font></td>\n";
         if ($active > 0)
            $DATA .= "<td rowspan=1 align=right class='$bc'>$country_count</td>\n";
         else 
            $DATA .= "<td align=right class='$bc'>$country_count</td>\n";
         /*if ($active > 0) 
            $DATA .= "<td rowspan=1 align=right class='$bc'><font size=-1 color=\"$ccc\">($scc$cc)</font></td>\n"; */
         if ($active > 0)
            $DATA .= "<td rowspan=1 align=right class='$bc'>$total_credit</td>\n";
         else
            $DATA .= "<td align=right class='$bc'>$total_credit</td>\n";
         /*if ($active > 0) 
              $DATA .= "<td rowspan=2 align=right class='$bc'><font size=-1 color=\"$ctcc\">($stcc$tcc)</font></td>\n"; */
         if ($active > 0) 
              $DATA .= "<td rowspan=2 align=right class='$bc'><font size=-1>$last_update</font></td>\n";
         else
              $DATA .= "<td align=right class='$bc'><font size=-1>$last_update</font></td>\n";
      if ($active > 0) {
         $DATA .= "</tr>\n";
         $DATA .= "<tr>\n";
         $DATA .= "<td align=right class='$bc'>$auser_count</td>\n";
         $DATA .= "<td align=right class='$bc'><font size=-1 color=\"$acuc\">($asuc$auc)</font></td>\n";
         $DATA .= "<td align=right class='$bc'>$ahost_count</td>\n";
         $DATA .= "<td align=right class='$bc'><font size=-1 color=\"$achc\">($ashc$ahc)</font></td>\n";
         $DATA .= "<td align=right class='$bc'>$ateam_count</td>\n";
         $DATA .= "<td align=right class='$bc'><font size=-1 color=\"$actc\">($astc$atc)</font></td>\n";
         $DATA .= "<td align=right class='$bc'><font size=-2 color=\"$ccc\">($scc$cc)</font></td>\n";
         $DATA .= "<td align=right class='$bc'><font size=-2 color=\"$ctcc\">($stcc$tcc)</font></td>\n";
      }
      $DATA .= "</tr>\n";
   }
   $DATA .= "</table>\n";

  mysql_select_db($db_e107);
  return $DATA;
 }

}

if(!function_exists("display_rankings")) {

  function display_rankings()
  {

     $DATA = "<table style='width:100%' class='nforumholder' cellpadding='0' cellspacing='0'>
     <tr>
         <th class='nforumcaption3' rowspan=3>".LAN_326."</th>
         <th class='nforumcaption3' style='text-align:center' colspan=6>".LAN_327."</th>
     </tr>
     <tr >
         <th class='nforumcaption3' style='text-align:center' colspan=2>".LAN_328."</th>
         <th class='nforumcaption3' style='text-align:center' colspan=2>".LAN_329."</th>
         <th class='nforumcaption3' style='text-align:center' colspan=2>".LAN_330."</th>
     </tr>
     <tr class='nforumcaption2'>
       <th class='nforumcaption3'>".LAN_331."</th><th class='nforumcaption3'>".LAN_332."</th>
       <th class='nforumcaption3'>".LAN_331."</th><th class='nforumcaption3'>".LAN_332."</th>
       <th class='nforumcaption3'>".LAN_331."</th><th class='nforumcaption3'>".LAN_332."</th>
     </tr>

     <tr><td class='nforumthread'>".LAN_333."</td>
         <td class='nforumthread'><a href=\"u_rank.php?list=tc_p0c0\">".LAN_334."</a></td>
         <td class='nforumthread'><a href=\"u_rank.php?list=rac_p0c0\">".LAN_334."</a></td>
         <td class='nforumthread'><a href=\"u_rank.php?list=tc_p2c0\">".LAN_334."</a></td>
         <td class='nforumthread'><a href=\"u_rank.php?list=rac_p2c0\">".LAN_334."</a></td>

         <td class='nforumthread'><a href=\"u_rank.php?list=tc_p5c0\">".LAN_334."</a></td>
         <td class='nforumthread'><a href=\"u_rank.php?list=rac_p5c0\">".LAN_334."</a></td>
     </tr>

     <tr><td class='nforumthread2'>".LAN_335."</td>
         <td class='nforumthread2'><a href=\"u_rank.php?list=tc_p1c1\">".LAN_334."</a></td>
         <td class='nforumthread2'><a href=\"u_rank.php?list=rac_p1c1\">".LAN_334."</a></td>

         <td class='nforumthread2'><a href=\"u_rank.php?list=tc_p2c1\">".LAN_334."</a></td>
         <td class='nforumthread2'><a href=\"u_rank.php?list=rac_p2c1\">".LAN_334."</a></td>

         <td class='nforumthread2'><a href=\"u_rank.php?list=tc_p5c1\">".LAN_334."</a></td>
         <td class='nforumthread2'><a href=\"u_rank.php?list=rac_p5c1\">".LAN_334."</a></td>
     </tr>

     <tr><td class='nforumthread'>".LAN_336."</td>
         <td class='nforumthread'><a href=\"u_rank.php?list=tc_p1c5\">".LAN_334."</a></td>
         <td class='nforumthread'><a href=\"u_rank.php?list=rac_p1c5\">".LAN_334."</a></td>

         <td class='nforumthread'><a href=\"u_rank.php?list=tc_p2c5\">".LAN_334."</a></td>
         <td class='nforumthread'><a href=\"u_rank.php?list=rac_p2c5\">".LAN_334."</a></td>

         <td class='nforumthread'><a href=\"u_rank.php?list=tc_p5c5\">".LAN_334."</a></td>
         <td class='nforumthread'><a href=\"u_rank.php?list=rac_p5c5\">".LAN_334."</a></td>

     </tr>

     <tr><td class='nforumthread2'>".LAN_337."</td>
         <td class='nforumthread2'><a href=\"u_rank.php?list=tc_p1c10\">".LAN_334."</a></td>
         <td class='nforumthread2'><a href=\"u_rank.php?list=rac_p1c10\">".LAN_334."</a></td>

         <td class='nforumthread2'><a href=\"u_rank.php?list=tc_p2c10\">".LAN_334."</a></td>
         <td class='nforumthread2'><a href=\"u_rank.php?list=rac_p2c10\">".LAN_334."</a></td>

         <td class='nforumthread2'><a href=\"u_rank.php?list=tc_p5c10\">".LAN_334."</a></td>
         <td class='nforumthread2'><a href=\"u_rank.php?list=rac_p5c10\">".LAN_334."</a></td>

     </tr>

     <tr><td class='nforumthread'>".LAN_338."</td>
         <td class='nforumthread'><a href=\"u_rank.php?list=tc_p1c20\">".LAN_334."</a></td>
         <td class='nforumthread'><a href=\"u_rank.php?list=rac_p1c20\">".LAN_334."</a></td>

         <td class='nforumthread'><a href=\"u_rank.php?list=tc_p2c20\">".LAN_334."</a></td>
         <td class='nforumthread'><a href=\"u_rank.php?list=rac_p2c20\">".LAN_334."</a></td>

         <td class='nforumthread'><a href=\"u_rank.php?list=tc_p5c20\">".LAN_334."</a></td>
         <td class='nforumthread'><a href=\"u_rank.php?list=rac_p5c20\">".LAN_334."</a></td>
     </tr>

     </table>";

   return $DATA;

  }
}


if(!function_exists("display_users")) {
function display_users($start,$end,$table)
{

   global $db_db, $db_e107;
   $name = "DB Connect Error";

   mysql_select_db($db_db);


   $query = "select a.user_cpid,a.total_credit,a.rac,b.rank,c.name,b_country.country,a.active_project_count " .
              "from b_cpid a, b_rank_user_".$table." b, b_users c left join b_country " .
              "on c.country_id=b_country.country_id " .
              "where b.rank >= $start and b.rank < $end and " .
              "a.table_id=b.b_cpid_id and  c.b_cpid_id=a.table_id  order by b.rank";


   $res = mysql_query($query);
   if (!$res) {
      mysql_select_db($db_e107);
      echo "<b>Error performing query: " . mysql_error() . "</b>";
      exit();
   }

   $data = "<table style='width:100%' class='nforumholder' cellpadding='0' cellspacing='0'>\r\n<td colspan=2 align=left>\r\n";

    if ($start >= 100)
    {
       $ns = $start - 100;
       if ($ns < 1) $ns = 1;
       $data .= "<a href=u_rank.php?list=$table&start=$ns>&lt;&lt; 100</a>&nbsp;&nbsp ";
    }

    if ($start >= 500)
    {
       $ns = $start - 500;
       if ($ns < 1) $ns = 1;
       $data .= "<a href=u_rank.php?list=$table&start=$ns>&lt;&lt; 500</a>&nbsp;&nbsp ";
    }

    if ($start >= 1000)
    {
       $ns = $start - 1000;
       if ($ns < 1) $ns = 1;
       $data .= "<a href=u_rank.php?list=$table&start=$ns>&lt;&lt; 1000</a>&nbsp;&nbsp ";
    }

    if ($start >= 10000)
    {
       $ns = $start - 10000;
       if ($ns < 1) $ns = 1;
       $data .= "<a href=u_rank.php?list=$table&start=$ns>&lt;&lt; 10000</a>&nbsp;&nbsp ";
    }

    if ($start >= 100000)
    {
       $ns = $start - 100000;
       if ($ns < 1) $ns = 1;
       $data .= "<a href=u_rank.php?list=$table&start=$ns>&lt;&lt; 100000</a>&nbsp;&nbsp ";
    }


   $data .= "</td><td align=center colspan=2><a href=u_rank.php>".LAN_339."</a></td><td colspan=3 align=right>\r\n";

   $ns = $start + 100;
   $data .= "<a href=u_rank.php?list=$table&start=$ns>&gt;&gt; 100</a>&nbsp;&nbsp ";
   $ns = $start + 500;
   $data .= "<a href=u_rank.php?list=$table&start=$ns>&gt;&gt; 500</a> &nbsp;&nbsp";
   $ns = $start + 1000;
   $data .= "<a href=u_rank.php?list=$table&start=$ns>&gt;&gt; 1000</a> &nbsp;&nbsp";
   $ns = $start + 10000;
   $data .= "<a href=u_rank.php?list=$table&start=$ns>&gt;&gt; 10000</a> &nbsp;&nbsp;";
   $ns = $start + 100000;
   $data .= "<a href=u_rank.php?list=$table&start=$ns>&gt;&gt; 100000</a> &nbsp;&nbsp;";

   $data .= "</td></tr>
   <tr>
     <td class='forumheader2'>".LAN_340."</td>
     <td class='forumheader2'>".LAN_341."</td>
     <td class='forumheader2'>".LAN_342."</td>
     <td class='forumheader2'>".LAN_343."</td>
     <td class='forumheader2'>".LAN_203."</td>
     <td class='forumheader2'>".LAN_344."</td>
     <td class='forumheader2'>".LAN_345."</td>
   </tr>";


    $last_cpid="";
    $pcount=-1;
    $names = array();
    $urls = array();
    $i=0;

    while ($row = mysql_fetch_array($res) )
    {
      $user_cpid=$row["user_cpid"];

      $pcount++;
      if ($last_cpid == $user_cpid)
      { array_push($names, $name);
         $name=$row["name"];
      }
      else
      {
         if ($last_cpid != "")
         {
            if ($i % 2 == 0) $bc = "nforumthread";
            else $bc = "nforumthread2";
            $i++;

            // print out all the different variations on the name
            $data .= "<tr>\n";
            $data .= "<td align=right class='$bc'>$rank</td>\n";
            $data .= "<td align=left class='$bc'>";

            $data .= "<a href=\"get_user.php?cpid=$last_cpid\">$name";
            $printed = array();
            array_push($printed,$name);
            foreach($names as $nn)
            {
                $found = 0;
                foreach ($printed as $n)
                {
                   if (strcasecmp($n,$nn)==0)
                       $found = 1;
                }
                if ($found == 0)
                {
                   $data .= " / $nn";
                   array_push($printed,$nn);
                }
            }

            $data .= "</a></td>\n";
            $data .= "<td align=right class='$bc'>$total_credit</td>\n";
            $data .= "<td align=right class='$bc'>$expavg_credit</td>\n";
            $data .= "<td align=right class='$bc'>$country</td>\n";
            $data .= "<td align=right class='$bc'>$acount</td>\n";
            $data .= "<td align=right class='$bc'>$pcount</td>\n";
            $data .= "</tr>\n";

            $pcount = 0;
            $names = array();

         }

         $last_cpid = $user_cpid;
         $name=$row["name"];
         $total_credit=$row["total_credit"];
         $expavg_credit=$row["rac"];
         $rank=$row["rank"];
         $country=$row["country"];
         $acount=$row["active_project_count"];

         $total_credit = number_format($total_credit);
         $expavg_credit = number_format($expavg_credit);
     }

   }

   $pcount++;
   if ($i % 2 == 0) $bc = "nforumthread";
   else $bc = "nforumthread2";
   $data .= "<tr>\n";
   $data .= "<td align=right class='$bc'>$rank</td>\n";
   $data .= "<td align=left class='$bc'>";

   $data .= "<a href=\"get_user.php?cpid=$last_cpid\">$name";
   $printed = array();
   array_push($printed,$name);
   foreach($names as $nn)
   {
       $found = 0;
       foreach ($printed as $n)
       {
           if (strcasecmp($n,$nn)==0)
               $found = 1;
       }
       if ($found == 0)
       {
           $data .= " / $nn";
           array_push($printed,$nn);
       }
   }
   $data .= "</a></td>\n";
   $data .= "<td align=right class='$bc'>$total_credit</td>\n";
   $data .= "<td align=right class='$bc'>$expavg_credit</td>\n";
   $data .= "<td align=right class='$bc'>$country</td>\n";
   $data .= "<td align=right class='$bc'>$acount</td>\n";
   $data .= "<td align=right class='$bc'>$pcount</td>\n";
   $data .= "</tr>\n";

   $data .= "</table>\n";

  return $data;
}
}
?>
