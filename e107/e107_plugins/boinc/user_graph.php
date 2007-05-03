<?php
Header("Content-Type: image/png");
require('./jpgraph/jpgraph.php');
require('./jpgraph/jpgraph_bar.php');
require('./jpgraph/jpgraph_pie.php');
require('./jpgraph/jpgraph_line.php');
include("bp_functions.php");


$cpid = $_GET["cpid"];
$type = $_GET["type"];
if(isset($_GET["project"])) $proj = $_GET["project"];

define("MAXPROJ",10);

function get_day_pointer($keytype) {
   $query = "select value from b_currentday where key_item='$keytype'";
   $res = mysql_query($query);
   if (!$res) {
     exit();
   }
   $value = 0;

   while ($row = mysql_fetch_array($res))
   {
      $value = $row["value"];
   }

   return $value;
}

function do_tc_graph($cpid,$type,$proj) {

   $tnow = time();

   if ($type == 7 || $type ==  8 || $type == 9 || $type == 10)
      $startday = get_day_pointer("week");
   else 
      $startday = get_day_pointer("tc");

   if ($startday == 0) {
	echo "Failed to get start counter<br>";
        return;
   }

   if ($type == 3 || $type == 5 || $type == 7 || $type == 9) {
      $query = "select a.name as pname, c.project_id,b.* ".
            "from b_projects a, b_users_total_credit_hist b, b_users c ".
            "where a.project_id=c.project_id and b.b_users_id=c.table_id and ".
            "c.user_cpid='$cpid'";
   } else
      $query = "select a.name as pname, c.project_id,b.* ".
            "from b_projects a, b_users_total_credit_hist b, b_users c ".
            "where a.project_id=c.project_id and b.b_users_id=c.table_id and ".
            "c.user_cpid='$cpid' and c.project_id=$proj";

   $res = mysql_query($query);
   if (!$res) {
     exit();
   }

   $graph = new Graph(600,300,"auto");
   $graph->SetScale("texlin");
   $graph->SetShadow();
   $graph->SetMarginColor('white');
   $graph->img->SetMargin(60,200,20,50);
   $graph->footer->left->set ("(C) 2007 http://boinc.netsoft-online.com/");


   $project_name="";
   $count = 0;
   $dataset = array();
   $dates = array();
   $names = array();
   //$date_to_find = mktime(0,0,0,$tn["mon"],$tn["mday"],$tn["year"]);

   while ($row = mysql_fetch_array($res))
   {
        $prev = -1;
        $start = $startday + 1;
        if ($start > 91 && ($type >=3 && $type <= 6)) $start = 1;
        if ($start > 52 && ($type >=7 && $type <= 10)) $start = 1;
        $end = $startday;

	$project_name = $row["pname"];
	$project_id = $row["project_id"];

	//echo "Start: $start<br>End: $end<br>";
	$dataset = array();

        while ($start != $end) {
                if ($type == 7 || $type == 8 || $type == 9 || $type == 10)
                   $rstring = "w_$start";
                else
                   $rstring = "d_$start";

                $c = $row[$rstring];
                if ($type == 3 || $type == 4 || $type == 7 || $type == 8) {
                    //echo "$start : $c<br>";
    		    array_push($dataset,$c);
                } else {
                    if ($prev == -1) {
                       if ($c == 0)
                         array_push($dataset,"");
                       else
                          $prev = $c;
                    } else {
                       $diff = $c - $prev;
                       if ($diff < 0) $diff = 0;
                       array_push($dataset,$diff);
                       $prev = $c;
                    }
                }
		$start++;
		if ($start > 91 && ($type >=3 && $type <= 6)) $start = 1;
                if ($start > 52 && ($type >=7 && $type <= 10)) $start = 1;
	}

      $dplot[$count] = new LinePLot($dataset);
      $dplot[$count]->SetFillColor(project_color($project_name));
      $dplot[$count]->SetLegend($project_name);
      $count++;
   }


   
   $accplog = new AccLinePlot($dplot);
   $graph->Add($accplog);
   $graph->xaxis->SetTickLabels($dates);
   $graph->xaxis->SetTextTickInterval(2);
   if ($type == 3 || $type == 4) {
      $date_to_find = $tnow - (90*24*60*60);
      $graph->title->Set("Total Credit (90 day hist)");
   } else if ($type == 7 || $type == 8) {
      $date_to_find = $tnow - (365*24*60*60);
      $graph->title->Set("Total Credit (52 week hist)");
   } else if ($type == 9 || $type == 10) {
      $date_to_find = $tnow - (365*24*60*60);
      $graph->title->Set("Credit per week (52 week hist)");
   } else {
      $date_to_find = $tnow - (89*24*60*60);
      $graph->title->Set("Credit per day (89 day hist)");
   }

   $fda = getdate($tnow);
   $lda = getdate($date_to_find);

   $xa =  $lda["month"] . " ".$lda["mday"].", ".$lda["year"];
   $xa .= " through ". $fda["month"] . " ".$fda["mday"].", ".$fda["year"];


   $graph->xaxis->title->Set($xa);
   $graph->title->SetFont(FF_FONT1,FS_BOLD);
   $graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
   $graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
   // Display the graph
   $graph->Stroke();


}


function do_pie_graph($cpid,$type) {

   $query = "select a.total_credit,a.rac,b.name from b_users a, b_projects b ".
            "where a.project_id=b.project_id and a.user_cpid='$cpid'";

   if ($type == 1)
   { 
      $query .= " order by a.total_credit desc";
   } else {
      $query .= " order by a.rac desc";
   }

   $res = mysql_query($query);
   if (!$res) {
     exit();
   }

   $cnt = 0;
   $sum = 0;

   $counts = array();
   $names = array();
   $colors = array();

   while ($row = mysql_fetch_array($res)) 
   {
      switch($type)
      {
          case 1:
              $p = $row["total_credit"];
              break;
          case 2:
              $p = $row["rac"];
              break;
      }
      $d = $row["name"];

      if ($cnt <= MAXPROJ) 
      {
         array_push($counts,$p);
         array_push($names,$d);
         array_push($colors,project_color($d));
      } else {
         $sum += $p;
      } 
      $cnt++; 
   }
   mysql_free_result($res);

   if ($cnt > MAXPROJ)
   {
      if ($cnt == MAXPROJ)
      {
         array_push($counts,$p);
         array_push($names,$d);
         array_push($colors,project_color($d));
      } else {
         array_push($counts,$sum);
         array_push($names,"Other"); 
         array_push($colors,project_color("other"));
      }
   }
   switch($type)
   {
        case 1:
           $title = "Total credit distribution";
           break;
        case 2:
           $title = "Current credit/day distribution";
           break;
   }

   // make the graph
   $graph = new PieGraph(375,300,"auto",5);
   //$graph->SetScale("textlin",$min,$max);
   //$graph->SetBackgroundGradient('blue','navy:0.5',GRAD_HOR,BGRAD_PLOT);
   // add a drop shadow
   $graph->SetShadow();
   //$graph->SetFrame(false);
   $graph->footer->left->set ("(C) 2007 http://boinc.netsoft-online.com/");
   
   $p1 = new PiePlot($counts);
   $p1->SetLegends($names);
   $p1->SetCenter(0.25,0.45);
   $p1->SetSlicecolors($colors);
   //$p1->SetStartAngle(45);
   $p1->value->SetFont(FF_FONT1,FS_BOLD);
   //$p1->value->SetColor("darkred");
   $p1->SetLabelPos(0.6);
   $graph->Add ($p1);

   // Setup the titles
   $graph->title->Set($title);

   // Display the graph
   $graph->Stroke();

}


$connect=mysql_connect($db_host,$db_login,$db_pass);
mysql_select_db($db_db);

if ($connect != 0)
{
    if ($type == 1 || $type == 2)
       do_pie_graph($cpid,$type);

    if ($type==3 || $type == 5 || $type == 7 || $type == 9)
       do_tc_graph($cpid,$type,0);

    if ($type == 4 || $type == 6 || $type == 8 || $type == 10)
       do_tc_graph($cpid,$type,$proj);

}
?>
