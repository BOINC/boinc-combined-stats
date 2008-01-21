<?
Header("Content-Type: image/png");
require('./jpgraph/jpgraph.php');
require('./jpgraph/jpgraph_line.php');
include("bp_functions.php");
//include_lan(e_PLUGIN.'boinc/languages/'.e_LANGUAGE.'/lan_boinc.php');

$project = $_GET["projectid"];
if (defined($_GET["type"])) $type = $_GET["type"];

$connect=mysql_connect($db_host,$db_login,$db_pass);
mysql_select_db($db_db);

if ($connect != 0)
{

   $query = "select name from b_projects where project_id=$project";
   $res = mysql_query($query);
   if (!$res) {
     exit();
   }

   $row = mysql_fetch_array($res);
   $project_name = $row["name"];
   mysql_free_result($res);
  
   $query = "select value from b_currentday where key_item='year'";
   $res = mysql_query($query);
   if (!$res) {
     exit();
   }

   $row = mysql_fetch_array($res);
   $startday = $row["value"];
   mysql_free_result($res);
  
   $fd = "";
   $ld = ""; 
   $min = "";
   $max = "";

   $query = "select * from  ";
   switch($type)
   {
       case 1:
          $query .="b_project_users_hist";
          $type_string = "User";
          break;
       case 2:
          $query .="b_project_hosts_hist";
          $type_string = "Host";
          break;
       case 3:
          $query .="b_project_teams_hist";
          $type_string = "Team";
          break;
       case 4:
          $query .="b_project_country_hist";
          $type_string = "Country";
          break;
       case 5:
          $query .="b_project_total_credit_hist";
          $type_string = "Credit granted per day";
          break;
       case 6:
          $query .="b_project_active_users_hist";
          $type_string = "Active User";
          break;
       case 7:
          $query .="b_project_active_hosts_hist";
          $type_string = "Active Host";
          break;
       case 8:
          $query .="b_project_active_teams_hist";
          $type_string = "Active Team";
          break;

      }
      
      $query .= " where project_id=$project ";


   $res = mysql_query($query);
   if (!$res) {
     exit();
   }

   $dates = array();
   $counts = array();
   $cnt=365;
   $prev=-1;
   $first = 0;
   $diff = 0;
   while ($row = mysql_fetch_array($res)) 
   {

	$start = $startday + 1;
   	if ($start > 365) $start = 1;
	$end = $startday;

	while ($start != $end) {
		$rstring = "d_$start";

		$c = $row[$rstring];
		if ($first == 0 ) {
			$min = $c;
			$max = $c;
			$first = 1;
		}  

		if ($type == 5) {
			// doing a diff graph
			if ($prev == -1 || $prev == 0) {
				$prev = $c;
				array_push($counts,0);
			} else {
				$diff = $c - $prev;
				if ($diff < 0) $diff = 0;
				array_push($counts,$diff);
				$prev = $c;
				if ($diff < $min) $min = $diff;
				if ($diff > $max) $max = $diff;
			}
		} else {
			if ($c < $min) $min = $c;
			if ($c > $max) $max = $c;
			array_push($counts,$c);
		}	
		$start = $start + 1;
		if ($start > 365) $start = 1;
	}      

   }
   
   mysql_free_result($res);

   switch($type)
   {
        case 1:
        case 2:
        case 3:
        case 4:
        case 6:
        case 7:
        case 8:
        case 9:
           $title = "$type_string counts for $project_name";
           break;
        case 5:
           $title = "$type_string for $project_name";
           break;
   }

   $tnow = time();
   $tstart = $tnow - (365*24*3600);
   if ($type == 5)
	$tstart += (24*3600);

   $fda = getdate($tstart);
   $lda = getdate($tnow);

   $xa =  $fda["month"] . " ".$fda["mday"].", ".$fda["year"];
   $xa .= " through ". $lda["month"] . " ".$lda["mday"].", ".$lda["year"]; 

   if ($min > 5000) 
      $min = $min - 1000;
   else
      $min = 0;
 
   if ($max > 5000) $max = $max + 1000;
   else $max = $max + 5;
   // make the graph
   $graph = new Graph(800,350,"auto"); 
   if ($type != 5) {
      $graph->SetScale("textlin",$min,$max);
   }
   else $graph->SetScale("textlin");

   //$graph->SetBackgroundGradient('blue','navy:0.5',GRAD_HOR,BGRAD_PLOT);
   // add a drop shadow
   $graph->SetShadow();

   // Adjust the margin a bit to make more room for titles
   if ($min > 1000000)
      $graph->img->SetMargin(100,30,20,40);
   else
      $graph->img->SetMargin(60,30,20,40);

   // Create a bar pot
   $bplot = new LinePlot($counts);
   
   //$graph->xaxis->SetTickLabels($dates);
   $graph->xaxis->SetTextLabelInterval(400);
   // Adjust fill color
   $color = project_color($project_name);
   $bplot->SetFillColor($color);
   //$bplot->SetWidth(0.6);
   //$bplot->SetShadow();
   //$bplot->value->Show();
   $graph->Add($bplot);

   // Setup the titles
   $graph->title->Set($title);
   $graph->xaxis->title->Set($xa);
   $graph->xaxis->title->Align('center');
   $graph->yaxis->title->Set("");

   $graph->title->SetFont(FF_FONT1,FS_BOLD);
   $graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
   $graph->yaxis->SetLabelFormatCallback('number_format');
   $graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);
   $graph->xaxis->SetTextLabelInterval(50);

   // Display the graph
   $graph->Stroke();
}
?>
