<?php
Header("Content-Type: image/png");
require('./jpgraph/jpgraph.php');
require('./jpgraph/jpgraph_bar.php');
require('./jpgraph/jpgraph_pie.php');
require('./jpgraph/jpgraph_line.php');
include("bp_functions.php");


$cpid = $_GET["cpid"];
if(isset($_GET["type"])) $type = $_GET["type"]; else return;
if(isset($_GET["project"])) $proj = $_GET["project"];


define("MAXPROJ",10);

function get_day_pointer($keytype, $dbhandle) {
	$query = "select value from currentday where key_item='$keytype'";
	$res = mysqli_query($dbhandle, $query);
	if (!$res) {
		exit();
	}
	$value = 0;
	
	while (($row = mysqli_fetch_array($res)))
	{
		$value = $row["value"];
	}
	
	return $value;
}


function do_tc_graph($cpid,$type,$proj, $dbhandle) {

	$tnow = time();
	
	if ($type == 7 || $type ==  8 || $type == 9 || $type == 10)
		$startday = get_day_pointer("ymm", $dbhandle);
	else 
		$startday = get_day_pointer("history90", $dbhandle);
	
	if ($startday == 0) {
		echo "Failed to get start counter<br>";
		return;
	}

	$graph = new Graph(600,300,"auto");
	$graph->SetScale("texlin");
	$graph->SetShadow();
	$graph->SetMarginColor('white');
	$graph->img->SetMargin(60,200,20,50);
	$graph->footer->left->set ("(C) 2009 http://boinc.netsoft-online.com/");
	$project_name="";
	$count = 0;
	$dataset = array();
	$dates = array();
	
	// get projects they are participating in
	// loop over each project to get the data
	if ($type == 3 || $type == 5 || $type == 7 || $type == 9)
		$query = "select b.shortname, b.name, b.project_color, a.user_id from cpid_map a, projects b where a.project_id=b.project_id and a.user_cpid='".$cpid."' and b.shown='Y'";
	else   
		$query = "select b.shortname, b.name, b.project_color, a.user_id from cpid_map a, projects b where a.project_id=b.project_id and a.user_cpid='".$cpid."' and b.shown='Y' and a.project_id=$proj";

	$res_loop = mysqli_query($dbhandle, $query);
	if (!$res_loop) {
		exit();
	}
   
	$points = array();
	while (($row_loop = mysqli_fetch_array($res_loop))) 
	{
		$project_name = $row_loop["name"];
		$project_color = $row_loop["project_color"];
		$project_userid = $row_loop["user_id"];
		$project_shortname = $row_loop["shortname"];
		
		$query = "select * from history_user_tc_$project_shortname where user_id=$project_userid";
	
		$res = mysqli_query($dbhandle, $query);
		if (!$res) {
			exit();
		}

		while (($row = mysqli_fetch_array($res)))
		{
			$prev = -1;
			$start = $startday + 1;
			if ($start > 91 && ($type >=3 && $type <= 6)) $start = 1;
			if ($start > 52 && ($type >=7 && $type <= 10)) $start = 1;

			$dataset = array();

			if ($type >=3 && $type <=6) 
				$max = 91;
			else
				$max = 120;
				
			for ($i=1;$i<=$max;$i++) 
			{
				if ($type == 7 || $type == 8 || $type == 9 || $type == 10)
					$rstring = "w_$start";
				else
					$rstring = "d_$start";

				$c = $row[$rstring];
				if ($type == 3 || $type == 4 || $type == 7 || $type == 8) {
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
				if ($start > 120 && ($type >=7 && $type <= 10)) $start = 1;
			}

			$point = array();
			array_push($point,$project_name);
			array_push($point,$project_color);
			array_push($point,$c);
			array_push($point,$dataset);
			array_push($points,$point);
			$count++;
		}
	}

	// Need to sort the array high to low, and keep the name and colors in line too...
	usort($points, "customsort");
	$cc = 0;
	$sum = false;
	$sumdataset = array();
	foreach ($points as $value) {
		if ($cc < MAXPROJ || $count == MAXPROJ) {
			$dplot[$cc] = new LinePLot($value[3]);
			$dplot[$cc]->SetFillColor($value[1]);
			$dplot[$cc]->SetLegend(substr($value[0],0,15));

		} else {
			$sum = true;
			foreach ($value[3] as $key => $value2) {
				if (array_key_exists($key,$sumdataset))
					$sumdataset[$key] += $value2;
				else 
					$sumdataset[$key] = $value2;
			}
		}
		$cc++;
	}
	if ($sum) {
		$dplot[MAXPROJ] = new LinePLot($sumdataset);
		$dplot[MAXPROJ]->SetFillColor("goldenrod1");
		$dplot[MAXPROJ]->SetLegend("Other");		
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

function customsort($a, $b) {
	if ($a[2] == $b[2])
		return 0;
	return ($a[2] < $b[2]) ? 1 : -1;	
}

function do_pie_graph($cpid,$type, $dbhandle) {

	// get projects they are participating in
	// loop over each project to get the data
	$query = "select b.shortname, b.name, b.project_color, a.user_id from cpid_map a, projects b where a.project_id=b.project_id and a.user_cpid='".$cpid."' and b.shown='Y'";   
	
	$res_loop = mysqli_query($dbhandle, $query);
	if (!$res_loop) {
		exit();
	}
	
	$cnt = 0;
	$points = array();
	$counts = array();
	$colors = array();
	$names = array();

	while (($row_loop = mysqli_fetch_array($res_loop))) {
		$project_name = $row_loop["name"];
		$project_shortname = $row_loop["shortname"];
		$user_id = $row_loop["user_id"];
		$project_color = $row_loop["project_color"];
		
		$query = "select total_credit, rac from users_$project_shortname where user_id=$user_id";

		$res = mysqli_query($dbhandle, $query);
		if (!$res) {
			exit();
		}

		while (($row = mysqli_fetch_array($res))) 
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
			
			$point = array();
			array_push($point,$project_name);
			array_push($point,$project_color);
			array_push($point,$p);
			array_push($points,$point);
			$cnt++; 
   		}
   		mysqli_free_result($res);
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

	// Need to sort the array high to low, and keep the name and colors in line too...
	usort($points, "customsort");
	$cc = 0;
	$sum = 0;
	foreach ($points as $value) {
		if ($cc < MAXPROJ || $cnt == MAXPROJ) {
			array_push($counts,$value[2]);
			array_push($colors,$value[1]);
			array_push($names,substr($value[0],0,15));
		} else {
			$sum = $sum + $value[2]; 
		}
		$cc++;
	}
	if ($sum > 0) {
		array_push($counts,$sum);
		array_push($colors,"goldenrod1");
		array_push($names,"Other");		
	}
	
	// make the graph
	$graph = new PieGraph(450,350,"auto",5);
	//$graph->SetScale("textlin",$min,$max);
	//$graph->SetBackgroundGradient('blue','navy:0.5',GRAD_HOR,BGRAD_PLOT);
	// add a drop shadow
	$graph->SetShadow();
	//$graph->SetFrame(false);
	$graph->footer->left->set ("(C) 2009 http://boinc.netsoft-online.com/");
	
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

$dbhandle = statsdb_connect();
if ($dbhandle)
{
	// type 1  = total credit distribution pie graph
	// type 2  = rac distribution pie graph
	// type 3  = 90 day history total credit all projects
	// type 4  = 90 day history total credit single project
	// type 5  = 89 day hist credit per day all projects
	// type 6  = 89 day hist credit per day single project
	// type 7  = 52 week history total credit all projects
	// type 8  = 52 week history total credit single project
	// type 9  = 52 week history credit per week all projects
	// type 10 = 52 week history credit per week single project
    if ($type == 1 || $type == 2)
       do_pie_graph($cpid,$type, $dbhandle);

    if ($type==3 || $type == 5 || $type == 7 || $type == 9)
       do_tc_graph($cpid,$type,0, $dbhandle);

    if ($type == 4 || $type == 6 || $type == 8 || $type == 10)
       do_tc_graph($cpid,$type,$proj, $dbhandle);

}
?>
