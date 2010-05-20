<?
Header ( "Content-Type: image/png" );
require ('./jpgraph/jpgraph.php');
require ('./jpgraph/jpgraph_line.php');
include ("bp_functions.php");

$project = $_GET ["projectid"];
$type = $_GET ["type"];

$dbhandle = statsdb_connect ();
if ($dbhandle) {
	
	$query = "select name, project_color from projects where project_id=$project";
	$res = mysqli_query ( $dbhandle, $query );
	if (! $res) {
		exit ();
	}
	
	$row = mysqli_fetch_array ( $res );
	$project_name = $row ["name"];
	$project_color = $row ["project_color"];
	mysqli_free_result ( $res );
	
	$query = "select value from currentday where key_item='history90'";
	$res = mysqli_query ( $dbhandle, $query );
	if (! $res) {
		exit ();
	}
	
	$row = mysqli_fetch_array ( $res );
	$startday = $row ["value"];
	mysqli_free_result ( $res );
	
	$fd = "";
	$ld = "";
	$min = "";
	$max = "";
	
	$query = "select * from  ";
	switch ($type) {
		case 1 :
			$query .= "history_projects_users";
			$type_string = "User";
			break;
		case 2 :
			$query .= "history_projects_hosts";
			$type_string = "Host";
			break;
		case 3 :
			$query .= "history_projects_teams";
			$type_string = "Team";
			break;
		case 4 : // Was country count, didn't bring it forward
			$query .= "history_projects_rac";
			$type_string = "Recent Average Credit";
			break;
		case 5 :
			$query .= "history_project_tc";
			$type_string = "Credit granted per day";
			break;
		case 6 :
			$query .= "history_projects_ausers";
			$type_string = "Active User";
			break;
		case 7 :
			$query .= "history_projects_ahosts";
			$type_string = "Active Host";
			break;
		case 8 :
			$query .= "history_projects_ateams";
			$type_string = "Active Team";
			break;
	}
	
	$query .= " where project_id=$project ";
	
	$res = mysqli_query ( $dbhandle, $query );
	if (! $res) {
		exit ();
	}
	
	$dates = array ();
	$counts = array ();
	$cnt = 91;
	$prev = - 1;
	$first = 0;
	$diff = 0;
	while ( ($row = mysqli_fetch_array ( $res )) ) {
		
		$start = $startday + 1;
		if ($start > 91)
			$start = 1;
		$end = $startday;
		
		for($i = 1; $i < 91; $i ++) {
			$rstring = "d_$start";
			
			$c = $row [$rstring];
			if ($first == 0) {
				$min = $c;
				$max = $c;
				$first = 1;
			}
			
			if ($type == 5) {
				// doing a diff graph
				if ($prev == - 1 || $prev == 0) {
					$prev = $c;
					array_push ( $counts, 0 );
				} else {
					$diff = $c - $prev;
					if ($diff < 0)
						$diff = 0;
					array_push ( $counts, $diff );
					$prev = $c;
					if ($diff < $min)
						$min = $diff;
					if ($diff > $max)
						$max = $diff;
				}
			} else {
				if ($c < $min)
					$min = $c;
				if ($c > $max)
					$max = $c;
				array_push ( $counts, $c );
			}
			$start = $start + 1;
			if ($start > 91)
				$start = 1;
		}
	}
	
	mysqli_free_result ( $res );
}

switch ($type) {
	case 1 :
	case 2 :
	case 3 :
	case 4 :
	case 6 :
	case 7 :
	case 8 :
	case 9 :
		$title = "$type_string counts for $project_name";
		break;
	case 5 :
		$title = "$type_string for $project_name";
		break;
}

$tnow = time ();
$tstart = $tnow - (91 * 24 * 3600);
if ($type == 5)
	$tstart += (24 * 3600);

$fda = getdate ( $tstart );
$lda = getdate ( $tnow );

$xa = $fda ["month"] . " " . $fda ["mday"] . ", " . $fda ["year"];
$xa .= " through " . $lda ["month"] . " " . $lda ["mday"] . ", " . $lda ["year"];

if ($min > 5000)
	$min = $min - 1000;
else
	$min = 0;

if ($max > 5000)
	$max = $max + 1000;
else
	$max = $max + 5;
	// make the graph
$graph = new Graph ( 800, 350, "auto" );
if ($type != 5) {
	$graph->SetScale ( "textlin", $min, $max );
} else
	$graph->SetScale ( "textlin" );
	
//$graph->SetBackgroundGradient('blue','navy:0.5',GRAD_HOR,BGRAD_PLOT);
// add a drop shadow
$graph->SetShadow ();

// Adjust the margin a bit to make more room for titles
if ($min > 1000000)
	$graph->img->SetMargin ( 100, 30, 20, 40 );
else
	$graph->img->SetMargin ( 60, 30, 20, 40 );
	
// Create a bar pot
$bplot = new LinePlot ( $counts );

//$graph->xaxis->SetTickLabels($dates);
$graph->xaxis->SetTextLabelInterval ( 400 );
// Adjust fill color
$color = $project_color;
$bplot->SetFillColor ( $color );
//$bplot->SetWidth(0.6);
//$bplot->SetShadow();
//$bplot->value->Show();
$graph->Add ( $bplot );

// Setup the titles
$graph->title->Set ( $title );
$graph->xaxis->title->Set ( $xa );
$graph->xaxis->title->Align ( 'center' );
$graph->yaxis->title->Set ( "" );

$graph->title->SetFont ( FF_FONT1, FS_BOLD );
$graph->yaxis->title->SetFont ( FF_FONT1, FS_BOLD );
$graph->yaxis->SetLabelFormatCallback ( 'number_format' );
$graph->xaxis->title->SetFont ( FF_FONT1, FS_BOLD );
$graph->xaxis->SetTextLabelInterval ( 50 );

// Display the graph
$graph->Stroke ();

?>
