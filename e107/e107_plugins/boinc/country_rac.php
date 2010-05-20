<?php
header ( 'Content-type: text/xml' );
print "<?xml version=\"1.0\" encoding=\"iso-8859-1\"?>\n";

require_once ("bp_functions.php");

?>


<countrydata>

<state id="range">
<data>0</data>
<color>ffffff</color>
</state>

<state id="range">
<data>1 - 50</data>
<color>f600c5</color>
</state>

<state id="range">
<data>51 - 100</data>
<color>c302d4</color>
</state>

<state id="range">
<data>101 - 500</data>
<color>0905a7</color>
</state>

<state id="range">
<data>501 - 1000</data>
<color>2afef2</color>
</state>

<state id="range">
<data>1001 - 5000</data>
<color>02d4b4</color>
</state>

<state id="range">
<data>5001 - 10000</data>
<color>61cc02</color>
</state>

<state id="range">
<data>10001 - 20000</data>
<color>c4ee00</color>
</state>

<state id="range">
<data>20001 - 50000</data>
<color>ffe721</color>
</state>

<state id="range">
<data>50001 - 100000</data>
<color>fd9b3b</color>
</state>

<state id="range">
<data>100001 - 99999999</data>
<color>ff1509</color>
</state>

<state id="default_color">
<color>000000</color>
</state>

<state id="background_color">
<color>ffffff</color>
</state>

<state id="outline_color">
<color>330000</color>
</state>

<?php

// connect to dtabase
$dbhandle = statsdb_connect ();

if ($dbhandle) {
	
	$query = "select a.diymap_code, a.country, b.rac from country a, country_rank b where a.country_id=b.country_id and a.diymap_code is not null order by a.diymap_code";
	
	$res = mysqli_query ( $dbhandle, $query );
	
	if (! $res) {
		echo "<error>Error performing query: " . mysqli_error ( $dbhandle ) . "</error>\n";
		echo "</countrydata>\n";
		exit ();
	}
	
	$prev = "";
	
	while ( $row = mysqli_fetch_array ( $res ) ) {
		$code = $row ["diymap_code"];
		$country = $row ["country"];
		$rac = $row ["rac"];
		
		$rac = round ( $rac / 100 );
		$rac_f = number_format ( $rac );
		
		if ($prev !== $code)
			echo "<state id=\"$code\">\n  <name>$country</name>\n  <data>$rac</data>\n  <hover>$rac_f GigaFLOPS</hover>\n</state>\n";
		$prev = $code;
	}
	
	echo "</countrydata>\n";
}

?>
