<?php
// Remember that we must include class2.php
require_once("../../class2.php");

require_once("bp_functions.php");
//@include_once(e_PLUGIN."bp/languages/".e_LANGUAGE.".php");
//@include_once(e_PLUGIN."bp/languages/English.php");

   // Kick out the display to e107 engine
   require_once(HEADERF);
?>

<div class='dcaption'>
<div class='left'></div>
<div class='right'></div>
<div class='center'>World BOINC Activity</div>
</div>
<div class='dbody'>
<div class='leftwrapper'>
<div class='rightwrapper'>
<div class='leftcontent'></div>
<div class='rightcontent'></div>
<div class='dcenter'><div class='dinner'>
This map shows the current global BOINC activity in GigaFLOPS. The number shown when you mouse over a country is the current GigaFLOPS produced by BOINC participents of that country.
You can zoom in on the map by holding down the left mouse button and draging the mouse over the area you wish to zoom in to.

<br />
<br />
<center>
<object type="application/x-shockwave-flash" data = "world.swf?data_file=country_rac.php" 
width="650" height="400" id="zoom_map" align="top">
<param name="movie" value="world.swf?data_file=country_rac.php" />
<param name="quality" value="high" />
<param name="bgcolor" value="#FFFFFF" />
</object>
</center>

<br />
<b>Key:</b>
<p/>
<p>Values are in GigaFLOPS
<br/>
<span style="background-color:#000000; border: solid #666666 1px;">&nbsp;&nbsp;&nbsp;</span>No data
<br/>
<span style="background-color:#ffffff; border: solid #666666 1px;">&nbsp;&nbsp;&nbsp;</span>&lt; 1 
<br/>
<span style="background-color:#f600c5; border: solid #666666 1px;">&nbsp;&nbsp;&nbsp;</span>1 - 50
<br/>
<span style="background-color:#c302d4; border: solid #666666 1px;">&nbsp;&nbsp;&nbsp;</span>51 - 100
<br/>
<span style="background-color:#0905a7; border: solid #666666 1px;">&nbsp;&nbsp;&nbsp;</span>101 - 500 
<br/>
<span style="background-color:#2afef2; border: solid #666666 1px;">&nbsp;&nbsp;&nbsp;</span>501 - 1,000 
<br/>
<span style="background-color:#02d4b4; border: solid #666666 1px;">&nbsp;&nbsp;&nbsp;</span>1,001 - 5,000
<br/>
<span style="background-color:#61cc02; border: solid #666666 1px;">&nbsp;&nbsp;&nbsp;</span>5,001 - 10,000
<br/>
<span style="background-color:#c4ee00; border: solid #666666 1px;">&nbsp;&nbsp;&nbsp;</span>10,001 - 20,000
<br/>
<span style="background-color:#ffe721; border: solid #666666 1px;">&nbsp;&nbsp;&nbsp;</span>20,001 - 50,000
<br/>
<span style="background-color:#fd9b3b; border: solid #666666 1px;">&nbsp;&nbsp;&nbsp;</span>50,001 - 100,000
<br/>
<span style="background-color:#ff1509; border: solid #666666 1px;">&nbsp;&nbsp;&nbsp;</span>100,001+ 
</p>

</div></div>
</div>
</div>
</div>
<div class='dbottom'>
<div class='left'></div>
<div class='right'></div>
<div class='center'>
</div>
</div>

<?php


   //$ns->tablerender("Project granted credit comparison", $data);
   require_once(FOOTERF);

?>
