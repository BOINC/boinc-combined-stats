<?php
// Remember that we must include class2.php
require_once("../../class2.php");

require_once("bp_functions.php");
include_lan(e_PLUGIN.'boinc/languages/'.e_LANGUAGE.'/lan_boinc.php');

   // Kick out the display to e107 engine
   require_once(HEADERF);
?>

<div class='dcaption'>
<div class='left'></div>
<div class='right'></div>
<div class='center'><?php echo LAN_100; ?></div>
</div>
<div class='dbody'>
<div class='leftwrapper'>
<div class='rightwrapper'>
<div class='leftcontent'></div>
<div class='rightcontent'></div>
<div class='dcenter'><div class='dinner'>
<?php echo LAN_101; ?>
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
<b><?php echo LAN_102;?></b>
<p/>
<p/><?php echo LAN_103;?>
<br/>
<span style="background-color:#000000; border: solid #666666 1px;">&nbsp;&nbsp;&nbsp;</span><?php echo LAN_104;?>
<br/>
<span style="background-color:#ffffff; border: solid #666666 1px;">&nbsp;&nbsp;&nbsp;</span>&lt; <?php echo LAN_105;?> 
<br/>
<span style="background-color:#f600c5; border: solid #666666 1px;">&nbsp;&nbsp;&nbsp;</span><?php echo LAN_106;?>
<br/>
<span style="background-color:#c302d4; border: solid #666666 1px;">&nbsp;&nbsp;&nbsp;</span><?php echo LAN_107;?>
<br/>
<span style="background-color:#0905a7; border: solid #666666 1px;">&nbsp;&nbsp;&nbsp;</span><?php echo LAN_108;?>
<br/>
<span style="background-color:#2afef2; border: solid #666666 1px;">&nbsp;&nbsp;&nbsp;</span><?php echo LAN_109;?>
<br/>
<span style="background-color:#02d4b4; border: solid #666666 1px;">&nbsp;&nbsp;&nbsp;</span><?php echo LAN_110;?>
<br/>
<span style="background-color:#61cc02; border: solid #666666 1px;">&nbsp;&nbsp;&nbsp;</span><?php echo LAN_111;?>
<br/>
<span style="background-color:#c4ee00; border: solid #666666 1px;">&nbsp;&nbsp;&nbsp;</span><?php echo LAN_112;?>
<br/>
<span style="background-color:#ffe721; border: solid #666666 1px;">&nbsp;&nbsp;&nbsp;</span><?php echo LAN_113;?>
<br/>
<span style="background-color:#fd9b3b; border: solid #666666 1px;">&nbsp;&nbsp;&nbsp;</span><?php echo LAN_114;?>
<br/>
<span style="background-color:#ff1509; border: solid #666666 1px;">&nbsp;&nbsp;&nbsp;</span><?php echo LAN_115;?>
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
   require_once(FOOTERF);
?>
