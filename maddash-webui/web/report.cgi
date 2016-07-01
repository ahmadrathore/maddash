#!/usr/bin/perl -T

use strict;
use CGI;
use URI::Escape;
 
my $cgi = new CGI();

print $cgi->header;

my $dashParam = $cgi->param("dashboard");
my $gridParam = $cgi->param("grid");
my $hostParam = $cgi->param("host");

my $type = "null";
my $name = "null";
my $host = $hostParam ? '"' . uri_escape($hostParam) . '"': "null";
if($dashParam && $gridParam){
	print "<h1>You cannot provide both a <i>dashboard</i> and <i>grid</i> parameter. Please only specify one.</h1>";
}elsif($dashParam){
	$type = "\"dashboard\"";
	$name = "\"$dashParam\"";
	print_page($type,$name, $host);
}elsif($gridParam){
	$type = "\"grid\"";
	$name = "\"$gridParam\"";
	print_page($type,$name, $host);
}else{
    print_page($type,$name, $host);
}

sub print_page(){
my($type,$name, $host) = @_;

print <<EOF;
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd">
<html>
	<head>
		<meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1">
		<title>MaDDash - Monitoring and Debugging Dashboard</title>
		<link rel="stylesheet" href="lib/dojo/dijit/themes/claro/claro.css" media="screen">
		<link rel="stylesheet" href="style/maddash-webui.css" media="screen">
		<link rel="stylesheet" href="style/maddash.css" media="screen">
		<link rel="stylesheet" href="style/tipsy.css" />
        <link rel="stylesheet" href="style/jquery.fancybox-1.3.4.css" />
	
	    <script src="lib/jquery.min.js"></script>
        <script src="lib/jquery-ui.min.js"></script>
        <script type="text/javascript" src="lib/jquery.tipsy.js"></script>
		<script src="lib/dojo/dojo/dojo.js" data-dojo-config="parseOnLoad: true"></script>
		<script src="lib/dojo/dojo/dojo-maddash.js"></script>
		<script src="lib/d3.v2.js"></script>
		<script type="text/javascript" src="lib/maddash.js"></script>
		<script type="text/javascript" src="lib/maddash-webui.js"></script>
		<script>
			require(["dijit/TitlePane","dijit/MenuBar","dijit/PopupMenuBarItem","dijit/MenuSeparator","dijit/DropDownMenu","dijit/MenuItem","dijit/layout/ContentPane","dojo/parser"]);
			
			function loadDashWidgets(){
			    var configDS = new MaDDashDataSource("etc/config.json", false);
			    var config = new MadDashConfig();
			    configDS.connect(config);
			    configDS.render();
			    var titlePane = new MadDashTitleSpan("maddashTitle", "index.cgi");
			    titlePane.render(config.data);
			    
			    //set custom click handler
			    var handleClick = function(d){
			        var href = "details.cgi?uri=" + d.uri;
                     window.open( href );
                }
			    var gs = new MaDDashDataSource("/maddash/grids"); 
				var ds = new MaDDashDataSource("/maddash/dashboards"); 
				var mnuds = new MaDDashDataSource("/maddash/dashboards"); 
				var refreshSource = null;
				if($type == "grid"){
				    gs.connect(new MaDDashReportPane("maddashDashboardPane", $type, $name, $host, config.data));
				    refreshSource = gs;
				}else{
				    ds.connect(new MaDDashReportPane("maddashDashboardPane", $type, $name, $host, config.data));
				    refreshSource = ds;
				}
				mnuds.connect(new MadDashNavMenu("maddashMenuBar", "index.cgi", config, {}, gs, refreshSource));
				refreshSource.connect(new MaDDashRefreshLabel("maddashRefreshStatus"));
				
				mnuds.render();
				ds.render();
			}
		</script>
	</head>
	<body class="claro" style="font-family:sans-serif" marginheight="0" marginwidth="0" onload="loadDashWidgets()">
		<div id="maddashTitle" class="maddashTitle"></div>
		<div id="maddashMenuBar"></div>
		<div id="maddashRefreshStatus"></div>
		<div id="maddashDashboardPane">
		    <img style='position:relative;left:49%;top:49%' height='20' width='20' class='loader' src='images/loader.gif'/>
		</div>
		<!-- add space for mouseover -->
	        <div id="footer" class="maddashFooter">
                </div>
                <div class="maddashFooterMoreInfo">
                  <br>
                  <hr width="90%">
                  More information on MaDDash available <a target="newwindow" href="http://software.es.net/maddash">here</a>
                </div>
	</body>
</html>
EOF

}

