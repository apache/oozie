<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
        <title>Oozie Web Console</title>
        <link rel="stylesheet" type="text/css" href="ext-2.2/resources/css/ext-all.css"/>
        <link rel="stylesheet" type="text/css" href="ext-2.2/resources/css/xtheme-default.css"/>
        <link rel="stylesheet" type="text/css" href="oozie-console.css"/>

        <!-- jquery needs to be before extjs -->
        <script type="text/javascript" charset="utf8" src="console/sla/js/table/jquery-1.8.3.min.js"></script>
        <link rel="stylesheet" type="text/css" href="console/sla/css/jquery.dataTables.css">
        <script type="text/javascript" src="console/sla/js/table/jquery.dataTables.min.js"></script>
        <script type="text/javascript" src="console/sla/js/table/jquery-ui-1.10.3.custom.min.js"></script>
        <script type="text/javascript" src="console/sla/js/table/jquery-ui-timepicker-addon.js"></script>
        <script type="text/javascript" src="console/sla/js/graph/jquery.flot.min.js"></script>
        <script type="text/javascript" src="console/sla/js/oozie-sla.js"></script>

        <script type="text/javascript" src="ext-2.2/adapter/ext/ext-base.js"></script>
        <script type="text/javascript" src="ext-2.2/ext-all.js"></script>
        <script type="text/javascript" src="ext-2.2/examples/grid/RowExpander.js"></script>
        <script type="text/javascript" src="json2.js"></script>
        <script type="text/javascript" src="oozie-console.js"></script>

    </head>
    <body>
        <div id="dependencies" style="display:none;color:red">
            <p><strong>Oozie web console is disabled.</strong></p>
            <p>To enable Oozie web console install the Ext JS library.</p>
            <p>Refer to <a href="./docs/DG_QuickStart.html">Oozie Quick Start</a> documentation for details.</p>
            <hr />
        </div>
        <!-- LIBS -->

        <div id="Header" style="padding:2">
           <img src="./oozie_50x.png" height="16" width="70"/>
           <a href="./docs/index.html" target="bottom">Documentation</a>
        </div>
        <%@ page
            import="org.apache.oozie.sla.service.SLAService"
            import="org.apache.oozie.service.InstrumentationService"
            import="org.apache.oozie.service.MetricsInstrumentationService"
        %>
        <%
            boolean isSLAServiceEnabled = SLAService.isEnabled();
            boolean isInstrumentationServiceEnabled = InstrumentationService.isEnabled();
            boolean isMetricsInstrumentationServiceEnabled = MetricsInstrumentationService.isEnabled();
        %>
        <div id="oozie-body" style="padding:2">
            <div class="x-tab-panel-header x-unselectable x-tab-strip-top" style="width:1048">
               <span style="font-family:tahoma,arial,helvetica; font-size:11px;font-weight: bold; color: #15428B;">
                 <script type="text/javascript">
                    var msg = "Oozie Web Console";
                    var isSLAServiceEnabled = "<%=isSLAServiceEnabled%>";
                    var isInstrumentationServiceEnabled = "<%=isInstrumentationServiceEnabled%>";
                    var isMetricsInstrumentationServiceEnabled = "<%=isMetricsInstrumentationServiceEnabled%>";
                    document.title = msg;
                    document.write(msg);
                 </script>
               </span>
            </div>
            <div id="oozie-console"></div>
            <div id="info"> </div>
        </div>
    </body>
</html>