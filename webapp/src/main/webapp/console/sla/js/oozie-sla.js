/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

function initializeDatePicker() {
    $("#startDate").datetimepicker({
        dateFormat: 'yy-mm-dd',
   });

   $("#endDate").datetimepicker({
        dateFormat: 'yy-mm-dd',
   });
}

function onSearchClick(){
    var queryParams = null;
    var filter = "filter=";
    var appName = $("#app_name").val();
    var jobId = $("#job_id").val();
    var nominalStart = $("#startDate").val();
    var nominalEnd = $("#endDate").val();

    if (appName == "" && jobId == "") {
        alert("AppName or JobId is required");
    }
    else if (appName != "" && jobId != "") {
        alert("Enter only one of AppName or JobId");
    }
    else {
        if (appName != "") {
            queryParams = filter+"app_name="+appName;
        }
        else if (jobId != "") {
            queryParams = filter+"id="+jobId+";parent_id="+jobId;
        }
        if (nominalStart != "") {
            var splitNominalStart = nominalStart.split(" ");
            queryParams += ";nominal_start="+splitNominalStart[0]+"T"+splitNominalStart[1]+"Z";
        }
        if (nominalEnd != "") {
            var splitNominalEnd = nominalEnd.split(" ");
            queryParams += ";nominal_end="+splitNominalEnd[0]+"T"+splitNominalEnd[1]+"Z";
        }
        fetchData(queryParams);
    }
}

function fetchData(queryParams) {
    $.ajax({
        type : 'GET',
        url : getOozieBase() + 'sla',
        data: queryParams,
        dataType : 'json',
        timeout : 120000, // 2 mins
        success : function(jsonData) {
            if (!jsonData || jsonData.slaSummaryList.length == 0) {
                alert ("No matching data found");
            }
            else {
                buildTableAndGraph(jsonData);
            }
        },
        error : function(response) {
            alert("Error: " + response.statusText);
        }
    });
}

function getSelectedTabIndex() {
    return $('#tabs').tabs().tabs('option', 'active');
}

function buildTableAndGraph(jsonData) {
    var isTableDrawn = 0;
    var isGraphDrawn = 0;
    if (getSelectedTabIndex() == '0'){
        $("#sla_table").show();
        drawTable(jsonData);
        isTableDrawn = 1;
    }
    else if (getSelectedTabIndex() == '1') {
        $("#sla-graph-uber-container").show();
        showGraphicalSLAStats(jsonData);
        isGraphDrawn = 1;
    }
    $('#tabs').tabs({
        activate: function(event, ui) {
            if (ui.newTab.index() == '1') {
                $("#Table ").hide();
                if (isGraphDrawn == 0) {
                    $("#sla-graph-uber-container").show();
                    showGraphicalSLAStats(jsonData);
                    isGraphDrawn = 1;
                }
                $("#Graph").show();
            }

            if (ui.newTab.index() == '0') {
                if (isTableDrawn == 0) {
                    $("#sla_table").show();
                    drawTable(jsonData);
                    isTableDrawn = 1;
                }
                $("#Table").show();
                /* Needed otherwise width of column header will be resized */
                oTable.fnAdjustColumnSizing();
                $("#Graph").hide();
            }
        }
    });
}

function timeElapsed(timeInMillis) {

    if (!timeInMillis) {
        if (timeInMillis == 0) {
            return "00:00:00";
        }
        return;
    }

    var timeSince = "";
    var addHour = true;
    var addMin = true;
    if (timeInMillis < 0) {
        timeSince = "-";
        timeInMillis = -timeInMillis;
    }
    var seconds = Math.floor(timeInMillis/1000);
    interval = Math.floor(seconds / 31536000);

    if (interval > 0) {
        timeSince += interval + "y ";
        seconds = (seconds %= 31536000);
    }

    interval = Math.floor(seconds / 2592000);
    if (interval > 0) {
        timeSince += interval + "m ";
        seconds = (seconds %= 2592000);
    }

    interval = Math.floor(seconds / 86400);
    if (interval > 0) {
        timeSince += interval + "d ";
        seconds = (seconds %= 86400);
    }

    interval = Math.floor(seconds / 3600);
    if (interval > 0) {
        if (interval < 10)
            timeSince += "0";
        timeSince += interval + ":";
        seconds = (seconds %= 3600);
        addHour = false;
    }

    interval = Math.floor(seconds / 60);
    if (interval > 0) {
        if (addHour) {
            timeSince += "00:";
        }
        addHour = false;
        if (interval < 10)
            timeSince += "0";
        timeSince += interval + ":";
        seconds = (seconds %= 60);
        addMin = false;
    }
    if (addHour) {
        timeSince += "00:";
    }
    if (addMin) {
        timeSince += "00:";
    }
    if (seconds < 10)
        timeSince += "0";
    return timeSince + seconds;
}