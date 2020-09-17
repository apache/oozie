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
   $(".datepicker").datetimepicker({
           dateFormat: 'yy-mm-dd'
   });
}

function onSearchClick(){
    var queryParams = "";
    var elements = document.querySelectorAll("#inputArea input")

    for (var i = 0; i < elements.length; i++) {
        if(elements[i].value != "" && elements[i].id != "job_id") {
            if (i!=0) {
                queryParams += ";";
            }
            if (elements[i].classList.contains("datepicker")) {
                var splitDate = elements[i].value.split(" ");
                queryParams += elements[i].id + "=" + splitDate[0] + "T" + splitDate[1] + "Z";
            } else {
                queryParams += elements[i].id + "=" + encodeURIComponent(elements[i].value);
            }
        }
    }

    var appName = $("#app_name").val();
    var jobId = $("#job_id").val();

    if (appName == "" && jobId == "") {
        alert("AppName or JobId is required");
    }
    else {
        if (jobId != "") {
            if (queryParams.length>0) {
                queryParams += ";";
            }
            queryParams += "id=" + jobId + ";parent_id=" + jobId;
        }
        fetchData("filter="+queryParams);
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