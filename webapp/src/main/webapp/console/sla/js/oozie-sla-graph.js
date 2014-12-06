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

function getPercentage(value, total) {
    if (value == 0 && total == 0) {
        return '(0%)';
    }
    return '(' + (Math.round(value/total * 100)) + '%)';
}

function showGraphicalSLAStats(jsonData) {

    // Do the calculation
    var slaStats = {
        startMet : 0,
        startMiss : 0,
        startNotStarted : 0,
        startMetInProcess : 0,
        startMetCompleted : 0,
        startMissNotStarted : 0,
        startMissInProcess : 0,
        startMissCompleted : 0,
        endMet : 0,
        endMiss : 0,
        endNotStarted : 0,
        endInProcess : 0,
        endMissNotStarted : 0,
        endMissInProcess : 0,
        endMissCompleted : 0,
        startTotal : 0,
        endTotal: jsonData.slaSummaryList.length
    };

    var plotValues = {
        nominalTime : [],
        expectedStart : [], //x-axis: Nominal Time, y-axis: Expected Start Time
        actualStart : [],   //x-axis: Nominal Time, y-axis: Actual Start Time
        expectedEnd : [],   //x-axis: Nominal Time, y-axis: Expected End Time
        actualEnd : [],     //x-axis: Nominal Time, y-axis: Actual End Time
    };

    // We are assuming response is ordered by nominal time
    var currentTime = new Date().getTime();

    for ( var i = 0; i < jsonData.slaSummaryList.length; i++) {
        var slaSummary = jsonData.slaSummaryList[i];
        plotValues.nominalTime[i] = slaSummary.nominalTime;
        plotValues.expectedStart[i] = [
                slaSummary.nominalTime,
                (slaSummary.expectedStart == null ? null : slaSummary.expectedStart - slaSummary.nominalTime) ];
        plotValues.actualStart[i] = [
                slaSummary.nominalTime,
                (slaSummary.actualStart == null ? null : slaSummary.actualStart - slaSummary.nominalTime) ];
        plotValues.expectedEnd[i] = [
                slaSummary.nominalTime,
                (slaSummary.expectedEnd - slaSummary.nominalTime) ];
        plotValues.actualEnd[i] = [
                slaSummary.nominalTime,
                (slaSummary.actualEnd == null ? null : slaSummary.actualEnd - slaSummary.nominalTime) ];

        if (slaSummary.expectedStart != null) {
            slaStats.startTotal++;
            if (slaSummary.actualStart == null) {
                if (currentTime < slaSummary.expectedStart) {
                    slaStats.startNotStarted++;
                } else {
                    slaStats.startMiss++;
                    slaStats.startMissNotStarted++;
                }
            } else if (slaSummary.actualStart <= slaSummary.expectedStart) {
                slaStats.startMet++;
                if (slaSummary.actualEnd == null) {
                    slaStats.startMetInProcess++;
                } else {
                    slaStats.startMetCompleted++;
                }
            } else {
                slaStats.startMiss++;
                if (slaSummary.actualEnd == null) {
                    slaStats.startMissInProcess++;
                } else {
                    slaStats.startMissCompleted++;
                }
            }
        }

        if (slaSummary.actualEnd == null) {
            if (currentTime > slaSummary.expectedEnd) {
                slaStats.endMiss++;
                if (slaSummary.actualStart == null) {
                    slaStats.endMissNotStarted++;
                } else {
                    slaStats.endMissInProcess++;
                }
            } else {
                if (slaSummary.actualStart == null) {
                    slaStats.endNotStarted++;
                } else {
                    slaStats.endInProcess++;
                }
            }
        } else if (slaSummary.actualEnd <= slaSummary.expectedEnd) {
            slaStats.endMet++;
        } else {
            slaStats.endMiss++;
            slaStats.endMissCompleted++;
        }
    }

    // Render the graph and stats table
    drawSLAGraph(jsonData, plotValues, slaStats.startTotal == 0 ? false : true);
    drawSLAStatsTable(jsonData, slaStats);

}

function drawSLAGraph(jsonData, plotValues, drawExpectedStart) {

    var datasetsToPlot = [{}];
    if (drawExpectedStart) {
        datasetsToPlot.push({label: "Expected Start", data:plotValues.expectedStart, color: 6});
    }
    datasetsToPlot.push({label: "Actual Start", data:plotValues.actualStart, color: "rgb(0,0,255)"});
    datasetsToPlot.push({label: "Expected End", data:plotValues.expectedEnd, color: 5});
    datasetsToPlot.push({label: "Actual End", data:plotValues.actualEnd, color: 25});

    var placeholder = $('#sla-graph-div');
    // TODO: Add support for timezone in future https://github.com/mde/timezone-js
    // timezoneId currently is not offset and is a string like America/Los_Angeles
    //var selectedTimezone = getTimeZone();
    //document.getElementById("sla-graph-xaxisLabel").innerHTML="<p>Nominal Time (in " + selectedTimezone + ")</p";

    var graphOptions = {
        series : {
            lines : {
                show : true
            },
            points : {
                show : true
            },
            shadowSize : 0
        },
        xaxis : {
            show : true,
            ticks: 12,
            // labelAngle: -60,
            mode : "time",
            timeformat : "%Y-%m-%d %H:%M",
            //timezone : selectedTimezone
            timezone: 'UTC'
        },
        yaxis : {
            show : true,
            ticks: 14,
            tickFormatter: timeSinceTickFormatter
        },
        grid : {
            hoverable : true,
            clickable : true
        },
        legend : {
            show : true,
            position : "nw"
        },
        zoom : {
            interactive : true,
            amount : 1.3
        },
        selection : {
            mode : "xy"
        }
    };

    function timeSinceTickFormatter(val, axis) {
        // oozie-sla.js
        return timeElapsed(val);
    }

    function plotGraph(placeholder, datasetsToPlot, options) {
        var slaGraphPlot = $.plot(placeholder, datasetsToPlot, options);

        // add Reset button
        $("<div class='button' style='right:50px;top:10px;text-align:center'>Reset Graph</div>").appendTo(placeholder)
                .click(function(event) {
                    event.preventDefault();
                    plotGraph(placeholder, datasetsToPlot, graphOptions);
                });

        // Add Zoom Out button
        $("<div class='button' style='right:59px;top:30px;text-align:center'>Zoom Out</div>").appendTo(placeholder)
                .click(function(event) {
                    event.preventDefault();
                    slaGraphPlot.zoomOut();
                });

        // Add panning buttons

        function addArrow(dir, right, top, offset) {
            $("<img class='button' src='console/sla/css/images/arrow-" + dir + ".gif' style='right:" +
                    right + "px;top:" + top + "px'>")
                .appendTo(placeholder)
                .click(function (e) {
                    e.preventDefault();
                    slaGraphPlot.pan(offset);
                });
        }

        addArrow("left", 85, 60, { left: 10 });
        addArrow("right", 55, 60, { left: -10 });
        addArrow("up", 70, 45, { top: 10 });
        addArrow("down", 70, 75, { top: -10 });


        placeholder.bind('plotselected', function(event, ranges) {

            // clamp the zooming to prevent eternal zoom
            if (ranges.xaxis.to - ranges.xaxis.from < 0.00001) {
                ranges.xaxis.to = ranges.xaxis.from + 0.00001;
            }
            if (ranges.yaxis.to - ranges.yaxis.from < 0.00001) {
                ranges.yaxis.to = ranges.yaxis.from + 0.00001;
            }
            // do the zooming
            plotGraph(placeholder, datasetsToPlot, $
                    .extend(true, {}, graphOptions, {
                        xaxis : {
                            min : ranges.xaxis.from,
                            max : ranges.xaxis.to
                        },
                        yaxis : {
                            min : ranges.yaxis.from,
                            max : ranges.yaxis.to
                        }
                    }));
        });


        placeholder.bind('plotclick',
            function (event, pos, item) {
                if (item) {
                    window.open('/oozie?job=' + jsonData.slaSummaryList[item.dataIndex].id);
                }
            }
        );

        placeholder.bind('plothover', function (event, pos, item) {
            if (item) {
                var pointIndex = item.dataIndex;
                var expectedStart = jsonData.slaSummaryList[pointIndex].expectedStart
                    ? new Date(jsonData.slaSummaryList[pointIndex].expectedStart).toUTCString()
                    : "";
                var actualStart = jsonData.slaSummaryList[pointIndex].actualStart
                    ? new Date(jsonData.slaSummaryList[pointIndex].actualStart).toUTCString()
                    : "";
                var actualEnd = jsonData.slaSummaryList[pointIndex].actualEnd
                    ? new Date(jsonData.slaSummaryList[pointIndex].actualEnd).toUTCString()
                    : "";
                $('#graphtooltip').html(
                        'Job ID : ' + jsonData.slaSummaryList[pointIndex].id + '<br />' +
                        'Parent ID : ' + jsonData.slaSummaryList[pointIndex].parentId + '<br />' +
                        'Nominal Time &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp : ' +
                            new Date(jsonData.slaSummaryList[pointIndex].nominalTime).toUTCString() + '<br />' +
                        'Expected Start Time : ' + expectedStart + '<br />' +
                        'Actual Start Time &nbsp;&nbsp;&nbsp : ' + actualStart + '<br />' +
                        'Expected End Time : ' +
                            new Date(jsonData.slaSummaryList[pointIndex].expectedEnd).toUTCString() + '<br />' +
                        'Actual End Time &nbsp;&nbsp;&nbsp;&nbsp : ' + actualEnd + '<br />' +
                        '<span style="font-size:10px;font-weight:bold;color:rgb(50%,50%,100%);">' +
                        '&nbsp;&nbsp;Double click or use mouse scrollwheel to zoom. <br />' +
                        '&nbsp;&nbsp;Use the arrow buttons to move. <br />' +
                        '&nbsp;&nbsp;Click on the data point to view job details</span>');
                var offset = item.pageY > 280 ? 300 : 150;
                var cssObj = {
                    'left': item.pageX + 'px',
                    'top': (item.pageY - offset) + 'px'
                };
                $('#graphtooltip').css(cssObj);
                $('#graphtooltip').show();
            } else {
                $('#graphtooltip').empty();
                $('#graphtooltip').hide();
            }
        });
    }

    plotGraph(placeholder, datasetsToPlot, graphOptions);

}


function drawSLAStatsTable(jsonData, slaStats) {

    var startStatsHtml = slaStats.startTotal == 0
        ? '<tr bgcolor="white"><td colspan="2">0/0 (0%)</td> <td colspan="3">0/0 (0%)</td> <td colspan="1">0/0 (0%)</td></tr>'
        : ('<tr bgcolor="white">' +
            '<td style="padding:4px" colspan="2">' + slaStats.startMet + '/' + slaStats.startTotal +
                getPercentage(slaStats.startMet, slaStats.startTotal) + '</td>' +
            '<td style="padding:4px" colspan="3">' + slaStats.startMiss + '/' + slaStats.startTotal +
                getPercentage(slaStats.startMiss, slaStats.startTotal) + '</td>' +
            '<td style="padding:4px" colspan="1">' + slaStats.startNotStarted + '/' + slaStats.startTotal +
                getPercentage(slaStats.startNotStarted, slaStats.startTotal) + '</td>' +
            '</tr>' +
            '<tr style=\'background-image: url("ext-2.2/resources/images/default/toolbar/bg.gif")\'>' +
            '<th style="padding:4px" colspan="1">In Process</th>' +
            '<th style="padding:4px" colspan="1">Completed</th>' +
            '<th style="padding:4px" colspan="1">Not Started</th>' +
            '<th style="padding:4px" colspan="1">In Process</th>' +
            '<th style="padding:4px" colspan="1">Completed</th>' +
            '<th style="padding:4px" colspan="1">&nbsp;</th>' +
            '<tr>' +
            '<tr bgcolor="white">' +
            '<td style="padding:4px" colspan="1">' + slaStats.startMetInProcess + '/' + slaStats.startMet +
                getPercentage(slaStats.startMetInProcess, slaStats.startMet) + '</td>' +
            '<td style="padding:4px" colspan="1">' + slaStats.startMetCompleted + '/' + slaStats.startMet +
                getPercentage(slaStats.startMetCompleted, slaStats.startMet) + '</td>' +
            '<td style="padding:4px" colspan="1">' + slaStats.startMissNotStarted + '/' + slaStats.startTotal +
                getPercentage(slaStats.startMissNotStarted, slaStats.startMiss) + '</td>' +
            '<td style="padding:4px" colspan="1">' + slaStats.startMissInProcess + '/' + slaStats.startMiss +
                getPercentage(slaStats.startMissInProcess, slaStats.startMiss) + '</td>' +
            '<td style="padding:4px" colspan="1">' + slaStats.startMissCompleted + '/' + slaStats.startMiss +
                getPercentage(slaStats.startMissCompleted, slaStats.startMiss) + '</td>' +
            '<td style="padding:4px" colspan="1">&nbsp;</td>' +
            '</tr>'
            );

     var tablehtml =
        '<table class="sla_table" cellspacing="0" border="3" width="100%">' +
            '<caption style="padding:5px;font-size:13px"><b>SLA Statistics</b></caption>' +
            '<tr style=\'background-image: url("ext-2.2/resources/images/default/toolbar/bg.gif")\'>' +
                '<th style="padding:4px;font-weight:bold" colspan="100%">Start SLA</th>' +
            '</tr>' +
            '<tr class="x-grid3-header">' +
                '<th style="padding:4px" colspan="2">Met</th>' +
                '<th style="padding:4px" colspan="3">Miss</th>' +
                '<th style="padding:4px" colspan="1">Not Started</th>' +
            '<tr>' +
             startStatsHtml +
            '<tr style=\'background-image: url("ext-2.2/resources/images/default/toolbar/bg.gif")\'>' +
                '<th style="padding:4px;font-weight:bold" colspan="100%">End SLA</th>' +
            '</tr>' +
            '<tr class="x-grid3-header">' +
                '<th style="padding:4px" colspan="1">Met</th>' +
                '<th style="padding:4px" colspan="3">Miss</th>' +
                '<th style="padding:4px" colspan="1">In Process</th>' +
                '<th style="padding:4px" colspan="1">Not Started</th>' +
            '</tr>' +
            '<tr bgcolor="white">' +
                '<td style="padding:4px" colspan="1">' + slaStats.endMet + '/' + slaStats.endTotal +
                    getPercentage(slaStats.endMet, slaStats.endTotal) + '</td>' +
                '<td style="padding:4px" colspan="3">' + slaStats.endMiss + '/' + slaStats.endTotal +
                    getPercentage(slaStats.endMiss, slaStats.endTotal) + '</td>' +
                '<td style="padding:4px" colspan="1">' + slaStats.endInProcess + '/' + slaStats.endTotal +
                    getPercentage(slaStats.endInProcess, slaStats.endTotal) + '</td>' +
                '<td style="padding:4px" colspan="1">' + slaStats.endNotStarted + '/' + slaStats.endTotal +
                    getPercentage(slaStats.endNotStarted, slaStats.endTotal) + '</td>' +
            '</tr>' +
            '<tr style=\'background-image: url("ext-2.2/resources/images/default/toolbar/bg.gif")\'>' +
            '<th style="padding:4px" colspan="1">&nbsp;</th>' +
            '<th style="padding:4px" colspan="1">Not Started</th>' +
            '<th style="padding:4px" colspan="1">In Process</th>' +
            '<th style="padding:4px" colspan="1">Completed</th>' +
            '<th style="padding:4px" colspan="1">&nbsp;</th>' +
            '<th style="padding:4px" colspan="1">&nbsp;</th>' +
            '<tr>' +
            '<tr bgcolor="white">' +
            '<td style="padding:4px" colspan="1">&nbsp;</td>' +
            '<td style="padding:4px" colspan="1">' + slaStats.endMissNotStarted + '/' + slaStats.endMiss +
                getPercentage(slaStats.endMissNotStarted, slaStats.endMiss) + '</td>' +
            '<td style="padding:4px" colspan="1">' + slaStats.endMissInProcess + '/' + slaStats.endMiss +
                getPercentage(slaStats.endMissInProcess, slaStats.endMiss) + '</td>' +
            '<td style="padding:4px" colspan="1">' + slaStats.endMissCompleted + '/' + slaStats.endMiss +
                getPercentage(slaStats.endMissCompleted, slaStats.endMiss) + '</td>' +
            '<td style="padding:4px" colspan="1">&nbsp;</td>' +
            '<td style="padding:4px" colspan="1">&nbsp;</td>' +
            '</tr>' +
        '</table>';
    $('#sla-table-div').html(tablehtml);
}
