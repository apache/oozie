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

var columnsToShow = [
              { "mData": null,  "bSortable": false, "sWidth":"0.1%", "bVisible": true},
              { "mData": "id"},
              { "mData": "slaStatus"},
              { "mData": "nominalTimeTZ", "sDefaultContent": ""},
              { "mData": "expectedStartTZ", "sDefaultContent": ""},
              { "mData": "actualStartTZ", "sDefaultContent": "" },
              { "mData": "startDiff", "sDefaultContent": ""},
              { "mData": "expectedEndTZ"},
              { "mData": "actualEndTZ", "sDefaultContent": ""},
              { "mData": "endDiff", "sDefaultContent": ""},
              { "mData": "expectedDuration", "sDefaultContent": "", "bVisible": false},
              { "mData": "actualDuration", "sDefaultContent": "", "bVisible": false},
              { "mData": "durDiff", "sDefaultContent": "", "bVisible": false},
              { "mData": "slaMisses", "sDefaultContent": ""},
              { "mData": "jobStatus", "sDefaultContent": ""},
              { "mData": "parentId", "sDefaultContent": "", "bVisible": false},
              { "mData": "appName", "bVisible": false},
              { "mData": "slaAlertStatus", "bVisible": false},
             ];

$.fn.dataTableExt.oApi.fnGetTds  = function ( oSettings, mTr )
{
    var anTds = [];
    var anVisibleTds = [];
    var iCorrector = 0;
    var nTd, iColumn, iColumns;

    /* Take either a TR node or aoData index as the mTr property */
    var iRow = (typeof mTr == 'object') ?
        oSettings.oApi._fnNodeToDataIndex(oSettings, mTr) : mTr;
    var nTr = oSettings.aoData[iRow].nTr;

    /* Get an array of the visible TD elements */
    for ( iColumn=0, iColumns=nTr.childNodes.length ; iColumn<iColumns ; iColumn++ )
    {
        nTd = nTr.childNodes[iColumn];
        if ( nTd.nodeName.toUpperCase() == "TD" )
        {
            anVisibleTds.push( nTd );
        }
    }

    /* Construct and array of the combined elements */
    for ( iColumn=0, iColumns=oSettings.aoColumns.length ; iColumn<iColumns ; iColumn++ )
    {
        if ( oSettings.aoColumns[iColumn].bVisible )
        {
            anTds.push( anVisibleTds[iColumn-iCorrector] );
        }
        else
        {
            anTds.push( oSettings.aoData[iRow]._anHidden[iColumn] );
            iCorrector++;
        }
    }

    return anTds;
};

function initializeTable() {
    $('#sla_table').dataTable({
        "bJQueryUI": true,
        "sScrollX": "100%",
        "bFilter": false,
        "aoColumns": columnsToShow,
        /** scroll required as this is within extjs panel, change size according to table attributes **/
        "sScrollY": "360px",
        "bPaginate": true,
        "bStateSave": true,
        "aaSorting": [[ 3, 'desc' ]],
        "bDestroy": true
    });

}

function drawTable(jsonData) {
    var currentTime = new Date().getTime();

    for ( var i = 0; i < jsonData.slaSummaryList.length; i++) {
        var slaMisses = "";
        var slaSummary = jsonData.slaSummaryList[i];

        slaSummary.nominalTimeTZ = new Date(slaSummary.nominalTime).toUTCString();
        if (slaSummary.expectedStart) {
            slaSummary.expectedStartTZ = new Date(slaSummary.expectedStart).toUTCString();
            if (slaSummary.actualStart) {
                if (slaSummary.actualStart > slaSummary.expectedStart) {
                    slaMisses = "START_MISS, ";
                }
            }
            else if (currentTime > slaSummary.expectedStart) {
                slaMisses = "START_MISS, ";
            }
        }
        if (slaSummary.actualStart) {
            slaSummary.actualStartTZ = new Date(slaSummary.actualStart).toUTCString();
        }
        if (slaSummary.expectedEnd) {
            slaSummary.expectedEndTZ = new Date(slaSummary.expectedEnd).toUTCString();
            if (slaSummary.actualEnd) {
                if (slaSummary.actualEnd > slaSummary.expectedEnd) {
                    slaMisses += "END_MISS, ";
                }
            }
            else if (currentTime > slaSummary.expectedEnd) {
                slaMisses += "END_MISS, ";
            }
        }
        if (slaSummary.actualEnd) {
            slaSummary.actualEndTZ = new Date(slaSummary.actualEnd).toUTCString();
        }
        if (slaSummary.expectedStart && slaSummary.actualStart) {
            // timeElapsed in oozie-sla.js
            slaSummary.startDiff = slaSummary.actualStart - slaSummary.expectedStart;
        }
        if (slaSummary.expectedEnd && slaSummary.actualEnd) {
            slaSummary.endDiff = slaSummary.actualEnd - slaSummary.expectedEnd;
        }
        if (slaSummary.actualDuration != -1 && slaSummary.expectedDuration != -1) {
            slaSummary.durDiff = slaSummary.actualDuration - slaSummary.expectedDuration;
            if (slaSummary.actualDuration > slaSummary.expectedDuration) {
                slaMisses += "DURATION_MISS, ";
            }
        }
        slaSummary.slaMisses = slaMisses.length > 2 ? slaMisses.substring(0, slaMisses.length - 2) : "";
    }
    oTable = $('#sla_table').dataTable(
            {
                "bJQueryUI" : true,
                "sDom" : 'CT<"clear"> <"H"lfr>t<"F"ip>',
                "oColVis" : {
                    "buttonText" : "Show/Hide columns",
                    "bRestore" : true,
                    "aiExclude" : [ 0 ]
                },
                "bStateSave" : true,
                "sScrollY" : "360px",
                "sScrollX" : "100%",
                "bPaginate" : true,
                "sPaginationType": "full_numbers",
                "oTableTools" : {
                    "sSwfPath" : "console/sla/js/table/copy_csv_xls_pdf.swf",
                    "aButtons" : [
                                   "copy",
                                   {
                                       "sExtends": "csv",
                                       // Ignore column 0
                                       "mColumns": [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]
                                   },
                                 ],
                },
                "aaData" : jsonData.slaSummaryList,
                "aoColumns" : columnsToShow,
                "fnRowCallback" : function(nRow, aData, iDisplayIndex, iDisplayIndexFull) {
                    var rowAllColumns = this.fnGetTds(nRow);
                    $(rowAllColumns[1]).html(
                            '<a href="/oozie?job=' + aData.id + '" target="_blank">' + aData.id
                                    + '</a>');
                    $(rowAllColumns[15]).html(
                            '<a href="/oozie?job=' + aData.parentId + '" target="_blank">'
                                    + aData.parentId + '</a>');
                    if (aData.slaStatus == "MISS") {
                        $(rowAllColumns[2]).css('color', 'red');
                    }
                    // Changing only the html with readable text to preserve sort order.
                    if (aData.startDiff || aData.startDiff == 0) {
                        $(rowAllColumns[6]).html(timeElapsed(aData.startDiff));
                    }
                    if (aData.endDiff || aData.endDiff == 0) {
                        $(rowAllColumns[9]).html(timeElapsed(aData.endDiff));
                    }
                    if (aData.expectedDuration == -1) {
                        $(rowAllColumns[10]).html("");
                    } else {
                        $(rowAllColumns[10]).html(timeElapsed(aData.expectedDuration));
                    }
                    if (aData.actualDuration == -1) {
                        $(rowAllColumns[11]).html("");
                    } else {
                        $(rowAllColumns[11]).html(timeElapsed(aData.actualDuration));
                    }
                    if (aData.durDiff || aData.durDiff == 0) {
                        $(rowAllColumns[12]).html(timeElapsed(aData.durDiff));
                    }
                    $("td:first", nRow).html(iDisplayIndexFull + 1);
                    return nRow;
                },
                "aaSorting" : [ [ 3, 'desc' ] ],
                "bDestroy" : true
            });
}
