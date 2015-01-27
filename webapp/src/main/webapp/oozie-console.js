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

// Warn about dependencies; this has to be done on .ready so that the divs will all be loaded and exist.  Otherwise, it won't find
// the 'dependencies' element
$(document).ready(function() {
    if (typeof Ext == 'undefined'){
        var warning = 'Missing JavaScript dependencies.';
        var dependencies = document.getElementById('dependencies');
        if (dependencies){
            warning += "\n" + (dependencies.innerText || dependencies.textContent);
            dependencies.style.display = '';
        }
        throw new Error(warning);
    }
});

Ext.override(Ext.Component, {
    saveState : function() {
        if (Ext.state.Manager && this.stateful !== false) {
            var state = this.getState();
            if (this.fireEvent('beforestatesave', this, state) !== false) {
                Ext.state.Manager.set(this.stateId || this.id, state);
                this.fireEvent('statesave', this, state);
            }
        }
    },
    stateful : false
});

function getLogs(url, searchFilter, logStatus, textArea, shouldParseResponse, errorMsg) {
    textArea.getEl().dom.value = '';
    if (searchFilter) {
        url = url + "&logfilter=" + searchFilter;
    }
    logStatus.getEl().dom.innerText = "Log status : Loading... done";

    if (!errorMsg) {
        errorMsg = "Fatal Error. Can't load logs.";
    }
    if (!window.XMLHttpRequest) {
        Ext.Ajax.request({
            url : url,
            timeout : 300000,
            success : function(response, request) {
                if (shouldParseResponse) {
                    processAndDisplayLog(response.responseText, textArea);
                    logStatus.getEl().dom.innerText = "Log status : Loading... done";

                } else {
                    textArea.getEl().dom.value = response.responseText;
                    logStatus.getEl().dom.innerText = "Log status : Loading... done";
                }
            },

            failure : function() {
                textArea.getEl().dom.value = errorMsg;
                logStatus.getEl().dom.innerText = "Log status : Errored out";
            }
        });

    } else {
        var xhr = new XMLHttpRequest();
        xhr.previous_text_length = 0;

        xhr.onerror = function() {
            textArea.getEl().dom.value = errorMsg;
        };
        xhr.onreadystatechange = function() {
            try {
                if (xhr.readyState > 2  && xhr.status == 200) {
                    var new_response = xhr.responseText
                            .substring(xhr.previous_text_length);
                    textArea.getEl().dom.value += new_response;
                    xhr.previous_text_length = xhr.responseText.length;

                }
                if (xhr.status != 200 && xhr.status != 0) {
                    var errorText = xhr.getResponseHeader('oozie-error-message');
                    textArea.getEl().dom.value = "Error :\n" + (errorText ? errorText : xhr.responseText);
                    logStatus.getEl().dom.innerText = "Log status : Errored out";

                }
                if (xhr.readyState =4  && xhr.status == 200) {
                    logStatus.getEl().dom.innerText = "Log status : Loading... done";
                }
            } catch (e) {
            }
        };
        xhr.open("GET", url, true);
        xhr.send();
    }
}

function processAndDisplayLog(response, textArea) {
    var responseLength = response.length;
    var twentyFiveMB = 25 * 1024 * 1024;
    if (responseLength > twentyFiveMB) {
        response = response.substring(responseLength - twentyFiveMB,
                responseLength);
        response = response.substring(response.indexOf("\n") + 1,
                responseLength);
        textArea.getEl().dom.value = response;
    } else {
        textArea.getEl().dom.value = response;
    }
}

var oozie_host = "";
var flattenedObject;

function getOozieClientVersion() {
    return 2;
}

function getOozieVersionsUrl() {
    var ctxtStr = location.pathname;
    return oozie_host + ctxtStr + "versions";
}

function getOozieBase() {
    var ctxtStr = location.pathname;
    return oozie_host + ctxtStr.replace(/[-]*console/, "") + "v" + getOozieClientVersion() + "/";
}

function getReqParam(name) {
    name = name.replace(/[\[]/, "\\\[").replace(/[\]]/, "\\\]");
    var regexS = "[\\?&]" + name + "=([^&#]*)";
    var regex = new RegExp(regexS);
    var results = regex.exec(window.location.href);
    if (results == null) {
        return "";
    }
    else {
        return results[1];
    }
}

// renderer functions
function valueRenderer(value, metadata, record, row, col, store) {
    if (value.length > 60) {
        return value.substring(0, 60) + " ...";
    }
    else {
        return value;
    }
}

function dateTime(value, metadata, record, row, col, store) {
    return value;
}

function checkUrl(value, metadata, record, row, col, store) {
    if (value != null) {
        return "Y";
    }
    else {
        return "N";
    }
}

function getTimeZone() {
    Ext.state.Manager.setProvider(new Ext.state.CookieProvider());
    return Ext.state.Manager.get("TimezoneId","GMT");
}

function getUserName() {
    Ext.state.Manager.setProvider(new Ext.state.CookieProvider());
    return Ext.state.Manager.get("UserName", null);
}

function getCustomFilter() {
    var filter = '';
    var userName = getUserName();
    if(userName) {
        filter = "user=" + userName;
    }
    Ext.state.Manager.setProvider(new Ext.state.CookieProvider());
    var customFilter = Ext.state.Manager.get("GlobalCustomFilter", null);
    if (customFilter) {
       if (filter) {
           filter = filter + ";" + customFilter;
       } else {
           filter = customFilter;
       }
    }
    return filter;
}

function convertStatusToUpperCase(filterText) {
    var converted = filterText.replace(/status=([a-zA-Z]+)/g, function(){
          var text = arguments[1];
          return "status="+ text.toUpperCase();
    });
    return converted;
}

if ( !String.prototype.endsWith ) {
    String.prototype.endsWith = function(pattern) {
        var d = this.length - pattern.length;
        return d >= 0 && this.lastIndexOf(pattern) === d;
    };
}

// Makes a tree node from an XML
function treeNodeFromXml(XmlEl) {
    var t = ((XmlEl.nodeType == 3) ? XmlEl.nodeValue : XmlEl.tagName);
    if (t.replace(/\s/g, '').length == 0) {
        return null;
    }
    var result = new Ext.tree.TreeNode({
        text: t
    });
    //  For Elements, process attributes and children
    if (XmlEl.nodeType == 1) {
        Ext.each(XmlEl.attributes, function(a) {
            result.appendChild(new Ext.tree.TreeNode({
                text: a.nodeName
            })).appendChild(new Ext.tree.TreeNode({
                text: a.nodeValue
            }));
        });
        Ext.each(XmlEl.childNodes, function(el) {
            var c = treeNodeFromXml(el);
            if (c)
                result.appendChild(c);
        });
    }
    return result;
}

function treeNodeFromJsonInstrumentation(json, rootText) {
    var result = new Ext.tree.TreeNode({
        text: rootText
    });
    //  For Elements, process attributes and children
    if (typeof json === 'object') {
        for (var i in json) {
            if (json[i]) {
                if (typeof json[i] == 'object') {
                    var c;
                    if (json[i]['group']) {
                        c = treeNodeFromJsonInstrumentation(json[i]['data'], json[i]['group']);
                    }
                    else {
                        c = treeNodeFromJsonInstrumentation(json[i], json[i]['name']);
                    }
                    if (c)
                        result.appendChild(c);
                }
                else if (typeof json[i] != 'function') {
                    result.appendChild(new Ext.tree.TreeNode({
                        text: i + " -> " + json[i]
                    }));
                }
            }
            else {
                result.appendChild(new Ext.tree.TreeNode({
                    text: i + " -> " + json[i]
                }));
            }
        }
    }
    else {
        result.appendChild(new Ext.tree.TreeNode({
            text: json
        }));
    }
    return result;
}

function treeNodeFromJsonMetrics(json, rootText) {
    var result = new Ext.tree.TreeNode({
        text: rootText
    });
    //  For Elements, process attributes and children
    if (typeof json === 'object') {
        for (var i in json) {
            if (json[i]) {
                if (typeof json[i] == 'object') {
                    var c;
                    if (json[i]) {
                        c = treeNodeFromJsonMetrics(json[i], i);
                        if (c) {
                            result.appendChild(c);
                        }
                    }
                }
                else if (typeof json[i] != 'function') {
                    result.appendChild(new Ext.tree.TreeNode({
                        text: i + " -> " + json[i]
                    }));
                }
            }
            else {
                result.appendChild(new Ext.tree.TreeNode({
                    text: i + " -> " + json[i]
                }));
            }
        }
    }
    else {
        result.appendChild(new Ext.tree.TreeNode({
            text: json
        }));
    }
    return result;
}

// Common stuff to get a paging toolbar for a data store
function getPagingBar(dataStore) {
    var pagingBar = new Ext.PagingToolbar({
        pageSize: 50,
        store: dataStore,
        displayInfo: true,
        displayMsg: '{0} - {1} of {2}',
        emptyMsg: "No data"
    });
    pagingBar.paramNames = {
        start: 'offset',
        limit: 'len'
    };
    return pagingBar;
}

//Image object display
Ext.ux.Image = Ext.extend(Ext.BoxComponent, {

    url: Ext.BLANK_IMAGE_URL,  //for initial src value

    autoEl: {
        tag: 'img',
        src: Ext.BLANK_IMAGE_URL
    },

    initComponent: function() {
         Ext.ux.Image.superclass.initComponent.call(this);
         this.addEvents('load');
   },

//  Add our custom processing to the onRender phase.
//  We add a ‘load’ listener to our element.
    onRender: function() {
        Ext.ux.Image.superclass.onRender.apply(this, arguments);
        this.el.on('load', this.onLoad, this);
        if(this.url){
            this.setSrc(this.url);
        }
    },

    onLoad: function() {
        this.fireEvent('load', this);
    },

    setSrc: function(src) {
        this.el.dom.src = src;
    },
    setAlt: function(altText) {
        this.autoEl.alt=altText;
    },

    onError: function(onErrorTxt) {
        this.autoEl.onError=onErrorTxt;
    }
});

function alertOnDAGError(){
    alert('Runtime error : Can\'t display the graph. Number of actions are more than dispaly limit 25');
}
// stuff to show details of a job
function jobDetailsPopup(response, request) {
    var jobDefinitionArea = new Ext.form.TextArea({
        fieldLabel: 'Definition',
        editable: false,
        name: 'definition',
        width: 1035,
        height: 400,
        autoScroll: true,
        emptyText: "Loading..."
    });
    var jobLogArea = new Ext.form.TextArea({
        fieldLabel: 'Logs',
        editable: false,
        name: 'logs',
        width: 1035,
        height: 400,
        autoScroll: true,
        emptyText: "To optimize log searching, you can provide search filter options as opt1=val1;opt2=val1;opt3=val1.\n" +
        "Available options are recent, start, end, loglevel, text, limit and debug.\n" +
        "For more detail refer documentation (/oozie/docs/DG_CommandLineTool.html#Filtering_the_server_logs_with_logfilter_options)"
    });

    var jobErrorLogArea = new Ext.form.TextArea({
        fieldLabel: 'ErrorLogs',
        editable: false,
        name: 'errorlogs',
        width: 1035,
        height: 400,
        autoScroll: true,
        emptyText: ""
    });

    function fetchDefinition(workflowId) {
        Ext.Ajax.request({
            url: getOozieBase() + 'job/' + workflowId + "?show=definition",
            timeout: 300000,
            success: function(response, request) {
                jobDefinitionArea.setRawValue(response.responseText);
            }

        });
    }

    function fetchErrorLogs(workflowId, errorLogStatus) {
        getLogs(getOozieBase() + 'job/' + workflowId + "?show=errorlog", null, errorLogStatus, jobErrorLogArea, false, null);
    }
    function fetchLogs(workflowId, logStatus) {
        getLogs(getOozieBase() + 'job/' + workflowId + "?show=log", searchFilterBox.getValue(), logStatus, jobLogArea, false, null);

    }
    var jobDetails = eval("(" + response.responseText + ")");
    var workflowId = jobDetails["id"];
    var appName = jobDetails["appName"];
    var jobActionStatus = new Ext.data.JsonStore({
        data: jobDetails["actions"],
        fields: ['id', 'name', 'type', 'startTime', 'retries', 'consoleUrl', 'endTime', 'externalId', 'status', 'trackerUri', 'workflowId', 'errorCode', 'errorMessage', 'conf', 'transition', 'externalStatus', 'externalChildIDs']
    });

    var formFieldSet = new Ext.form.FieldSet({
        autoHeight: true,
        defaultType: 'textfield',
        items: [ {
            fieldLabel: 'Job Id',
            editable: false,
            name: 'id',
            width: 400,
            value: jobDetails["id"]
        }, {
            fieldLabel: 'Name',
            editable: false,
            name: 'appName',
            width: 400,
            value: jobDetails["appName"]
        }, {
            fieldLabel: 'App Path',
            editable: false,
            name: 'appPath',
            width: 400,
            value: jobDetails["appPath"]
        }, {
            fieldLabel: 'Run',
            editable: false,
            name: 'run',
            width: 400,
            value: jobDetails["run"]
        }, {
            fieldLabel: 'Status',
            editable: false,
            name: 'status',
            width: 400,
            value: jobDetails["status"]
        }, {
            fieldLabel: 'User',
            editable: false,
            name: 'user',
            width: 400,
            value: jobDetails["user"]
        }, {
            fieldLabel: 'Group',
            editable: false,
            name: 'group',
            width: 400,
            value: jobDetails["group"]
        }, {
            fieldLabel: 'Parent Coord',
            editable: false,
            name: 'parentId',
            width: 400,
            value: jobDetails["parentId"]
        }, {
            fieldLabel: 'Create Time',
            editable: false,
            name: 'createdTime',
            width: 400,
            value: jobDetails["createdTime"]
        }, {
            fieldLabel: 'Start Time',
            editable: false,
            name: 'startTime',
            width: 400,
            value: jobDetails["startTime"]
        }, {
            fieldLabel: 'Last Modified',
            editable: false,
            name: 'lastModTime',
            width: 400,
            value: jobDetails["lastModTime"]
        },{
            fieldLabel: 'End Time',
            editable: false,
            name: 'endTime',
            width: 400,
            value: jobDetails["endTime"]
        } ]
    });
    var fs = new Ext.FormPanel({
        frame: true,
        labelAlign: 'right',
        labelWidth: 85,
        items: [formFieldSet],
        tbar: [ {
            text: "&nbsp;&nbsp;&nbsp;",
            icon: 'ext-2.2/resources/images/default/grid/refresh.gif',
            handler: function() {
                Ext.Ajax.request({
                    url: getOozieBase() + 'job/' + workflowId + "?timezone=" + getTimeZone(),
                    timeout: 300000,
                    success: function(response, request) {
                        jobDetails = eval("(" + response.responseText + ")");
                        jobActionStatus.loadData(jobDetails["actions"]);
                        fs.getForm().setValues(jobDetails);
                    }

                });
            }
        }]

    });
    var jobs_grid = new Ext.grid.GridPanel({
        store: jobActionStatus,
        loadMask: true,
        columns: [new Ext.grid.RowNumberer(), {
            id: 'id',
            header: "Action Id",
            width: 300,
            sortable: true,
            dataIndex: 'id'
        }, {
            header: "Name",
            width: 80,
            sortable: true,
            dataIndex: 'name'
        }, {
            header: "Type",
            width: 80,
            sortable: true,
            dataIndex: 'type'
        }, {
            header: "Status",
            width: 120,
            sortable: true,
            dataIndex: 'status'
        }, {
            header: "Transition",
            width: 80,
            sortable: true,
            dataIndex: 'transition'
        }, {
            header: "StartTime",
            width: 170,
            sortable: true,
            dataIndex: 'startTime'
        }, {
            header: "EndTime",
            width: 170,
            sortable: true,
            dataIndex: 'endTime'
        } ],
        stripeRows: true,
        // autoHeight: true,
        autoScroll: true,
        frame: true,
        height: 400,
        width: 1600,
        title: 'Actions',
        listeners: {
            cellclick: {
                fn: showActionContextMenu
            }
        }

    });
    function showActionContextMenu(thisGrid, rowIndex, cellIndex, e) {
        var actionStatus = thisGrid.store.data.items[rowIndex].data;
        actionDetailsGridWindow(actionStatus);
        function actionDetailsGridWindow(actionStatus) {
            var formFieldSet = new Ext.form.FieldSet({
                title: actionStatus.actionName,
                autoHeight: true,
                width: 520,
                defaultType: 'textfield',
                items: [ {
                    fieldLabel: 'Name',
                    editable: false,
                    name: 'name',
                    width: 400,
                    value: actionStatus["name"]
                }, {
                    fieldLabel: 'Type',
                    editable: false,
                    name: 'type',
                    width: 400,
                    value: actionStatus["type"]
                }, {
                    fieldLabel: 'Transition',
                    editable: false,
                    name: 'transition',
                    width: 400,
                    value: actionStatus["transition"]
                }, {
                    fieldLabel: 'Start Time',
                    editable: false,
                    name: 'startTime',
                    width: 400,
                    value: actionStatus["startTime"]
                }, {
                    fieldLabel: 'End Time',
                    editable: false,
                    name: 'endTime',
                    width: 400,
                    value: actionStatus["endTime"]
                }, {
                    fieldLabel: 'Status',
                    editable: false,
                    name: 'status',
                    width: 400,
                    value: actionStatus["status"]
                }, {
                    fieldLabel: 'Error Code',
                    editable: false,
                    name: 'errorCode',
                    width: 400,
                    value: actionStatus["errorCode"]
                }, {
                    fieldLabel: 'Error Message',
                    editable: false,
                    name: 'errorMessage',
                    width: 400,
                    value: actionStatus["errorMessage"]
                }, {
                    fieldLabel: 'External ID',
                    editable: false,
                    name: 'externalId',
                    width: 400,
                    value: actionStatus["externalId"]
                }, {
                    fieldLabel: 'External Status',
                    editable: false,
                    name: 'externalStatus',
                    width: 400,
                    value: actionStatus["externalStatus"]
                }, new Ext.form.TriggerField({
                    fieldLabel: 'Console URL',
                    editable: false,
                    name: 'consoleUrl',
                    width: 400,
                    value: actionStatus["consoleUrl"],
                    triggerClass: 'x-form-search-trigger',
                    onTriggerClick: function() {
                        window.open(actionStatus["consoleUrl"]);
                    }

                }), {
                    fieldLabel: 'Tracker URI',
                    editable: false,
                    name: 'trackerUri',
                    width: 400,
                    value: actionStatus["trackerUri"]

                }
            ]});
            var detail = new Ext.FormPanel({
                frame: true,
                labelAlign: 'right',
                labelWidth: 85,
                //width: 540,
                items: [formFieldSet]
            });
            var urlUnit = new Ext.FormPanel();
            populateUrlUnit(actionStatus, urlUnit);
            var win = new Ext.Window({
                title: 'Action (Name: ' + actionStatus["name"] + '/JobId: ' + workflowId + ')',
                closable: true,
                width: 560,
                autoHeight: true,
                plain: true,
                items: [new Ext.TabPanel({
                    activeTab: 0,
                    autoHeight: true,
                    deferredRender: false,
                    items: [ {
                        title: 'Action Info',
                        items: detail
                    }, {
                        title: 'Action Configuration',
                        items: new Ext.form.TextArea({
                            fieldLabel: 'Configuration',
                            editable: false,
                            name: 'config',
                            height: 350,
                            width: 540,
                            autoScroll: true,
                            value: actionStatus["conf"]
                        })
                    }],
                    tbar: [{
                        text: "&nbsp;&nbsp;&nbsp;",
                        icon: 'ext-2.2/resources/images/default/grid/refresh.gif',
                        handler: function() {
                            refreshActionDetails(workflowId+"@"+actionStatus["name"], detail, urlUnit);
                        }
                    }]
                })]
            });

            // Tab to show list of child Job URLs for pig action
            var childJobsItem = {
                title : 'Child Job URLs',
                autoScroll : true,
                frame : true,
                labelAlign : 'right',
                labelWidth : 70,
                height: 350,
                width: 540,
                items : urlUnit
            };
            if (actionStatus.type == "pig" || actionStatus.type == "hive" || actionStatus.type == "map-reduce") {
                var tabPanel = win.items.get(0);
                tabPanel.add(childJobsItem);
            }
            win.setPosition(50, 50);
            win.show();
        }
    }

    function populateUrlUnit(actionStatus, urlUnit) {
        var consoleUrl = actionStatus["consoleUrl"];
        var externalChildIDs = actionStatus["externalChildIDs"];
        if (consoleUrl && externalChildIDs && externalChildIDs != "null") {
            var urlPrefix = consoleUrl.trim().split(/_/)[0];
            // externalChildIds is a comma-separated string of each child job
            // ID.
            // Create URL list by appending jobID portion after stripping "job"
            var jobIds = externalChildIDs.split(/,/);
            var count = 1;
            jobIds.forEach(function(jobId) {
                jobId = jobId.trim().split(/job/);
                if (jobId.length > 1) {
                    jobId = jobId[1];
                    var jobUrl = new Ext.form.TriggerField({
                        fieldLabel : 'Child Job ' + count,
                        editable : false,
                        name : 'childJobURLs',
                        width : 400,
                        value : urlPrefix + jobId,
                        triggerClass : 'x-form-search-trigger',
                        onTriggerClick : function() {
                            window.open(urlPrefix + jobId);
                        }
                    });
                    if (jobId != undefined) {
                        urlUnit.add(jobUrl);
                        count++;
                    }
                }
            });
            if (count == 1) {
                var note = new Ext.form.TextField({
                    fieldLabel : 'Child Job',
                    value : 'n/a'
                });
                urlUnit.add(note);
            }
        } else {
            var note = new Ext.form.TextField({
                fieldLabel : 'Child Job',
                value : 'n/a'
            });
            urlUnit.add(note);
        }
    }

    function refreshActionDetails(actionId, detail, urlUnit) {
        Ext.Ajax.request({
            url: getOozieBase() + 'job/' + actionId + "?timezone=" + getTimeZone(),
            timeout: 300000,
            success: function(response, request) {
                var results = eval("(" + response.responseText + ")");
                detail.getForm().setValues(results);
                urlUnit.getForm().setValues(results);
                populateUrlUnit(results, urlUnit);
            }
        });
    }

    var imageContainer = new Ext.Container({
        autoEl: {},
        height: '1000px',
        weidht: '1000px',
        autoScroll: true,
        style  : { overflow: 'auto', overflowX: 'hidden' }
    });

    var searchFilter = new Ext.form.Label({
        text : 'Enter Search Filter'
    });

    var searchFilterBox = new Ext.form.TextField({
                 fieldLabel: 'searchFilterBox',
                 name: 'searchFilterBox',
                 width: 350,
                 value: ''
    });

    var logStatus = new Ext.form.Label({
        text : 'Log Status : '
    });

    var errorLogStatus = new Ext.form.Label({
        text : 'Log Status : '
    });

    var getLogButton = new Ext.Button({
        text: 'Get Logs',
        ctCls: 'x-btn-over',
        handler: function() {
            fetchLogs(workflowId, logStatus);
            }
    });

    var isLoadedDAG = false;
    var isErrorLogLoaded = false;

    var jobDetailsTab = new Ext.TabPanel({
        activeTab: 0,
        autoHeight: true,
        deferredRender: false,
        items: [ {
            title: 'Job Info',
            items: fs

        }, {
            title: 'Job Definition',
            items: jobDefinitionArea
        }, {
            title: 'Job Configuration',
            items: new Ext.form.TextArea({
                fieldLabel: 'Configuration',
                editable: false,
                name: 'config',
                width: 1035,
                height: 430,
                autoScroll: true,
                value: jobDetails["conf"]
            })
        }, {
            title: 'Job Log',
            items: jobLogArea,
            tbar: [ searchFilter, searchFilterBox, getLogButton, {xtype: 'tbfill'}, logStatus]
        },
        {
            title: 'Job Error Log',
            items: jobErrorLogArea,
            tbar: [ {
                text: "&nbsp;&nbsp;&nbsp;",
                icon: 'ext-2.2/resources/images/default/grid/refresh.gif',
                handler: function() {
                    fetchErrorLogs(workflowId, errorLogStatus);
                }
            }, {xtype: 'tbfill'}, errorLogStatus]
        },
        {
            title: 'Job DAG',
            items: imageContainer,
            tbar: [{
                text: "&nbsp;&nbsp;&nbsp;"
                // To avoid OOM
                /*icon: 'ext-2.2/resources/images/default/grid/refresh.gif',
                handler: function() {
                    fetchDAG(workflowId);
                }*/
            }]
        }]
    });
    jobDetailsTab.addListener("tabchange", function(panel, selectedTab) {
        if (selectedTab.title == "Job Info") {
            jobs_grid.setVisible(true);
            return;
        }
        else if (selectedTab.title == 'Job Definition') {
            fetchDefinition(workflowId);
        }
        else if (selectedTab.title == 'Job Error Log') {
            if(!isErrorLogLoaded){
                fetchErrorLogs(workflowId, errorLogStatus);
                isErrorLogLoaded=true;
            }
        } else if(selectedTab.title == 'Job DAG') {
                if(!isLoadedDAG){
                var dagImage=   new Ext.ux.Image({
                        id: 'dagImage',
                        url: getOozieBase() + 'job/' + workflowId + '?show=graph',
                        autoScroll: true
                        });
                    dagImage.setAlt('Runtime error : Can\'t display the graph. Number of actions are more than display limit 25');
                    dagImage.onError('alertOnDAGError()');
                    imageContainer.add(dagImage);
                    imageContainer.syncSize();
                    imageContainer.doLayout(true);
                    isLoadedDAG=true;
                 }
                }
        jobs_grid.setVisible(false);
    });
    var win = new Ext.Window({
        title: 'Job (Name: ' + appName + '/JobId: ' + workflowId + ')',
        closable: true,
        width: 1050,
        autoHeight: true,
        plain: true,
        items: [jobDetailsTab, jobs_grid]
    });
    win.setPosition(10, 10);
    win.show();
}

function coordJobDetailsPopup(response, request) {
    var isErrorLogLoaded= false;

    var jobDefinitionArea = new Ext.form.TextArea({
        fieldLabel: 'Definition',
        editable: false,
        name: 'definition',
        width: 1035,
        height: 400,
        autoScroll: true,
        emptyText: "Loading..."
    });
    var jobLogArea = new Ext.form.TextArea({
        fieldLabel: 'Logs',
        editable: false,
        id: 'jobLogAreaId',
        name: 'logs',
        width: 1035,
        height: 400,
        autoScroll: true,
        emptyText: "Enter the list of actions in the format similar to 1,3-4,7-40 to get logs for specific coordinator actions. " +
                   "To get the log for the coordinator job, leave the actions field empty.\n\n" +
        "To optimize log searching, you can provide search filter options as opt1=val1;opt2=val1;opt3=val1.\n" +
        "Available options are recent, start, end, loglevel, text, limit and debug.\n" +
        "For more detail refer documentation (/oozie/docs/DG_CommandLineTool.html#Filtering_the_server_logs_with_logfilter_options)"
    });
    var jobErrorLogArea = new Ext.form.TextArea({
        fieldLabel: 'ErrorLogs',
        editable: false,
        id: 'jobErrorLogAreaId',
        name: 'errorlogs',
        width: 1035,
        height: 400,
        autoScroll: true,
        emptyText: ""
    });
    var getLogButton = new Ext.Button({
        text: 'Get Logs',
        ctCls: 'x-btn-over',
        handler: function() {
            fetchLogs(coordJobId, actionsTextBox.getValue());
        }
    });
    var actionsTextBox = new Ext.form.TextField({
             fieldLabel: 'ActionsList',
             name: 'ActionsList',
             width: 150,
             value: ''
         });

    var actionsText = new Ext.form.Label({
        text : 'Enter action list : '
    });

    var searchFilter = new Ext.form.Label({
        text : 'Enter Search Filter'
    });

    var searchFilterBox = new Ext.form.TextField({
                 fieldLabel: 'searchFilterBox',
                 name: 'searchFilterBox',
                 width: 350,
                 value: ''
    });

    var logStatus = new Ext.form.Label({
        text : 'Log Status : '
    });
    var errorLogStatus = new Ext.form.Label({
        text : 'Log Status : '
    });
    function fetchDefinition(coordJobId) {
        Ext.Ajax.request({
            url: getOozieBase() + 'job/' + coordJobId + "?show=definition",
            timeout: 300000,
            success: function(response, request) {
                jobDefinitionArea.setRawValue(response.responseText);
            }
        });
    }
    function fetchLogs(coordJobId, actionsList) {
        if (actionsList == '') {
            getLogs(getOozieBase() + 'job/' + coordJobId + "?show=log",
                    searchFilterBox.getValue(), logStatus, jobLogArea, true, null);
        } else {
            getLogs(getOozieBase() + 'job/' + coordJobId
                    + "?show=log&type=action&scope=" + actionsList, searchFilterBox.getValue(), logStatus, jobLogArea,
                    true,
                    'Action List format is wrong. Format should be similar to 1,3-4,7-40');
        }
    }
    function fetchErrorLogs(coordJobId) {
            getLogs(getOozieBase() + 'job/' + coordJobId + "?show=errorlog",
                    null, errorLogStatus, jobErrorLogArea, true, null);
    }


    var jobDetails = eval("(" + response.responseText + ")");
    var coordJobId = jobDetails["coordJobId"];
    var appName = jobDetails["coordJobName"];
    var jobActionStatus = new Ext.data.JsonStore({
        autoLoad: {params:{offset: 0, len: 50}},
        totalProperty: 'total',
        root: 'actions',
        fields: ['id', 'name', 'type', 'createdConf', 'runConf', 'actionNumber', 'createdTime', 'externalId',
                 'lastModifiedTime', 'nominalTime', 'status', 'missingDependencies', 'externalStatus', 'trackerUri',
                 'consoleUrl', 'errorCode', 'errorMessage', 'actions', 'externalChildIDs'],
        proxy: new Ext.data.HttpProxy({
           url: getOozieBase() + 'job/' + coordJobId + "?timezone=" + getTimeZone() + "&order=desc",
           method: 'GET'
        })
    });

    var formFieldSet = new Ext.form.FieldSet({
        autoHeight: true,
        defaultType: 'textfield',
        items: [ {
            fieldLabel: 'Job Id',
            editable: false,
            name: 'coordJobId',
            width: 400,
            value: jobDetails["coordJobId"]
        }, {
            fieldLabel: 'Name',
            editable: false,
            name: 'coordJobName',
            width: 400,
            value: jobDetails["coordJobName"]
        }, {
            fieldLabel: 'Status',
            editable: false,
            name: 'status',
            width: 400,
            value: jobDetails["status"]
        }, {
            fieldLabel: 'User',
            editable: false,
            name: 'user',
            width: 400,
            value: jobDetails["user"]
        }, {
            fieldLabel: 'Group',
            editable: false,
            name: 'group',
            width: 400,
            value: jobDetails["group"]
        }, {
            fieldLabel: 'Frequency',
            editable: false,
            name: 'frequency',
            width: 400,
            value: jobDetails["frequency"]
        }, {
            fieldLabel: 'Unit',
            editable: false,
            name: 'timeUnit',
            width: 400,
            value: jobDetails["timeUnit"]
        }, {
            fieldLabel: 'Parent Bundle',
            editable: false,
            name: 'bundleId',
            width: 400,
            value: jobDetails["bundleId"]
        }, {
            fieldLabel: 'Start Time',
            editable: false,
            name: 'startTime',
            width: 200,
            value: jobDetails["startTime"]
        }, {
            fieldLabel: 'Next Matd',
            editable: false,
            name: 'nextMaterializedTime',
            width: 200,
            value: jobDetails["nextMaterializedTime"]
        }, {
            fieldLabel: 'End Time',
            editable: false,
            name: 'endTime',
            width: 200,
            value: jobDetails["endTime"]
        }, {
            fieldLabel: 'Pause Time',
            editable: false,
            name: 'pauseTime',
            width: 200,
            value: jobDetails["pauseTime"]
        }, {
            fieldLabel: 'Concurrency',
            editable: false,
            name: 'concurrency',
            width: 200,
            value: jobDetails["concurrency"]
        } ]
    });
    var fs = new Ext.FormPanel({
        frame: true,
        labelAlign: 'right',
        labelWidth: 85,
        items: [formFieldSet],
        tbar: [ {
            text: "&nbsp;&nbsp;&nbsp;",
            icon: 'ext-2.2/resources/images/default/grid/refresh.gif',
            handler: function() {
                Ext.Ajax.request({
                    url: getOozieBase() + 'job/' + coordJobId + "?timezone=" + getTimeZone() + "&offset=0&len=0",
                    timeout: 300000,
                    success: function(response, request) {
                        jobDetails = eval("(" + response.responseText + ")");
                        fs.getForm().setValues(jobDetails);
                        jobActionStatus.reload();
                    }

                });
            }
        }]

    });
    var coord_jobs_grid = new Ext.grid.GridPanel({
        store: jobActionStatus,
        loadMask: true,
        columns: [new Ext.grid.RowNumberer(), {
            id: 'id',
            header: "Action Id",
            width: 260,
            sortable: true,
            dataIndex: 'id'
        }, {
            header: "Status",
            width: 80,
            sortable: true,
            dataIndex: 'status'
        }, {
            header: "Ext Id",
            width: 220,
            sortable: true,
            dataIndex: 'externalId'
        }, {
            header: "Error Code",
            width: 80,
            sortable: true,
            dataIndex: 'errorCode'
        }, {
            header: "Created Time",
            width: 170,
            sortable: true,
            dataIndex: 'createdTime'
        }, {
            header: "Nominal Time",
            width: 170,
            sortable: true,
            dataIndex: 'nominalTime'
        }, {
            header: "Last Mod Time",
            width: 170,
            sortable: true,
            dataIndex: 'lastModifiedTime'
        } ],
        stripeRows: true,
        // autoHeight: true,
        autoScroll: true,
        frame: true,
        height: 400,
        width: 1600,
        title: 'Actions',
        bbar: getPagingBar(jobActionStatus),
        listeners: {
            cellclick: {
                fn: showWorkflowPopup
            }
        }

    });
    function showWorkflowPopup(thisGrid, rowIndex, cellIndex, e) {
        var actionStatus = thisGrid.store.data.items[rowIndex].data;
        var workflowId = actionStatus["externalId"];
        jobDetailsGridWindow(workflowId);
    }
    // alert("Coordinator PopUP 4 inside coordDetailsPopup ");
    function showCoordActionContextMenu(thisGrid, rowIndex, cellIndex, e) {
        var actionStatus = thisGrid.store.data.items[rowIndex].data;
        actionDetailsGridWindow(actionStatus);
        function actionDetailsGridWindow(actionStatus) {
            var formFieldSet = new Ext.form.FieldSet({
                title: actionStatus.actionName,
                autoHeight: true,
                width: 520,
                defaultType: 'textfield',
                items: [ {
                    fieldLabel: 'Name',
                    editable: false,
                    name: 'name',
                    width: 400,
                    value: actionStatus["name"]
                }, {
                    fieldLabel: 'Type',
                    editable: false,
                    name: 'type',
                    width: 400,
                    value: actionStatus["type"]
                }, {
                    fieldLabel: 'externalId',
                    editable: false,
                    name: 'externalId',
                    width: 400,
                    value: actionStatus["externalId"]
                }, {
                    fieldLabel: 'Start Time',
                    editable: false,
                    name: 'startTime',
                    width: 400,
                    value: actionStatus["startTime"]
                }, {
                    fieldLabel: 'Nominal Time',
                    editable: false,
                    name: 'nominalTime',
                    width: 400,
                    value: actionStatus["nominalTime"]
                }, {
                    fieldLabel: 'Status',
                    editable: false,
                    name: 'status',
                    width: 400,
                    value: actionStatus["status"]
                }, {
                    fieldLabel: 'Error Code',
                    editable: false,
                    name: 'errorCode',
                    width: 400,
                    value: actionStatus["errorCode"]
                }, {
                    fieldLabel: 'Error Message',
                    editable: false,
                    name: 'errorMessage',
                    width: 400,
                    value: actionStatus["errorMessage"]
                }, {
                    fieldLabel: 'External Status',
                    editable: false,
                    name: 'externalStatus',
                    width: 400,
                    value: actionStatus["externalStatus"]
                }, new Ext.form.TriggerField({
                    fieldLabel: 'Console URL',
                    editable: false,
                    name: 'consoleUrl',
                    width: 400,
                    value: actionStatus["consoleUrl"],
                    triggerClass: 'x-form-search-trigger',
                    onTriggerClick: function() {
                        window.open(actionStatus["consoleUrl"]);
                    }

                }), {
                    fieldLabel: 'Tracker URI',
                    editable: false,
                    name: 'trackerUri',
                    width: 400,
                    value: actionStatus["trackerUri"]

                } ]
            });
            /*
             var detail = new Ext.FormPanel( {
             frame: true,
             labelAlign: 'right',
             labelWidth: 85,
             width: 540,
             items: [formFieldSet]
             });
             var win = new Ext.Window( {
             title: 'Action (Name: ' + actionStatus["name"] + '/JobId: ' + coordJobId + ')',
             closable: true,
             width: 560,
             autoHeight: true,
             plain: true,
             items: [new Ext.TabPanel( {
             activeTab: 0,
             autoHeight: true,
             deferredRender: false,
             items: [ {
             title: 'Action Info',
             items: detail
             }, {
             title: 'Action Configuration',
             items: new Ext.form.TextArea( {
             fieldLabel: 'Configuration',
             editable: false,
             name: 'config',
             height: 350,
             width: 540,
             autoScroll: true,
             value: actionStatus["conf"]
             })
             }, ]
             })]
             });
             win.setPosition(50, 50);
             win.show();
             */
        }
    }

    var rerunActionText = new Ext.form.Label({
        text : 'Enter Coordinator Action number : ',
        ctCls: 'spaces'
    });
    var rerunActionTextBox = new Ext.form.TextField({
        fieldLabel: 'RerunAction',
        name: 'RerunAction',
        width: 150,
        value: ''
    });
    var store = new Ext.data.JsonStore({
        baseParams: {
            scope: 0,
        },
        root: 'workflows',
        fields: ['id', 'status', 'startTime', 'endTime'],
        proxy: new Ext.data.HttpProxy({
            url: getOozieBase() + 'job/' + coordJobId + '?show=allruns&type=action',
            timeout: 300000
        })
    });
    store.proxy.conn.method = "GET";

    var rerunsUnit = new Ext.grid.GridPanel({
        autoScroll: true,
        height: 200,
        autoRender: true,
        store: store,
        columns: [new Ext.grid.RowNumberer(), {
            header: 'Workflow Id',
            dataIndex: 'id',
            id: 'id',
            width: 240
        },{
            header: 'Workflow Status',
            dataIndex: 'status',
            id: 'status',
            width: 200
        },{
            header: 'Started',
            dataIndex: 'startTime',
            id: 'startTime',
            width: 240
        },{
            header: 'Ended',
            dataIndex: 'endTime',
            id: 'endTime',
            width: 240
        }],
        listeners: {
            cellclick: function (rerunsUnit, rowIndex, colIndex) {
                var obj = store.getAt(rowIndex);
                jobDetailsGridWindow(obj.data.id);
            },
        },
        frame: false
    });
    function populateReruns(coordActionId) {
        var actionNum = rerunActionTextBox.getValue();
        store.baseParams.scope = actionNum;
        store.reload();
    }
    var getRerunsButton = new Ext.Button({
        text: 'Get Workflows',
        ctCls: 'x-btn-over',
        ctCls: 'spaces',
        handler: function() {
            populateReruns(rerunsUnit);
        }
    });

    var jobDetailsTab = new Ext.TabPanel({
        activeTab: 0,
        autoHeight: true,
        deferredRender: false,
        items: [ {
            title: 'Coord Job Info',
            items: fs
        },{
       title: 'Coord Job Definition',
            items: jobDefinitionArea
    },{
            title: 'Coord Job Configuration',
            items: new Ext.form.TextArea({
            fieldLabel: 'Configuration',
            editable: false,
            name: 'config',
                width: 1035,
                height: 430,
                autoScroll: true,
                value: jobDetails["conf"]
            })
	},{
           title: 'Coord Job Log',
           items: jobLogArea,
           tbar: [
                  actionsText,actionsTextBox, searchFilter, searchFilterBox, getLogButton, {xtype: 'tbfill'}, logStatus]
       },
       {
           title: 'Coord Error Log',
           items: jobErrorLogArea,
           tbar: [ {
               text: "&nbsp;&nbsp;&nbsp;",
               icon: 'ext-2.2/resources/images/default/grid/refresh.gif',
               handler: function() {
                   fetchErrorLogs(coordJobId);
               }
           },{xtype: 'tbfill'}, errorLogStatus]
       },
       {
           title: 'Coord Action Reruns',
           items: rerunsUnit,
           tbar: [
               rerunActionText, rerunActionTextBox, getRerunsButton]
       },
    ]});

    jobDetailsTab.addListener("tabchange", function(panel, selectedTab) {
        if (selectedTab.title == "Coord Job Info") {
            coord_jobs_grid.setVisible(true);
            return;
        }
        else if (selectedTab.title == 'Coord Job Definition') {
            fetchDefinition(coordJobId);
        }
        else if (selectedTab.title == 'Coord Action Reruns') {
            rerunsUnit.setVisible(true);
        }
        else if (selectedTab.title == 'Coord Error Log') {
            if(!isErrorLogLoaded){
                fetchErrorLogs(coordJobId);
                isErrorLogLoaded=true;
            }
        }
        coord_jobs_grid.setVisible(false);
    });
    var win = new Ext.Window({
        title: 'Job (Name: ' + appName + '/coordJobId: ' + coordJobId + ')',
        closable: true,
        width: 1050,
        autoHeight: true,
        plain: true,
        items: [jobDetailsTab, coord_jobs_grid]
    });
    win.setPosition(10, 10);
    win.show();
}

function bundleJobDetailsPopup(response, request) {

    var isErrorLogLoaded = false;
    var jobDefinitionArea = new Ext.form.TextArea({
        fieldLabel: 'Definition',
        editable: false,
        name: 'definition',
        width: 1005,
        height: 400,
        autoScroll: true,
        emptyText: "Loading..."

    });
    var jobDetails = eval("(" + response.responseText + ")");
    var bundleJobId = jobDetails["bundleJobId"];
    var bundleJobName = jobDetails["bundleJobName"];
    var jobActionStatus = new Ext.data.JsonStore({
        data: jobDetails["bundleCoordJobs"],
        fields: ['coordJobId', 'coordJobName', 'coordJobPath', 'frequency', 'timeUnit', 'nextMaterializedTime', 'status', 'startTime', 'endTime', 'pauseTime','user','group']
    });

    var formFieldSet = new Ext.form.FieldSet({
        autoHeight: true,
        defaultType: 'textfield',
        items: [ {
            fieldLabel: 'Job Id',
            editable: false,
            name: 'bundleJobId',
            width: 400,
            value: jobDetails["bundleJobId"]
        }, {
            fieldLabel: 'Name',
            editable: false,
            name: 'bundleJobName',
            width: 400,
            value: jobDetails["bundleJobName"]
        }, {
            fieldLabel: 'Status',
            editable: false,
            name: 'status',
            width: 400,
            value: jobDetails["status"]
        }, {
            fieldLabel: 'Kickoff Time',
            editable: false,
            name: 'kickoffTime',
            width: 400,
            value: jobDetails["kickoffTime"]
        }, {
            fieldLabel: 'Created Time',
            editable: false,
            name: 'createdTime',
            width: 400,
            value: jobDetails["createdTime"]
        }, {
            fieldLabel: 'User',
            editable: false,
            name: 'user',
            width: 170,
            value: jobDetails["user"]
        }, {
            fieldLabel: 'Group',
            editable: false,
            name: 'group',
            width: 170,
            value: jobDetails["group"]
        } ]
    });

    var fs = new Ext.FormPanel({
        frame: true,
        labelAlign: 'right',
        labelWidth: 85,
        width: 1010,
        items: [formFieldSet],
        tbar: [ {
            text: "&nbsp;&nbsp;&nbsp;",
            icon: 'ext-2.2/resources/images/default/grid/refresh.gif',
            handler: function() {
                Ext.Ajax.request({
                    url: getOozieBase() + 'job/' + bundleJobId + "?timezone=" + getTimeZone(),
                    timeout: 300000,
                    success: function(response, request) {
                        jobDetails = eval("(" + response.responseText + ")");
                        jobActionStatus.loadData(jobDetails["bundleCoordJobs"]);
                        fs.getForm().setValues(jobDetails);
                    }

                });
            }
        }]

    });

    var coord_jobs_grid = new Ext.grid.GridPanel({
        store: jobActionStatus,
        loadMask: true,
        columns: [new Ext.grid.RowNumberer(), {
            id: 'coordJobId',
            header: "Coord Job Id",
            width: 240,
            sortable: true,
            dataIndex: 'coordJobId'
        }, {
            header: "Coord Job Name",
            width: 100,
            sortable: true,
            dataIndex: 'coordJobName'
        }, {
            header: "Status",
            width: 80,
            sortable: true,
            dataIndex: 'status'
        }, {
            header: "User",
            width: 80,
            sortable: true,
            dataIndex: 'user'
        }, {
            header: "Group",
            width: 80,
            sortable: true,
            dataIndex: 'group'
        }, {
            header: "Frequency",
            width: 80,
            sortable: true,
            dataIndex: 'frequency'
        }, {
            header: "Time Unit",
            width: 80,
            sortable: true,
            dataIndex: 'timeUnit'
        }, {
            header: "Start Time",
            width: 200,
            sortable: true,
            dataIndex: 'startTime'
        }, {
            header: "End Time",
            width: 200,
            sortable: true,
            dataIndex: 'endTime'
        }, {
            header: "Next Materialized",
            width: 200,
            sortable: true,
            dataIndex: 'nextMaterializedTime'
        } ],
        stripeRows: true,
        // autoHeight: true,
        autoScroll: true,
        frame: true,
        height: 400,
        width: 1000,
        title: 'Coord Jobs',
        bbar: getPagingBar(jobActionStatus),
        listeners: {
            cellclick: {
                fn: showCoordJobContextMenu
            }
        }
    });

    var jobLogArea = new Ext.form.TextArea({
        fieldLabel: 'Logs',
        editable: false,
        name: 'logs',
        width: 1010,
        height: 400,
        autoScroll: true,
        emptyText: "To optimize log searching, you can provide search filter options as opt1=val1;opt2=val1;opt3=val1.\n" +
        "Available options are recent, start, end, loglevel, text, limit and debug.\n" +
        "For more detail refer documentation (/oozie/docs/DG_CommandLineTool.html#Filtering_the_server_logs_with_logfilter_options)"
    });

    var jobErrorLogArea = new Ext.form.TextArea({
        fieldLabel: 'ErrorLogs',
        editable: false,
        name: 'errorlogs',
        width: 1010,
        height: 400,
        autoScroll: true,
        emptyText: ""
    });

    var searchFilter = new Ext.form.Label({
        text : 'Enter Search Filter'
    });

    var searchFilterBox = new Ext.form.TextField({
                 fieldLabel: 'searchFilterBox',
                 name: 'searchFilterBox',
                 width: 350,
                 value: ''
    });

    var logStatus = new Ext.form.Label({
        text : 'Log Status : '
    });

    var errorLogStatus = new Ext.form.Label({
        text : 'Log Status : '
    });

    var getLogButton = new Ext.Button({
        text: 'Get Logs',
        ctCls: 'x-btn-over',
        handler: function() {
            getLogs(getOozieBase() + 'job/' + bundleJobId + "?show=log", searchFilterBox.getValue(), logStatus, jobLogArea, false, null);
        }
    });


    var jobDetailsTab = new Ext.TabPanel({
        activeTab: 0,
        autoHeight: true,
        deferredRender: false,
        items: [ {
            title: 'Bundle Job Info',
            items: fs
        },{
           title: 'Bundle Job Definition',
             items: jobDefinitionArea
        },{
            title: 'Bundle Job Configuration',
             items: new Ext.form.TextArea({
              fieldLabel: 'Configuration',
              editable: false,
              name: 'config',
                width: 1010,
                height: 430,
                autoScroll: true,
                value: jobDetails["conf"]
             })
      },{
           title: 'Bundle Job Log',
           items: jobLogArea,
             tbar: [searchFilter, searchFilterBox, getLogButton, {xtype: 'tbfill'}, logStatus]
      },
     {
         title: 'Bundle Error Log',
         items: jobErrorLogArea,
         tbar: [ {
             text: "&nbsp;&nbsp;&nbsp;",
             icon: 'ext-2.2/resources/images/default/grid/refresh.gif',
             handler: function() {
                 getLogs(getOozieBase() + 'job/' + bundleJobId + "?show=errorlog", null, errorLogStatus, jobErrorLogArea,
                         false, null);
             }
         }, {xtype: 'tbfill'}, errorLogStatus]
     }
      ]});

    jobDetailsTab.addListener("tabchange", function(panel, selectedTab) {
        if (selectedTab.title == "Bundle Job Info") {
            coord_jobs_grid.setVisible(true);
            return;
        }
        else if (selectedTab.title == 'Bundle Job Definition') {
            fetchDefinition(bundleJobId);
        }
        else if (selectedTab.title == 'Bundle Error Log') {
            if(!isErrorLogLoaded){
                getLogs(getOozieBase() + 'job/' + bundleJobId + "?show=errorlog", null, errorLogStatus, jobErrorLogArea,
                        false, null);
                isErrorLogLoaded=true;
            }
        }
        coord_jobs_grid.setVisible(false);
    });

    function fetchDefinition(bundleJobId) {
        Ext.Ajax.request({
            url: getOozieBase() + 'job/' + bundleJobId + "?show=definition",
            timeout: 300000,
            success: function(response, request) {
                jobDefinitionArea.setRawValue(response.responseText);
            }
        });
    }

    var win = new Ext.Window({
        title: 'Job (Name: ' + bundleJobName + '/bundleJobId: ' + bundleJobId + ')',
        closable: true,
        width: 1020,
        autoHeight: true,
        plain: true,
        items: [jobDetailsTab, coord_jobs_grid]
    });
    win.setPosition(10, 10);
    win.show();
}

function jobDetailsGridWindow(workflowId) {
    Ext.Ajax.request({
        url: getOozieBase() + 'job/' + workflowId + "?timezone=" + getTimeZone(),
        timeout: 300000,
        success: jobDetailsPopup

    });
}

function coordJobDetailsGridWindow(coordJobId) {
    Ext.Ajax.request({
        /*
         Ext.Msg.show({
         title:'Coord JobDetails Window Popup'
         msg: 'coordJobDetailsGridWindow invoked',
         buttons: Ext.Msg.OK,
         icon: Ext.MessageBox.INFO
         });
         */
        url: getOozieBase() + 'job/' + coordJobId + "?timezone=" + getTimeZone() + "&offset=0&len=0",
        timeout: 300000,
        success: coordJobDetailsPopup
    });
}

function coordActionDetailsGridWindow(coordActionId) {
    Ext.Ajax.request({
        url: getOozieBase() + 'job/' + coordActionId + "?timezone=" + getTimeZone(),
        timeout: 300000,
        success: function(response, request) {
            var coordAction = JSON.parse(response.responseText);
            var workflowId = coordAction.externalId;
            jobDetailsGridWindow(workflowId);
        }
    });
}

function bundleJobDetailsGridWindow(bundleJobId) {
    Ext.Ajax.request({
        url: getOozieBase() + 'job/' + bundleJobId + "?timezone=" + getTimeZone(),
        timeout: 300000,
        success: bundleJobDetailsPopup
    });
}

function showConfigurationInWindow(dataObject, windowTitle) {
    var configGridData = new Ext.data.JsonStore({
        data: dataObject,
        root: 'elements',
        fields: ['name', 'value']

    });
    var configGrid = new Ext.grid.GridPanel({
        store: configGridData,
        loadMask: true,
        columns: [new Ext.grid.RowNumberer(), {
            id: 'name',
            header: "Name",
            width: 160,
            sortable: true,
            dataIndex: 'name'
        }, {
            header: "Value",
            width: 240,
            sortable: true,
            dataIndex: 'value'
        } ],
        stripeRows: true,
        autoHeight: true,
        autoScroll: true,
        frame: false,
        width: 600
    });
    var win = new Ext.Window({
        title: windowTitle,
        closable: true,
        autoWidth: true,
        autoHeight: true,
        plain: true,
        items: [configGrid]
    });
    win.show();
}

/* 
 * create the coord jobs store
 */
var coord_jobs_store = new Ext.data.JsonStore({
    baseParams: {
        jobtype: "coord",
        filter: "status=RUNNING;status=RUNNINGWITHERROR",
        timezone: getTimeZone()
    },
    idProperty: 'coordJobId',
    totalProperty: 'total',
    autoLoad: false,
    root: 'coordinatorjobs',
    fields: ['coordJobId', 'coordJobName', 'status', 'user', 'group', 'frequency', 'timeUnit', {name: 'startTime', sortType: Ext.data.SortTypes.asDate}, {name: 'nextMaterializedTime', sortType: Ext.data.SortTypes.asDate}],
    proxy: new Ext.data.HttpProxy({
        url: getOozieBase() + 'jobs'
    })
});
coord_jobs_store.proxy.conn.timeout = 300000;
coord_jobs_store.proxy.conn.method = "GET";

/* 
 * create the wf jobs store
 */
var jobs_store = new Ext.data.JsonStore({
    baseParams: {
        filter: "status=RUNNING",
        timezone: getTimeZone()
    },
    idProperty: 'id',
    totalProperty: 'total',
    autoLoad: true,
    root: 'workflows',
    fields: ['appPath', 'appName', 'id', 'conf', 'status', {name: 'createdTime', sortType: Ext.data.SortTypes.asDate}, {name: 'startTime', sortType: Ext.data.SortTypes.asDate}, {name: 'lastModTime', sortType: Ext.data.SortTypes.asDate}, {name: 'endTime', sortType: Ext.data.SortTypes.asDate}, 'user', 'group', 'run', 'actions'],
    proxy: new Ext.data.HttpProxy({
        url: getOozieBase() + 'jobs'
    })
});
jobs_store.proxy.conn.timeout = 300000;
jobs_store.proxy.conn.method = "GET";

/* 
 * create the bundle jobs store
 */
var bundle_jobs_store = new Ext.data.JsonStore({

    baseParams: {
        jobtype: "bundle",
        filter: "status=RUNNING;status=RUNNINGWITHERROR",
        timezone: getTimeZone()
    },
    idProperty: 'bundleJobId',
    totalProperty: 'total',
    autoLoad: false,
    root: 'bundlejobs',
    fields: ['bundleJobId', 'bundleJobName', 'bundleJobPath', 'conf', 'status', {name: 'kickoffTime', sortType: Ext.data.SortTypes.asDate}, {name: 'startTime', sortType: Ext.data.SortTypes.asDate}, {name: 'endTime', sortType: Ext.data.SortTypes.asDate}, {name: 'pauseTime', sortType: Ext.data.SortTypes.asDate}, {name: 'createdTime', sortType: Ext.data.SortTypes.asDate}, 'user', 'group', 'bundleCoordJobs'],
    proxy: new Ext.data.HttpProxy({
        url: getOozieBase() + 'jobs'
    })
});
bundle_jobs_store.proxy.conn.timeout = 300000;
bundle_jobs_store.proxy.conn.method = "GET";

var configGridData = new Ext.data.JsonStore({
    data: {
        elements: []
    },
    root: 'elements',
    fields: ['name', 'value', 'ovalue']

});
function getConfigObject(responseTxt) {
    var fo = {
        elements: []
    };
    var responseObj = eval('(' + responseTxt + ')');
    var j = 0;
    for (var i in responseObj) {
        fo.elements[j] = {};
        fo.elements[j].name = i;
        fo.elements[j].value = responseObj[i];
        j ++;
    }
    return fo;
}
// All the actions
var refreshCustomJobsAction = new Ext.Action({
    text: Ext.state.Manager.get('CustomWFJobFilter','status=KILLED'),
    handler: function() {
        jobs_store.baseParams.filter = getCustomFilter() ? getCustomFilter() + ";" + this.text : this.text;
        jobs_store.baseParams.timezone = getTimeZone();
        jobs_store.reload();
        Ext.getCmp('jobs_active').setIconClass('');
        Ext.getCmp('jobs_all').setIconClass('');
        Ext.getCmp('jobs_done').setIconClass('');
    }

});

var refreshActiveJobsAction = new Ext.Action({
    id: 'jobs_active',
    text: 'Active Jobs',
    handler: function() {
        jobs_store.baseParams.filter = getCustomFilter() ? getCustomFilter() + ';status=RUNNING' : 'status=RUNNING';
        jobs_store.baseParams.timezone = getTimeZone();
        jobs_store.reload();
        Ext.getCmp('jobs_active').setIconClass('job-filter');
        Ext.getCmp('jobs_all').setIconClass('');
        Ext.getCmp('jobs_done').setIconClass('');

    }
});

var refreshAllJobsAction = new Ext.Action({
    id: 'jobs_all',
    text: 'All Jobs',
    handler: function() {
        jobs_store.baseParams.filter = getCustomFilter()? getCustomFilter() : "";
        jobs_store.baseParams.timezone = getTimeZone();
        jobs_store.reload();
        Ext.getCmp('jobs_active').setIconClass('');
        Ext.getCmp('jobs_all').setIconClass('job-filter');
        Ext.getCmp('jobs_done').setIconClass('');
    }

});

var refreshDoneJobsAction = new Ext.Action({
    id: 'jobs_done',
    text: 'Done Jobs',
    handler: function() {
        var doneJobStatus = 'status=SUCCEEDED;status=KILLED;status=FAILED';
        jobs_store.baseParams.filter = getCustomFilter() ? getCustomFilter() + ';' + doneJobStatus : doneJobStatus;
        jobs_store.baseParams.timezone = getTimeZone();
        jobs_store.reload();
        Ext.getCmp('jobs_active').setIconClass('');
        Ext.getCmp('jobs_all').setIconClass('');
        Ext.getCmp('jobs_done').setIconClass('job-filter');
    }
});

var refreshCoordCustomJobsAction = new Ext.Action({
    text: Ext.state.Manager.get('CustomCoordJobFilter', 'status=KILLED'),
    handler: function() {
        coord_jobs_store.baseParams.filter = getCustomFilter() ? getCustomFilter() + ';' + this.text : this.text;
        coord_jobs_store.baseParams.timezone = getTimeZone();
        coord_jobs_store.reload();
        Ext.getCmp('coord_active').setIconClass('');
        Ext.getCmp('coord_all').setIconClass('');
        Ext.getCmp('coord_done').setIconClass('');
    }
});


var refreshCoordActiveJobsAction = new Ext.Action({
    id: 'coord_active',
    text: 'Active Jobs',
    handler: function() {
        var coordActiveStatus = 'status=RUNNING;status=RUNNINGWITHERROR';
        coord_jobs_store.baseParams.filter = getCustomFilter() ? getCustomFilter() + ';' + coordActiveStatus : coordActiveStatus;
        coord_jobs_store.baseParams.timezone = getTimeZone();
        coord_jobs_store.reload();
        Ext.getCmp('coord_active').setIconClass('job-filter');
        Ext.getCmp('coord_all').setIconClass('');
        Ext.getCmp('coord_done').setIconClass('');
        /*
         Ext.Ajax.request( {
         url: getOozieBase() + 'jobs/?jobtype=coord',
         success: function(response, request) {
         var coordData = getConfigObject(response.responseText);
         coord_jobs_store.loadData(coordData);
         },
         });
         */
    }
});

var refreshCoordAllJobsAction = new Ext.Action({
    id: 'coord_all',
    text: 'All Jobs',
    handler: function() {
        coord_jobs_store.baseParams.filter = getCustomFilter() ? getCustomFilter() : '';
        coord_jobs_store.baseParams.timezone = getTimeZone();
        coord_jobs_store.reload();
        Ext.getCmp('coord_active').setIconClass('');
        Ext.getCmp('coord_all').setIconClass('job-filter');
        Ext.getCmp('coord_done').setIconClass('');
        /*
         Ext.Ajax.request( {
         url: getOozieBase() + 'jobs/?jobtype=coord',
         success: function(response, request) {
         var coordData = getConfigObject(response.responseText);
         coord_jobs_store.loadData(coordData);
         },
         });
         */
    }
});

var refreshCoordDoneJobsAction = new Ext.Action({
    id: 'coord_done',
    text: 'Done Jobs',
    handler: function() {
        var coordDoneStatus = 'status=SUCCEEDED;status=KILLED;status=FAILED;status=DONEWITHERROR';
        coord_jobs_store.baseParams.filter = getCustomFilter() ? getCustomFilter() + ';' + coordDoneStatus : coordDoneStatus;
        coord_jobs_store.baseParams.timezone = getTimeZone();
        coord_jobs_store.reload();
        Ext.getCmp('coord_active').setIconClass('');
        Ext.getCmp('coord_all').setIconClass('');
        Ext.getCmp('coord_done').setIconClass('job-filter');
        /*
         Ext.Ajax.request( {
         url: getOozieBase() + 'jobs' + '?jobtype=coord',
         success: function(response, request) {
         var coordData = getConfigObject(response.responseText);
         coord_jobs_store.loadData(coordData);
         },
         });
         */
    }
});

var refreshBundleActiveJobsAction = new Ext.Action({
    id: 'bundle_active',
    text: 'Active Jobs',
    handler: function() {
        var bundleActiveStatus = 'status=RUNNING;status=RUNNINGWITHERROR';
        bundle_jobs_store.baseParams.filter = getCustomFilter() ? getCustomFilter() + ';' + bundleActiveStatus : bundleActiveStatus;
        bundle_jobs_store.baseParams.timezone = getTimeZone();
        bundle_jobs_store.reload();
        Ext.getCmp('bundle_active').setIconClass('job-filter');
        Ext.getCmp('bundle_all').setIconClass('');
        Ext.getCmp('bundle_done').setIconClass('');
    }
});

var refreshBundleAllJobsAction = new Ext.Action({
    id: 'bundle_all',
    text: 'All Jobs',
    handler: function() {
        bundle_jobs_store.baseParams.filter = getCustomFilter() ? getCustomFilter() : '';
        bundle_jobs_store.baseParams.timezone = getTimeZone();
        bundle_jobs_store.reload();
        Ext.getCmp('bundle_active').setIconClass('');
        Ext.getCmp('bundle_all').setIconClass('job-filter');
        Ext.getCmp('bundle_done').setIconClass('');
    }
});

var refreshBundleDoneJobsAction = new Ext.Action({
    id: 'bundle_done',
    text: 'Done Jobs',
    handler: function() {
        var bundleDoneStatus = 'status=SUCCEEDED;status=KILLED;status=FAILED;status=DONEWITHERROR';
        bundle_jobs_store.baseParams.filter = getCustomFilter() ? getCustomFilter() + ';' + bundleDoneStatus : bundleDoneStatus
        bundle_jobs_store.baseParams.timezone = getTimeZone();
        bundle_jobs_store.reload();
        Ext.getCmp('bundle_active').setIconClass('');
        Ext.getCmp('bundle_all').setIconClass('');
        Ext.getCmp('bundle_done').setIconClass('job-filter');
    }
});

var refreshBundleCustomJobsAction = new Ext.Action({
    text: Ext.state.Manager.get('CustomBundleJobFilter', 'status=KILLED'),
    handler: function() {
        bundle_jobs_store.baseParams.filter = getCustomFilter() ? getCustomFilter() + this.text : this.text;
        bundle_jobs_store.baseParams.timezone = getTimeZone();
        bundle_jobs_store.reload();
        Ext.getCmp('bundle_active').setIconClass('');
        Ext.getCmp('bundle_all').setIconClass('');
        Ext.getCmp('bundle_done').setIconClass('');
    }
});

var helpFilterAction = new Ext.Action({
    text: 'Help',
    handler: function() {
        Ext.Msg.show({
            title:'Filter Help!',
            msg: 'Results in this console can be filtered by "status".\n "status" can have values "RUNNING", "SUCCEEDED", "KILLED", "FAILED".\n To add multiple filters, use ";" as the separator. \nFor ex. "status=KILLED;status=SUCCEEDED" will return jobs which are either in SUCCEEDED or KILLED status',
            buttons: Ext.Msg.OK,
            icon: Ext.MessageBox.INFO
        });
    }
});

var changeFilterAction = new Ext.Action({
    text: 'Custom Filter',
    handler: function() {
        Ext.Msg.prompt('Filter Criteria', 'Filter text:', function(btn, text) {
            if (btn == 'ok' && text) {
                var filter = convertStatusToUpperCase(text);
                refreshCustomJobsAction.setText(filter);
                Ext.state.Manager.setProvider(new Ext.state.CookieProvider({
                    expires: new Date(new Date().getTime()+315569259747)
                }));
                Ext.state.Manager.set('CustomWFJobFilter', filter);
                jobs_store.baseParams.filter = getCustomFilter() ? getCustomFilter() + ";" + filter : filter;
                jobs_store.baseParams.timezone = getTimeZone();
                jobs_store.reload();
            }
        });
    }
});

var changeCoordFilterAction = new Ext.Action({
    text: 'Custom Filter',
    handler: function() {
        Ext.Msg.prompt('Filter Criteria', 'Filter text:', function(btn, text) {
            if (btn == 'ok' && text) {
                var filter = convertStatusToUpperCase(text);
                refreshCoordCustomJobsAction.setText(filter);
                Ext.state.Manager.setProvider(new Ext.state.CookieProvider({
                    expires: new Date(new Date().getTime()+315569259747)
                }));
                Ext.state.Manager.set("CustomCoordJobFilter", filter);
                coord_jobs_store.baseParams.filter = getCustomFilter() ? getCustomFilter() + ";" + filter : filter;
                coord_jobs_store.baseParams.timezone = getTimeZone();
                coord_jobs_store.reload();
            }
        });
    }
});

var changeBundleFilterAction = new Ext.Action({
    text: 'Custom Filter',
    handler: function() {
        Ext.Msg.prompt('Filter Criteria', 'Filter text:', function(btn, text) {
            if (btn == 'ok' && text) {
                var filter = convertStatusToUpperCase(text);
                refreshBundleCustomJobsAction.setText(filter);
                Ext.state.Manager.setProvider(new Ext.state.CookieProvider({
                    expires: new Date(new Date().getTime()+315569259747)
                }));
                Ext.state.Manager.set("CustomBundleJobFilter", filter);
                bundle_jobs_store.baseParams.filter = getCustomFilter() ? getCustomFilter() + ";" + filter : filter;
                bundle_jobs_store.baseParams.timezone = getTimeZone();
                bundle_jobs_store.reload();
            }
        });
    }
});


var getSupportedVersions = new Ext.Action({
    text: 'Checking server for supported versions...',
    handler: function() {
        Ext.Ajax.request({
            url: getOozieVersionsUrl(),
            success: function(response, request) {
                var versions = JSON.parse(response.responseText);
                for (var i = 0; i < versions.length; i += 1) {
                    if (versions[i] == getOozieClientVersion()) {
                        initConsole();
                        return;
                    }
                }
                Ext.Msg.alert('Oozie Console Alert!', 'Server doesn\'t support client version: v' + getOozieClientVersion());
            }

        });
    }

});

var checkStatus = new Ext.Action({
    text: 'Status - Unknown',
    handler: function() {
        Ext.Ajax.request({
            url: getOozieBase() + 'admin/status',
            success: function(response, request) {
                var status = eval("(" + response.responseText + ")");
                if (status.safeMode) {
                    checkStatus.setText("<font color='700000' size='2> Safe Mode - ON </font>");
                }
                else {
                    checkStatus.setText("<font color='007000' size='2> Status - Normal</font>");
                }
            }
        });
    }
});


var serverVersion = new Ext.Action({
    text: 'Oozie Server Version',
    handler: function() {
        Ext.Ajax.request({
            url: getOozieBase() + 'admin/build-version',
            success: function(response, request) {
                var ret = eval("(" + response.responseText + ")");
                serverVersion.setText("<font size='2'>Server version [" + ret['buildVersion'] + "]</font>");
            }
        });
    }
});

var viewConfig = new Ext.Action({
    text: 'Configuration',
    handler: function() {
        Ext.Ajax.request({
            url: getOozieBase() + 'admin/configuration',
            success: function(response, request) {
                var configData = getConfigObject(response.responseText);
                configGridData.loadData(configData);
            }

        });
    }

});
var viewInstrumentation = new Ext.Action({
    text: "&nbsp;&nbsp;&nbsp;",
    icon: 'ext-2.2/resources/images/default/grid/refresh.gif',
    handler: function() {
        Ext.Ajax.request({
            url: getOozieBase() + 'admin/instrumentation',
            success: function(response, request) {
                var jsonData = eval("(" + response.responseText + ")");
                var timers = treeNodeFromJsonInstrumentation(jsonData["timers"], "timers");
                timers.expanded = false;
                var samplers = treeNodeFromJsonInstrumentation(jsonData["samplers"], "samplers");
                samplers.expanded = false;
                var counters = treeNodeFromJsonInstrumentation(jsonData["counters"], "counters");
                counters.expanded = false;
                var variables = treeNodeFromJsonInstrumentation(jsonData["variables"], "variables");
                variables.expanded = false;
                while (instrumentationTreeRoot.hasChildNodes()) {
                    var child = instrumentationTreeRoot.firstChild;
                    instrumentationTreeRoot.removeChild(child);
                }
                instrumentationTreeRoot.appendChild(samplers);
                instrumentationTreeRoot.appendChild(counters);
                instrumentationTreeRoot.appendChild(timers);
                instrumentationTreeRoot.appendChild(variables);
                instrumentationTreeRoot.expand(false, true);
            }

        });
    }

});
var viewMetrics = new Ext.Action({
    text: "&nbsp;&nbsp;&nbsp;",
    icon: 'ext-2.2/resources/images/default/grid/refresh.gif',
    handler: function() {
        Ext.Ajax.request({
            url: getOozieBase() + 'admin/metrics',
            success: function(response, request) {
                var jsonData = eval("(" + response.responseText + ")");
                var timers = treeNodeFromJsonMetrics(jsonData["timers"], "timers");
                timers.expanded = false;
                var histograms = treeNodeFromJsonMetrics(jsonData["histograms"], "histograms");
                histograms.expanded = false;
                var counters = treeNodeFromJsonMetrics(jsonData["counters"], "counters");
                counters.expanded = false;
                var gauges = treeNodeFromJsonMetrics(jsonData["gauges"], "gauges");
                gauges.expanded = false;
                while (metricsTreeRoot.hasChildNodes()) {
                    var child = metricsTreeRoot.firstChild;
                    metricsTreeRoot.removeChild(child);
                }
                metricsTreeRoot.appendChild(counters);
                metricsTreeRoot.appendChild(timers);
                metricsTreeRoot.appendChild(histograms);
                metricsTreeRoot.appendChild(gauges);
                metricsTreeRoot.expand(false, true);
            }

        });
    }

});

var viewSystemDetails = new Ext.Action({
    text: 'Java System Props',
    handler: function() {
        Ext.Ajax.request({
            url: getOozieBase() + 'admin/java-sys-properties',
            success: function(response, request) {
                var configData = getConfigObject(response.responseText);
                configGridData.loadData(configData);
            }

        });
    }

});

var viewOSDetails = new Ext.Action({
    text: 'OS Env',
    handler: function() {
        Ext.Ajax.request({
            url: getOozieBase() + 'admin/os-env',
            success: function(response, request) {
                var configData = getConfigObject(response.responseText);
                configGridData.loadData(configData);
            }
        });
    }
});

var instrumentationTreeRoot = new Ext.tree.TreeNode({
    text: "Instrumentation",
    expanded: true
});

var metricsTreeRoot = new Ext.tree.TreeNode({
    text: "Metrics",
    expanded: true
});

var timeZones_store = new Ext.data.JsonStore({
    autoLoad: true,
    root: 'available-timezones',
    fields: ['timezoneDisplayName','timezoneId'],
    proxy: new Ext.data.HttpProxy({
        url: getOozieBase() + 'admin' + "/available-timezones"
    })
});
timeZones_store.proxy.conn.timeout = 300000;
timeZones_store.proxy.conn.method = "GET";

function showCoordJobContextMenu(thisGrid, rowIndex, cellIndex, e) {
    var coordJobId = thisGrid.store.data.items[rowIndex].data.coordJobId;
    coordJobDetailsGridWindow(coordJobId);
}

function initConsole() {
    function showJobContextMenu(thisGrid, rowIndex, cellIndex, e) {
        var workflowId = thisGrid.store.data.items[rowIndex].data.id;
        jobDetailsGridWindow(workflowId);
    }

    function showBundleJobContextMenu(thisGrid, rowIndex, cellIndex, e) {
        var bundleJobId = thisGrid.store.data.items[rowIndex].data.bundleJobId;
        bundleJobDetailsGridWindow(bundleJobId);
    }

    var jobs_grid = new Ext.grid.GridPanel({
        store: jobs_store,
        loadMask: true,
        columns: [new Ext.grid.RowNumberer(), {
            id: 'id',
            header: "Job Id",
            width: 220,
            sortable: true,
            dataIndex: 'id'
        }, {
            header: "Name",
            width: 100,
            sortable: true,
            dataIndex: 'appName'
        }, {
            header: "Status",
            width: 70,
            sortable: true,
            dataIndex: 'status'
        }, {
            header: "Run",
            width: 30,
            sortable: true,
            dataIndex: 'run'
        }, {
            header: "User",
            width: 60,
            sortable: true,
            dataIndex: 'user'
        }, {
            header: "Group",
            width: 60,
            sortable: true,
            dataIndex: 'group'
        }, {
            header: "Created",
            width: 170,
            sortable: true,
            dataIndex: 'createdTime'
        }, {
            header: "Started",
            width: 170,
            sortable: true,
            dataIndex: 'startTime'
        }, {
            header: "Last Modified",
            width: 170,
            sortable: true,
            dataIndex: 'lastModTime'
        }, {
            header: "Ended",
            width: 170,
            sortable: true,
            dataIndex: 'endTime'
        } ],

        stripeRows: true,
        autoScroll: true,
        frame: false,
        width: 1050,
        tbar: [ {
            text: "&nbsp;&nbsp;&nbsp;",
            icon: 'ext-2.2/resources/images/default/grid/refresh.gif',
            handler: function() {
                jobs_store.baseParams.filter = getCustomFilter();
                jobs_store.baseParams.timezone = getTimeZone();
                jobs_store.reload();
            }
        }, refreshAllJobsAction, refreshActiveJobsAction, refreshDoneJobsAction, {
            text: 'Custom Filter',
            menu: [refreshCustomJobsAction, changeFilterAction, helpFilterAction ]
        }, {
            xtype: 'tbfill'
        }, checkStatus, serverVersion],
        title: 'Workflow Jobs',
        bbar: getPagingBar(jobs_store),
        listeners: {
            cellclick: {
                fn: showJobContextMenu
            }
        }
    });
    var expander = new Ext.grid.RowExpander({
        tpl: new Ext.Template('<br><p><b>Name:</b> {name}</p>', '<p><b>Value:</b> {value}</p>')
    });
    var adminGrid = new Ext.grid.GridPanel({
        store: configGridData,
        loadMask: true,
        columns: [expander, {
            id: 'name',
            header: "Name",
            width: 300,
            sortable: true,
            dataIndex: 'name'
        }, {
            id: 'value',
            header: "Value",
            width: 740,
            sortable: true,
            renderer: valueRenderer,
            dataIndex: 'value'
        } ],
        height: 500,
        width: 1040,
        autoScroll: true,
        frame: false,
        tbar: [viewConfig, viewSystemDetails, viewOSDetails, {
            xtype: 'tbfill'
        }, checkStatus, serverVersion],
        viewConfig: {
            forceFit: true

        },
        plugins: expander,
        collapsible: true,
        animCollapse: false,
        title: "System Info"
    });
    var instrumentationArea = new Ext.tree.TreePanel({
        autoScroll: true,
        useArrows: true,
        height: 300,
        root: instrumentationTreeRoot,
        tbar: [viewInstrumentation, {
            xtype: 'tbfill'
        }, checkStatus, serverVersion],
        title: 'Instrumentation'
    });
    var metricsArea = new Ext.tree.TreePanel({
        autoScroll: true,
        useArrows: true,
        height: 300,
        root: metricsTreeRoot,
        tbar: [viewMetrics, {
            xtype: 'tbfill'
        }, checkStatus, serverVersion],
        title: 'Metrics'
    });

    var slaDashboard = new Ext.Panel({
        title: 'SLA',
        autoLoad : {
            url : 'console/sla/oozie-sla.html',
            scripts: true
        }
    });

    var coordJobArea = new Ext.grid.GridPanel({
        store: coord_jobs_store,
        loadMask: true,
        columns: [new Ext.grid.RowNumberer(), {
            id: 'id',
            header: "Job Id",
            width: 230,
            sortable: true,
            dataIndex: 'coordJobId'
        }, {
            header: "Name",
            width: 100,
            sortable: true,
            dataIndex: 'coordJobName'
        }, {
            header: "Status",
            width: 80,
            sortable: true,
            dataIndex: 'status'
    }, {
            header: "User",
            width: 80,
            sortable: true,
            dataIndex: 'user'
        }, {
            header: "Group",
            width: 80,
            sortable: true,
            dataIndex: 'group'
        }, {
            header: "Frequency",
            width: 70,
            sortable: true,
            dataIndex: 'frequency'
        }, {
            header: "Unit",
            width: 60,
            sortable: true,
            dataIndex: 'timeUnit'
        }, {
            header: "Started",
            width: 170,
            sortable: true,
            dataIndex: 'startTime'
        }, {
            header: "Next Materialization",
            width: 170,
            sortable: true,
            dataIndex: 'nextMaterializedTime'
        }],

        stripeRows: true,
        autoScroll: true,
        useArrows: true,
        height: 300,
        tbar: [ {
            text: "&nbsp;&nbsp;&nbsp;",
            icon: 'ext-2.2/resources/images/default/grid/refresh.gif',
            handler: function() {
                coord_jobs_store.baseParams.filter = getCustomFilter();
                coord_jobs_store.baseParams.timezone = getTimeZone();
                coord_jobs_store.reload();
            }
        }, refreshCoordAllJobsAction, refreshCoordActiveJobsAction, refreshCoordDoneJobsAction,

{
            text: 'Custom Filter',
            menu: [refreshCoordCustomJobsAction, changeCoordFilterAction, helpFilterAction ]
        },

            {
                xtype: 'tbfill'
            }, checkStatus, serverVersion],
        title: 'Coordinator Jobs',
        bbar: getPagingBar(coord_jobs_store),
        listeners: {
            cellclick: {
                fn: showCoordJobContextMenu
            }
        }
    });
    var bundleJobArea = new Ext.grid.GridPanel({
        store: bundle_jobs_store,
        loadMask: true,
        columns: [new Ext.grid.RowNumberer(), {
            id: 'id',
            header: "Job Id",
            width: 230,
            sortable: true,
            dataIndex: 'bundleJobId'
        }, {
            header: "Name",
            width: 80,
            sortable: true,
            dataIndex: 'bundleJobName'
        }, {
            header: "Status",
            width: 80,
            sortable: true,
            dataIndex: 'status'
        }, {
            header: "User",
            width: 60,
            sortable: true,
            dataIndex: 'user'
        }, {
            header: "Group",
            width: 60,
            sortable: true,
            dataIndex: 'group'
        }, {
            header: "Kickoff Time",
            width: 170,
            sortable: true,
            dataIndex: 'kickoffTime'
        }, {
            header: "Created Time",
            width: 170,
            sortable: true,
            dataIndex: 'createdTime'
        }],

        stripeRows: true,
        autoScroll: true,
        useArrows: true,
        height: 300,
        tbar: [ {
            text: "&nbsp;&nbsp;&nbsp;",
            icon: 'ext-2.2/resources/images/default/grid/refresh.gif',
            handler: function() {
                bundle_jobs_store.baseParams.filter = getCustomFilter();
                bundle_jobs_store.baseParams.timezone = getTimeZone();
                bundle_jobs_store.reload();
            }
        }, refreshBundleAllJobsAction, refreshBundleActiveJobsAction, refreshBundleDoneJobsAction,
        {
            text: 'Custom Filter',
            menu: [refreshBundleCustomJobsAction, changeBundleFilterAction, helpFilterAction ]
        },
            {
                xtype: 'tbfill'
            }, checkStatus, serverVersion],
        title: 'Bundle Jobs',
        bbar: getPagingBar(bundle_jobs_store),
        listeners: {
            cellclick: {
                fn: showBundleJobContextMenu
            }
        }
    });
    Ext.state.Manager.setProvider(new Ext.state.CookieProvider());
    var currentTimezone = Ext.state.Manager.get("TimezoneId","GMT");
    var userName = Ext.state.Manager.get("UserName","");
    var commonFilter = Ext.state.Manager.get("GlobalCustomFilter","");
    var settingsArea = new Ext.FormPanel({
        title: 'Settings',
        items: [{
            xtype: 'combo',
            width: 300,
            fieldLabel: 'Timezone',
            emptyText: 'Select a timezone...',
            store: timeZones_store,
            displayField: 'timezoneDisplayName',
            valueField: 'timezoneId',
            selectOnFocus: true,
            mode: 'local',
            typeAhead: true,
            editable: false,
            triggerAction: 'all',
            value: currentTimezone,
            listeners:
            { select: { fn:function(combo, value)
                {
                    Ext.state.Manager.setProvider(new Ext.state.CookieProvider({
                        expires: new Date(new Date().getTime()+315569259747) // about 10 years from now!
                    }));
                    Ext.state.Manager.set("TimezoneId",this.value);
                }
            }}
        },{
            xtype: 'label',
            width: 300,
            style: 'font: normal 12px tahoma',
            text: 'Global Filter'
        },{
            xtype: 'field',
            width: 300,
            fieldLabel: '- by User Name',
            inputType: 'text',
            typeAhead: true,
            editable: false,
            triggerAction: 'all',
            value: userName,
            listeners:
            { change: { fn:function(field, value)
                {
                    Ext.state.Manager.setProvider(new Ext.state.CookieProvider({
                        expires: new Date(new Date().getTime()+315569259747) // about 10 years from now!
                    }));
                    Ext.state.Manager.set("UserName", value);
                }
            }}
        },{
            xtype: 'field',
            width: 300,
            fieldLabel: '- by Filter Text',
            inputType: 'text',
            typeAhead: true,
            editable: false,
            triggerAction: 'all',
            value: commonFilter,
            listeners:
            { change: { fn:function(field, value)
                {
                    Ext.state.Manager.setProvider(new Ext.state.CookieProvider({
                        expires: new Date(new Date().getTime()+315569259747) // about 10 years from now!
                    }));
                    var upper_value = convertStatusToUpperCase(value);
                    Ext.state.Manager.set("GlobalCustomFilter", upper_value);
                }
            }}
        }]
    });
    // main tab panel containing Workflow Jobs, Coordinator Jobs, Bundle Jobs, System Info, ...
    var tabs = new Ext.TabPanel({
        renderTo: 'oozie-console',
        height: 580,
        title: "Oozie Web Console"

    });
    tabs.add(jobs_grid);
    tabs.add(coordJobArea);
    tabs.add(bundleJobArea);
    if (isSLAServiceEnabled == "true") {
        tabs.add(slaDashboard);
    }
    tabs.add(adminGrid);
    if (isInstrumentationServiceEnabled == "true") {
        tabs.add(instrumentationArea);
    }
    if (isMetricsInstrumentationServiceEnabled == "true") {
        tabs.add(metricsArea);
    }
    tabs.add(settingsArea);
    tabs.setActiveTab(jobs_grid);
    // showing Workflow Jobs active tab as default
    // and loading coord,bundle only when tab opened
    Ext.getCmp('jobs_active').setIconClass('job-filter');
    var isFirstLoadCoord = 0;
    var isFirstLoadBundle = 0;
    tabs.addListener("tabchange", function(panel, selectedTab) {
        if (selectedTab.title == 'Workflow Jobs') {
            jobs_grid.setVisible(true);
            return;
        }
        else if (selectedTab.title == 'Coordinator Jobs') {
            if (isFirstLoadCoord == 0) {
                refreshCoordActiveJobsAction.execute();
                isFirstLoadCoord = 1;
            }
            coordJobArea.setVisible(true);
            return;
        }
        else if (selectedTab.title == 'Bundle Jobs') {
           if (isFirstLoadBundle == 0) {
                refreshBundleActiveJobsAction.execute();
                isFirstLoadBundle = 1;
            }
            bundleJobArea.setVisible(true);
            return;
        }
    });
    checkStatus.execute();
    viewConfig.execute();
    serverVersion.execute();
    if (isInstrumentationServiceEnabled == "true") {
        viewInstrumentation.execute();
    }
    if (isMetricsInstrumentationServiceEnabled == "true") {
        viewMetrics.execute();
    }
    var jobId = getReqParam("job");
    if (jobId != "") {
        if (jobId.endsWith("-C")) {
            coordJobDetailsGridWindow(jobId);
        }
        else if (jobId.endsWith("-B")) {
            bundleJobDetailsGridWindow(jobId);
        }
        else if (jobId.endsWith("-W")) {
            jobDetailsGridWindow(jobId);
        }
        else if (jobId.indexOf("-C@") !=-1) {
            coordActionDetailsGridWindow(jobId);
        }
        else if (jobId.endsWith("-W@") !=-1) {
            jobId = jobId.substring(0, index + 2);
            jobDetailsGridWindow(jobId);
        }

    }
}
// now the on ready function
Ext.onReady(function() {
    getSupportedVersions.execute();
});
