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

// Warn about dependencies
if (typeof Ext == 'undefined'){
    var warning = 'Missing JavaScript dependencies.';
    var dependencies = document.getElementById('dependencies');
    if (dependencies){
        warning += "\n" + (dependencies.innerText || dependencies.textContent);
        dependencies.style.display = '';
    }
    throw new Error(warning);
}

//so it works from remote browsers, "http://localhost:8080";
var oozie_host = "";
var flattenedObject;

function getOozieClientVersion() {
    return 1;
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

function treeNodeFromJson(json, rootText) {
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
                        c = treeNodeFromJson(json[i]['data'], json[i]['group']);
                    }
                    else {
                        c = treeNodeFromJson(json[i], json[i]['name']);
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

// Image object display
Ext.ux.Image = Ext.extend(Ext.BoxComponent, {

    url: Ext.BLANK_IMAGE_URL,  //for initial src value

    autoEl: {
        tag: 'img',
        src: Ext.BLANK_IMAGE_URL,
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
    }
});

// stuff to show details of a job
function jobDetailsPopup(response, request) {
    var jobDefinitionArea = new Ext.form.TextArea({
        fieldLabel: 'Definition',
        editable: false,
        name: 'definition',
        width: 1005,
        height: 400,
        autoScroll: true,
        emptyText: "Loading..."
    });
    var jobLogArea = new Ext.form.TextArea({
        fieldLabel: 'Logs',
        editable: false,
        name: 'logs',
        width: 1010,
        height: 400,
        autoScroll: true,
        emptyText: "Loading..."
    });
    function fetchDefinition(workflowId) {
        Ext.Ajax.request({
            url: getOozieBase() + 'job/' + workflowId + "?show=definition",
            success: function(response, request) {
                jobDefinitionArea.setRawValue(response.responseText);
            }

        });
    }
    function fetchLogs(workflowId) {
        Ext.Ajax.request({
            url: getOozieBase() + 'job/' + workflowId + "?show=log",
            success: function(response, request) {
                jobLogArea.setRawValue(response.responseText);
            }

        });
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
            width: 200,
            value: jobDetails["id"]
        }, {
            fieldLabel: 'Name',
            editable: false,
            name: 'appName',
            width: 200,
            value: jobDetails["appName"]
        }, {
            fieldLabel: 'App Path',
            editable: false,
            name: 'appPath',
            width: 200,
            value: jobDetails["appPath"]
        }, {
            fieldLabel: 'Run',
            editable: false,
            name: 'run',
            width: 200,
            value: jobDetails["run"]
        }, {
            fieldLabel: 'Status',
            editable: false,
            name: 'status',
            width: 200,
            value: jobDetails["status"]
        }, {
            fieldLabel: 'User',
            editable: false,
            name: 'user',
            width: 200,
            value: jobDetails["user"]
        }, {
            fieldLabel: 'Group',
            editable: false,
            name: 'group',
            width: 200,
            value: jobDetails["group"]
        }, {
            fieldLabel: 'Create Time',
            editable: false,
            name: 'createdTime',
            width: 200,
            value: jobDetails["createdTime"]
        }, {
            fieldLabel: 'Nominal Time',
            editable: false,
            name: 'nominalTime',
            width: 200,
            value: jobDetails["nominalTime"]

        }, {
            fieldLabel: 'Start Time',
            editable: false,
            name: 'startTime',
            width: 200,
            value: jobDetails["startTime"]
        }, {
            fieldLabel: 'Last Modified',
            editable: false,
            name: 'lastModTime',
            width: 200,
            value: jobDetails["lastModTime"]
        },{
            fieldLabel: 'End Time',
            editable: false,
            name: 'endTime',
            width: 200,
            value: jobDetails["endTime"]
        }, ]
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
                    url: getOozieBase() + 'job/' + workflowId + "?timezone=" + getTimeZone(),
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
            width: 240,
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
        }, ],
        stripeRows: true,
        // autoHeight: true,
        autoScroll: true,
        frame: true,
        height: 400,
        width: 1200,
        title: 'Actions',
        listeners: {
            cellclick: {
                fn: showActionContextMenu
            }
        }

    });
    function showActionContextMenu(thisGrid, rowIndex, cellIndex, e) {
        var jobContextMenu = new Ext.menu.Menu('taskContext');
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
                width: 540,
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
                    }, {
                        title: 'Child Job URLs',
                        autoScroll: true,
                        frame: true,
                        labelAlign: 'right',
                        labelWidth: 70,
                        items: urlUnit
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
            win.setPosition(50, 50);
            win.show();
        }
    }

	function populateUrlUnit(actionStatus, urlUnit) {
		var consoleUrl = actionStatus["consoleUrl"];
        var externalChildIDs = actionStatus["externalChildIDs"];
		if(undefined !== consoleUrl && null !== consoleUrl && undefined !== externalChildIDs && null !== externalChildIDs) {
	        var urlPrefix = consoleUrl.trim().split(/_/)[0];
            //externalChildIds is a comma-separated string of each child job ID.
            //Create URL list by appending jobID portion after stripping "job"
            var jobIds = externalChildIDs.split(/,/);
            var count = 1;
            jobIds.forEach(function(jobId) {
                jobId = jobId.trim().split(/job/)[1];
		        var jobUrl = new Ext.form.TriggerField({
			        fieldLabel: 'Child Job ' + count,
			        editable: false,
			        name: 'childJobURLs',
			        width: 400,
			        value: urlPrefix + jobId,
			        triggerClass: 'x-form-search-trigger',
			        onTriggerClick: function() {
			            window.open(urlPrefix + jobId);
			        }
	            });
	            if(jobId != undefined) {
                    urlUnit.add(jobUrl);
	                count++;
	            }
	        });
        } else {
            var note = new Ext.form.TextField({
                fieldLabel: 'Child Job',
                value: 'n/a'
            });
            urlUnit.add(note);
        }
	}

    function refreshActionDetails(actionId, detail, urlUnit) {
        Ext.Ajax.request({
            url: getOozieBase() + 'job/' + actionId + "?timezone=" + getTimeZone(),
            success: function(response, request) {
                var results = eval("(" + response.responseText + ")");
                detail.getForm().setValues(results);
                urlUnit.getForm().setValues(results);
                populateUrlUnit(results, urlUnit);
            }
        });
    }

    var dagImg = new Ext.ux.Image({
                id: 'dagImage',
                url: getOozieBase() + 'job/' + workflowId + "?show=graph",
                readOnly: true,
                editable: false,
                autoScroll: true
    });

    function fetchDAG(workflowId) {
        dagImg.setSrc(getOozieBase() + 'job/' + workflowId + '?show=graph&token=' + Math.random());
    }

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
                width: 1010,
                height: 430,
                autoScroll: true,
                value: jobDetails["conf"]
            })
        }, {
            title: 'Job Log',
            items: jobLogArea,
            tbar: [ {
                text: "&nbsp;&nbsp;&nbsp;",
                icon: 'ext-2.2/resources/images/default/grid/refresh.gif',
                handler: function() {
                    fetchLogs(workflowId);
                }
            }]

        }, {
            title: 'Job DAG',
            items: dagImg,
            tbar: [{
                text: "&nbsp;&nbsp;&nbsp;",
                icon: 'ext-2.2/resources/images/default/grid/refresh.gif',
                handler: function() {
                    fetchDAG(workflowId);
                }
            }]
        }]
    });
    jobDetailsTab.addListener("tabchange", function(panel, selectedTab) {
        if (selectedTab.title == "Job Info") {
            jobs_grid.setVisible(true);
            return;
        }
        if (selectedTab.title == 'Job Log') {
            fetchLogs(workflowId);
        }
        else if (selectedTab.title == 'Job Definition') {
            fetchDefinition(workflowId);
        } else if(selectedTab.title == 'Job DAG') {
            fetchDAG(workflowId);
        }
        jobs_grid.setVisible(false);
    });
    var win = new Ext.Window({
        title: 'Job (Name: ' + appName + '/JobId: ' + workflowId + ')',
        closable: true,
        width: 1020,
        autoHeight: true,
        plain: true,
        items: [jobDetailsTab, jobs_grid]
    });
    win.setPosition(10, 10);
    win.show();
}

function coordJobDetailsPopup(response, request) {

    var jobDefinitionArea = new Ext.form.TextArea({
        fieldLabel: 'Definition',
        editable: false,
        name: 'definition',
        width: 1005,
        height: 400,
        autoScroll: true,
        emptyText: "Loading..."
    });
    var jobLogArea = new Ext.form.TextArea({
        fieldLabel: 'Logs',
        editable: false,
	id: 'jobLogAreaId',
        name: 'logs',
        width: 1010,
        height: 400,
        autoScroll: true,
        emptyText: "Loading..."
    });
    var getLogButton = new Ext.Button({
	    text: 'Retrieve log',
	    handler: function() {
                    fetchLogs(coordJobId, actionsTextBox.getValue());
	    }
    });
    var actionsTextBox = new Ext.form.TextField({
             fieldLabel: 'ActionsList',
             name: 'ActionsList',
             value: 'Enter the action list here'
         });
    function fetchDefinition(coordJobId) {
        Ext.Ajax.request({
            url: getOozieBase() + 'job/' + coordJobId + "?show=definition",
            success: function(response, request) {
                jobDefinitionArea.setRawValue(response.responseText);
            }
        });
    }
    function fetchLogs(coordJobId, actionsList) {
	if(actionsList=='') {
	    Ext.Ajax.request({
            url: getOozieBase() + 'job/' + coordJobId + "?show=log",
	        success: function(response, request) {
		    processAndDisplayLog(response.responseText);
                }
            });
	}
	else {
            Ext.Ajax.request({
                url: getOozieBase() + 'job/' + coordJobId + "?show=log&type=action&scope="+actionsList,
                timeout: 300000,
                success: function(response, request) {
		    processAndDisplayLog(response.responseText);
                },
		failure: function() {
                    Ext.MessageBox.show({
                        title: 'Format Error',
                        msg: 'Action List format is wrong. Format should be similar to 1,3-4,7-40',
                        buttons: Ext.MessageBox.OK,
                        icon: Ext.MessageBox.ERROR
                    });
		}
            });
	}
    }
    function processAndDisplayLog(response)
    {
	var responseLength = response.length;
	var twentyFiveMB = 25*1024*1024;
	if(responseLength > twentyFiveMB) {
	    response = response.substring(responseLength-twentyFiveMB,responseLength)
	    response = response.substring(response.indexOf("\n")+1,responseLength);
	    jobLogArea.setRawValue(response);
	}
	else {
	    jobLogArea.setRawValue(response);
	}
    }
    var jobDetails = eval("(" + response.responseText + ")");
    var coordJobId = jobDetails["coordJobId"];
    var appName = jobDetails["coordJobName"];
    var jobActionStatus = new Ext.data.JsonStore({
        data: jobDetails["actions"],
        fields: ['id', 'name', 'type', 'createdConf', 'runConf', 'actionNumber', 'createdTime', 'externalId', 'lastModifiedTime', 'nominalTime', 'status', 'missingDependencies', 'externalStatus', 'trackerUri', 'consoleUrl', 'errorCode', 'errorMessage', 'actions', 'externalChildIDs']

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
            width: 200,
            value: jobDetails["coordJobName"]
        }, {
            fieldLabel: 'Status',
            editable: false,
            name: 'status',
            width: 200,
            value: jobDetails["status"]
        }, {
            fieldLabel: 'User',
            editable: false,
            name: 'user',
            width: 200,
            value: jobDetails["user"]
        }, {
            fieldLabel: 'Group',
            editable: false,
            name: 'group',
            width: 200,
            value: jobDetails["group"]
        }, {
            fieldLabel: 'Frequency',
            editable: false,
            name: 'frequency',
            width: 200,
            value: jobDetails["frequency"]
        }, {
            fieldLabel: 'Unit',
            editable: false,
            name: 'timeUnit',
            width: 200,
            value: jobDetails["timeUnit"]
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
        }, ]
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
                    url: getOozieBase() + 'job/' + coordJobId + "?timezone=" + getTimeZone(),
                    success: function(response, request) {
                        jobDetails = eval("(" + response.responseText + ")");
                        jobActionStatus.loadData(jobDetails["actions"]);
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
            id: 'id',
            header: "Action Id",
            width: 240,
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
            width: 160,
            sortable: true,
            dataIndex: 'createdTime'
        }, {
            header: "Nominal Time",
            width: 160,
            sortable: true,
            dataIndex: 'nominalTime'
        }, {
            header: "Last Mod Time",
            width: 170,
            sortable: true,
            dataIndex: 'LastModifiedTime'
        }, ],
        stripeRows: true,
        // autoHeight: true,
        autoScroll: true,
        frame: true,
        height: 400,
        width: 1000,
        title: 'Actions',
        listeners: {
            cellclick: {
                fn: showWorkflowPopup
            }
        }

    });
    function showWorkflowPopup(thisGrid, rowIndex, cellIndex, e) {
        var jobContextMenu = new Ext.menu.Menu('taskContext');
        var actionStatus = thisGrid.store.data.items[rowIndex].data;
        var workflowId = actionStatus["externalId"];
        jobDetailsGridWindow(workflowId);
    }
    // alert("Coordinator PopUP 4 inside coordDetailsPopup ");
    function showCoordActionContextMenu(thisGrid, rowIndex, cellIndex, e) {
        var jobContextMenu = new Ext.menu.Menu('taskContext');
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

                }, ]
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
                width: 1010,
                height: 430,
                autoScroll: true,
                value: jobDetails["conf"]
            })
	},{
           title: 'Coord Job Log',
	   items: [jobLogArea, actionsTextBox, getLogButton],
           tbar: [ {
                text: "&nbsp;&nbsp;&nbsp;",
                icon: 'ext-2.2/resources/images/default/grid/refresh.gif'
                 }]
	   }]
});

    jobDetailsTab.addListener("tabchange", function(panel, selectedTab) {
        if (selectedTab.title == "Coord Job Info") {
            coord_jobs_grid.setVisible(true);
            return;
        }
        if (selectedTab.title == 'Coord Job Log') {
	    actionsTextBox.setValue('Enter the action list here');
        }
        else if (selectedTab.title == 'Coord Job Definition') {
            fetchDefinition(coordJobId);
        }
        coord_jobs_grid.setVisible(false);
    });

    var win = new Ext.Window({
        title: 'Job (Name: ' + appName + '/coordJobId: ' + coordJobId + ')',
        closable: true,
        width: 1020,
        autoHeight: true,
        plain: true,
        items: [jobDetailsTab, coord_jobs_grid]
    });
    win.setPosition(10, 10);
    win.show();
}

function bundleJobDetailsPopup(response, request) {
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
            width: 200,
            value: jobDetails["bundleJobName"]
        }, {
            fieldLabel: 'Status',
            editable: false,
            name: 'status',
            width: 200,
            value: jobDetails["status"]
        }, {
            fieldLabel: 'Kickoff Time',
            editable: false,
            name: 'kickoffTime',
            width: 200,
            value: jobDetails["kickoffTime"]
        }, {
            fieldLabel: 'Created Time',
            editable: false,
            name: 'createdTime',
            width: 200,
            value: jobDetails["createdTime"]
        }, {
            fieldLabel: 'User',
            editable: false,
            name: 'user',
            width: 170,
            value: jobDetails["user"]
        }, {
            fieldLabel: 'group',
            editable: false,
            name: 'group',
            width: 170,
            value: jobDetails["group"]
        }, ]
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
        }, ],
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

    var jobDetailsTab = new Ext.TabPanel({
        activeTab: 0,
        autoHeight: true,
        deferredRender: false,
        items: [ {
            title: 'Bundle Job Info',
            items: fs
        }]
    });

    jobDetailsTab.addListener("tabchange", function(panel, selectedTab) {
        if (selectedTab.title == "Bundle Job Info") {
            coord_jobs_grid.setVisible(true);
            return;
        }
        //if (selectedTab.title == 'Job Log') {
        //    fetchLogs(workflowId);
        //}
        //else if (selectedTab.title == 'Job Definition') {
        //    fetchDefinition(workflowId);
        //}
        coord_jobs_grid.setVisible(false);
    });

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
        url: getOozieBase() + 'job/' + coordJobId + "?timezone=" + getTimeZone(),
        success: coordJobDetailsPopup
        // success: alert("succeeded " + response),
        // failure: alert("Coordinator PopUP did not work" + coordJobId),
    });
}

function bundleJobDetailsGridWindow(bundleJobId) {
    Ext.Ajax.request({
        url: getOozieBase() + 'job/' + bundleJobId + "?timezone=" + getTimeZone(),
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
        }, ],
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
        filter: "",
        timezone: getTimeZone()
    },
    idProperty: 'coordJobId',
    totalProperty: 'total',
    autoLoad: true,
    root: 'coordinatorjobs',
    fields: ['coordJobId', 'coordJobName', 'status', 'user', 'group', 'frequency', 'timeUnit', {name: 'startTime', sortType: Ext.data.SortTypes.asDate}, {name: 'nextMaterializedTime', sortType: Ext.data.SortTypes.asDate}],
    proxy: new Ext.data.HttpProxy({
        url: getOozieBase() + 'jobs'
    })
});
coord_jobs_store.proxy.conn.method = "GET";

/* 
 * create the wf jobs store
 */
var jobs_store = new Ext.data.JsonStore({
    baseParams: {
        filter: "",
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
jobs_store.proxy.conn.method = "GET";

/* 
 * create the bundle jobs store
 */
var bundle_jobs_store = new Ext.data.JsonStore({

	baseParams: {
        jobtype: "bundle",
        filter: "",
        timezone: getTimeZone()
    },
    idProperty: 'bundleJobId',
    totalProperty: 'total',
    autoLoad: true,
    root: 'bundlejobs',
    fields: ['bundleJobId', 'bundleJobName', 'bundleJobPath', 'conf', 'status', {name: 'kickoffTime', sortType: Ext.data.SortTypes.asDate}, {name: 'startTime', sortType: Ext.data.SortTypes.asDate}, {name: 'endTime', sortType: Ext.data.SortTypes.asDate}, {name: 'pauseTime', sortType: Ext.data.SortTypes.asDate}, {name: 'createdTime', sortType: Ext.data.SortTypes.asDate}, 'user', 'group', 'bundleCoordJobs'],
    proxy: new Ext.data.HttpProxy({
        url: getOozieBase() + 'jobs'
    })
});
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
    text: 'status=KILLED',
    handler: function() {
        jobs_store.baseParams.filter = this.text;
        jobs_store.baseParams.timezone = getTimeZone();
        jobs_store.reload();
    }

});

var refreshActiveJobsAction = new Ext.Action({
    text: 'Active Jobs',
    handler: function() {
        jobs_store.baseParams.filter = 'status=RUNNING';
        jobs_store.baseParams.timezone = getTimeZone();
        jobs_store.reload();
    }
});

var refreshAllJobsAction = new Ext.Action({
    text: 'All Jobs',
    handler: function() {
        jobs_store.baseParams.filter = '';
        jobs_store.baseParams.timezone = getTimeZone();
        jobs_store.reload();
    }

});

var refreshDoneJobsAction = new Ext.Action({
    text: 'Done Jobs',
    handler: function() {
        jobs_store.baseParams.filter = 'status=SUCCEEDED;status=KILLED';
        jobs_store.baseParams.timezone = getTimeZone();
        jobs_store.reload();
    }
});

var refreshCoordCustomJobsAction = new Ext.Action({
    text: 'status=KILLED',
    handler: function() {
        coord_jobs_store.baseParams.filter = this.text;
        coord_jobs_store.baseParams.timezone = getTimeZone();
        coord_jobs_store.reload();
    }
});


var refreshCoordActiveJobsAction = new Ext.Action({
    text: 'Active Jobs',
    handler: function() {
        coord_jobs_store.baseParams.filter = 'status=RUNNING';
        coord_jobs_store.baseParams.timezone = getTimeZone();
        coord_jobs_store.reload();
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
    text: 'All Jobs',
    handler: function() {
        coord_jobs_store.baseParams.filter = '';
        coord_jobs_store.baseParams.timezone = getTimeZone();
        coord_jobs_store.reload();
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
    text: 'Done Jobs',
    handler: function() {
        coord_jobs_store.baseParams.filter = 'status=SUCCEEDED;status=KILLED';
        coord_jobs_store.baseParams.timezone = getTimeZone();
        coord_jobs_store.reload();
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
    text: 'Active Jobs',
    handler: function() {
        bundle_jobs_store.baseParams.filter = 'status=RUNNING';
        bundle_jobs_store.baseParams.timezone = getTimeZone();
        bundle_jobs_store.reload();
    }
});

var refreshBundleAllJobsAction = new Ext.Action({
    text: 'All Jobs',
    handler: function() {
		bundle_jobs_store.baseParams.filter = '';
                bundle_jobs_store.baseParams.timezone = getTimeZone();
		bundle_jobs_store.reload();
    }
});

var refreshBundleDoneJobsAction = new Ext.Action({
    text: 'Done Jobs',
    handler: function() {
	bundle_jobs_store.baseParams.filter = 'status=SUCCEEDED;status=KILLED';
        bundle_jobs_store.baseParams.timezone = getTimeZone();
        bundle_jobs_store.reload();
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
                refreshCustomJobsAction.setText(text);
                jobs_store.baseParams.filter = text;
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
                refreshCoordCustomJobsAction.setText(text);
                coord_jobs_store.baseParams.filter = text;
                coord_jobs_store.baseParams.timezone = getTimeZone();
                coord_jobs_store.reload();
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

        })
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
                serverVersion.setText("<font color='800000' size='2>[" + ret['buildVersion'] + "]</font>");
            }
        })
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
                var timers = treeNodeFromJson(jsonData["timers"], "timers");
                timers.expanded = false;
                var samplers = treeNodeFromJson(jsonData["samplers"], "samplers");
                samplers.expanded = false;
                var counters = treeNodeFromJson(jsonData["counters"], "counters");
                counters.expanded = false;
                var variables = treeNodeFromJson(jsonData["variables"], "variables");
                variables.expanded = false;
                while (treeRoot.hasChildNodes()) {
                    var child = treeRoot.firstChild;
                    treeRoot.removeChild(child);
                }
                treeRoot.appendChild(samplers);
                treeRoot.appendChild(counters);
                treeRoot.appendChild(timers);
                treeRoot.appendChild(variables);
                treeRoot.expand(false, true);
            }

        });
    }

});

var viewCoordJobs = new Ext.Action({
    text: 'All Jobs',
    handler: function() {
        /*
         Ext.Ajax.request( {
         url: getOozieBase() + 'jobs/?jobtype=coord',
         success: function(response, request) {
         var configData = getConfigObject(response.responseText);
         coord_job_store.loadData(configData);
         },
         });
         */
        // coord_jobs_store.baseParams.filter = 'jobtype=coord';
        coord_jobs_store.baseParams.timezone = getTimeZone();
        coord_jobs_store.reload();
    }
});

var viewBundleJobs = new Ext.Action({
    text: 'All Jobs',
    handler: function() {
        bundle_jobs_store.baseParams.timezone = getTimeZone();
        bundle_jobs_store.reload();
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

var treeRoot = new Ext.tree.TreeNode({
    text: "Instrumentation",
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
timeZones_store.proxy.conn.method = "GET";

function showCoordJobContextMenu(thisGrid, rowIndex, cellIndex, e) {
    var jobContextMenu = new Ext.menu.Menu('taskContext');
    var coordJobId = thisGrid.store.data.items[rowIndex].data.coordJobId;
    coordJobDetailsGridWindow(coordJobId);
}

function initConsole() {
    function showJobContextMenu(thisGrid, rowIndex, cellIndex, e) {
        var jobContextMenu = new Ext.menu.Menu('taskContext');
        var workflowId = thisGrid.store.data.items[rowIndex].data.id;
        jobDetailsGridWindow(workflowId);
    }

    function showBundleJobContextMenu(thisGrid, rowIndex, cellIndex, e) {
        var jobContextMenu = new Ext.menu.Menu('taskContext');
        var bundleJobId = thisGrid.store.data.items[rowIndex].data.bundleJobId;
        bundleJobDetailsGridWindow(bundleJobId);
    }

    var jobs_grid = new Ext.grid.GridPanel({
        store: jobs_store,
        loadMask: true,
        columns: [new Ext.grid.RowNumberer(), {
            id: 'id',
            header: "Job Id",
            width: 190,
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
        }, ],

        stripeRows: true,
        autoScroll: true,
        frame: false,
        width: 1050,
        tbar: [ {
            text: "&nbsp;&nbsp;&nbsp;",
            icon: 'ext-2.2/resources/images/default/grid/refresh.gif',
            handler: function() {
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
        }, ],
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
    var resultArea = new Ext.tree.TreePanel({
        autoScroll: true,
        useArrows: true,
        height: 300,
        root: treeRoot,
        tbar: [viewInstrumentation, {
            xtype: 'tbfill'
        }, checkStatus, serverVersion],
        title: 'Instrumentation'

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
            header: "frequency",
            width: 70,
            sortable: true,
            dataIndex: 'frequency'
        }, {
            header: "unit",
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
        },],

        stripeRows: true,
        autoScroll: true,
        useArrows: true,
        height: 300,
        tbar: [ {
            text: "&nbsp;&nbsp;&nbsp;",
            icon: 'ext-2.2/resources/images/default/grid/refresh.gif',
            handler: function() {
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
        },],

        stripeRows: true,
        autoScroll: true,
        useArrows: true,
        height: 300,
        tbar: [ {
            text: "&nbsp;&nbsp;&nbsp;",
            icon: 'ext-2.2/resources/images/default/grid/refresh.gif',
            handler: function() {
                bundle_jobs_store.baseParams.timezone = getTimeZone();
                bundle_jobs_store.reload();
            }
        }, refreshBundleAllJobsAction, refreshBundleActiveJobsAction, refreshBundleDoneJobsAction,
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
        }]
    });
    var tabs = new Ext.TabPanel({
        renderTo: 'oozie-console',
        height: 500,
        width: 1050,
        title: "Oozie Web Console"

    });
    tabs.add(jobs_grid);
    tabs.add(coordJobArea);
    tabs.add(bundleJobArea);
    tabs.add(adminGrid);
    tabs.add(resultArea);
    tabs.add(settingsArea);
    tabs.setActiveTab(jobs_grid);
    checkStatus.execute();
    viewConfig.execute();
    serverVersion.execute();
    viewInstrumentation.execute();
    // viewCoordJobs.execute();
    var jobId = getReqParam("job");
    if (jobId != "") {
        jobDetailsGridWindow(jobId);
    }
}
// now the on ready function
Ext.onReady(function() {
    getSupportedVersions.execute();
});
