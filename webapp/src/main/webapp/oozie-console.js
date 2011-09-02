
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//so it works from remote browsers, "http://localhost:8080";
var oozie_host = "";
var flattenedObject;

function getOozieClientVersion() {
    return 0;
}

function getOozieVersionsUrl() {
    var ctxtStr = location.pathname;
    return oozie_host + ctxtStr + "versions";
}

function getOozieBase() {
    var ctxtStr = location.pathname;
    return oozie_host + ctxtStr.replace(/[-]*console/, "") + "v" + getOozieClientVersion() + "/";
}

function getReqParam( name ) {
  name = name.replace(/[\[]/,"\\\[").replace(/[\]]/,"\\\]");
  var regexS = "[\\?&]"+name+"=([^&#]*)";
  var regex = new RegExp( regexS );
  var results = regex.exec( window.location.href );
  if( results == null ){
    return "";
  }
  else{
    return results[1];
  }
}

// renderer functions
function valueRenderer(value, metadata, record, row, col, store) {
    if (value.length > 60) {
        return value.substring(0, 60) + " ...";
    } else {
        return value;
    }
}

function dateTime(value, metadata, record, row, col, store) {
    return value;
}

function checkUrl(value, metadata, record, row, col, store) {
    if (value != null) {
        return "Y";
    } else {
        return "N";
    }
}

// Makes a tree node from an XML
function treeNodeFromXml(XmlEl) {
    var t = ((XmlEl.nodeType == 3) ? XmlEl.nodeValue: XmlEl.tagName);
    if (t.replace(/\s/g, '').length == 0) {
        return null;
    }
    var result = new Ext.tree.TreeNode( {
        text: t
    });
    //  For Elements, process attributes and children
    if (XmlEl.nodeType == 1) {
        Ext.each(XmlEl.attributes, function(a) {
            result.appendChild(new Ext.tree.TreeNode( {
                text: a.nodeName
            })).appendChild(new Ext.tree.TreeNode( {
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
    var result = new Ext.tree.TreeNode( {
        text: rootText,
    });
    //  For Elements, process attributes and children
    if (typeof json === 'object') {
        for (var i in json) {
            if(json[i]){
                if (typeof json[i] == 'object') {
                    var c;
                    if(json[i]['group']) {
                        c = treeNodeFromJson(json[i]['data'], json[i]['group']);
                    }else {
                        c = treeNodeFromJson(json[i], json[i]['name']);
                    }
                    if (c)
                        result.appendChild(c);
                }
                else if (typeof json[i] != 'function') {
                        result.appendChild(new Ext.tree.TreeNode( {
                            text: i + " -> " + json[i],
                        }));
                }
            }
            else {
                result.appendChild(new Ext.tree.TreeNode( {
                    text: i + " -> " + json[i],
                }));
            }
        }
    } else {
        result.appendChild(new Ext.tree.TreeNode( {
            text: json,
        }));
    }
    return result;
}

// Common stuff to get a paging toolbar for a data store
function getPagingBar(dataStore) {
    var pagingBar = new Ext.PagingToolbar( {
        pageSize: 50,
        store: dataStore,
        displayInfo: true,
        displayMsg: '{0} - {1} of {2}',
        emptyMsg: "No data",

    });
    pagingBar.paramNames = {
        start: 'offset',
        limit: 'len'
    };
    return pagingBar;
}

// stuff to show details of a job
function jobDetailsPopup(response, request) {
    var jobDefinitionArea = new Ext.form.TextArea( {
        fieldLabel: 'Definition',
        editable: false,
        name: 'definition',
        width: 1005,
        height: 400,
        autoScroll: true,
        emptyText: "Loading..."
    });
    var jobLogArea = new Ext.form.TextArea( {
        fieldLabel: 'Logs',
        editable: false,
        name: 'logs',
        width: 1010,
        height: 400,
        autoScroll: true,
        emptyText: "Loading..."
    });
    function fetchDefinition(workflowId) {
        Ext.Ajax.request( {
            url: getOozieBase() + 'job/' + workflowId + "?show=definition",
            success: function(response, request) {
                jobDefinitionArea.setRawValue(response.responseText);
            },

        });
    }
    function fetchLogs(workflowId) {
        Ext.Ajax.request( {
            url: getOozieBase() + 'job/' + workflowId + "?show=log",
            success: function(response, request) {
                jobLogArea.setRawValue(response.responseText);
            },

        });
    }
    var jobDetails = eval("(" + response.responseText + ")");
    var workflowId = jobDetails["id"];
    var appName = jobDetails["appName"];
    var jobActionStatus = new Ext.data.JsonStore( {
        data: jobDetails["actions"],
        fields: ['id', 'name', 'type', 'startTime', 'retries', 'consoleUrl', 'endTime', 'externalId', 'status', 'trackerUri', 'workflowId', 'errorCode', 'errorMessage', 'conf', 'transition', 'externalStatus'],

    });
    var formFieldSet = new Ext.form.FieldSet( {
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
    var fs = new Ext.FormPanel( {
        frame: true,
        labelAlign: 'right',
        labelWidth: 85,
        width: 1010,
        items: [formFieldSet],
        tbar: [ {
            text: "&nbsp;&nbsp;&nbsp;",
            icon: 'ext-2/resources/images/default/grid/refresh.gif',
            handler: function() {
                Ext.Ajax.request( {
                    url: getOozieBase() + 'job/' + workflowId,
                    success: function(response, request) {
                        jobDetails = eval("(" + response.responseText + ")");
                        jobActionStatus.loadData(jobDetails["actions"]);
                        fs.getForm().setValues(jobDetails);
                    },

                });
            }
        }],

    });
    var jobs_grid = new Ext.grid.GridPanel( {
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
        autoHeight: true,
        autoScroll: true,
        frame: false,
        width: 1000,
        title: 'Actions',
        listeners: {
            cellclick: {
                fn: showActionContextMenu
            }
        },

    });
    function showActionContextMenu(thisGrid, rowIndex, cellIndex, e) {
        var jobContextMenu = new Ext.menu.Menu('taskContext');
        var actionStatus = thisGrid.store.data.items[rowIndex].data;
        actionDetailsGridWindow(actionStatus);
        function actionDetailsGridWindow(actionStatus) {
            var formFieldSet = new Ext.form.FieldSet( {
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
                    }, new Ext.form.TriggerField( {
                    fieldLabel: 'Console URL',
                    editable: false,
                    name: 'consoleUrl',
                    width: 400,
                    value: actionStatus["consoleUrl"],
                    triggerClass: 'x-form-search-trigger',
                    onTriggerClick: function() {
                        window.open(actionStatus["consoleUrl"]);
                    },

                }), {
                    fieldLabel: 'Tracker URI',
                    editable: false,
                    name: 'trackerUri',
                    width: 400,
                    value: actionStatus["trackerUri"],

                }, ]
                });
            var detail = new Ext.FormPanel( {
                frame: true,
                labelAlign: 'right',
                labelWidth: 85,
                width: 540,
                items: [formFieldSet]
                });
            var win = new Ext.Window( {
                title: 'Action (Name: ' + actionStatus["name"] + '/JobId: ' + workflowId + ')',
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
        }
    }
    var jobDetailsTab = new Ext.TabPanel( {
        activeTab: 0,
        autoHeight: true,
        deferredRender: false,
        items: [ {
            title: 'Job Info',
            items: fs,

        }, {
            title: 'Job Definition',
            items: jobDefinitionArea,

        }, {
            title: 'Job Configuration',
            items: new Ext.form.TextArea( {
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
                icon: 'ext-2/resources/images/default/grid/refresh.gif',
                handler: function() {
                    fetchLogs(workflowId);
                }
            }],

        }]
        });
    jobDetailsTab.addListener("tabchange", function(panel, selectedTab) {
        if (selectedTab.title == "Job Info") {
            jobs_grid.setVisible(true);
            return;
        }
        if (selectedTab.title == 'Job Log') {
            fetchLogs(workflowId);
        } else if (selectedTab.title == 'Job Definition') {
            fetchDefinition(workflowId);
        }
        jobs_grid.setVisible(false);
    });
    var win = new Ext.Window( {
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

function jobDetailsGridWindow(workflowId) {
    Ext.Ajax.request( {
        url: getOozieBase() + 'job/' + workflowId,
        success: jobDetailsPopup,

    });
}

function showConfigurationInWindow(dataObject, windowTitle) {
    var configGridData = new Ext.data.JsonStore( {
        data: dataObject,
        root: 'elements',
        fields: ['name', 'value'],

    });
    var configGrid = new Ext.grid.GridPanel( {
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
        width: 600,

    });
    var win = new Ext.Window( {
        title: windowTitle,
        closable: true,
        autoWidth: true,
        autoHeight: true,
        plain: true,
        items: [configGrid]
        });
    win.show();
}
// Stores
// create the data store
var jobs_store = new Ext.data.JsonStore( {
    baseParams: {
        filter: ""
    },
    idProperty: 'id',
    totalProperty: 'total',
    autoLoad: true,
    root: 'workflows',
    fields: ['appPath', 'appName', 'id', 'conf', 'status', 'createdTime', 'startTime', 'lastModTime', 'endTime', 'user', 'group', 'run', 'actions'],
    proxy: new Ext.data.HttpProxy( {
        url: getOozieBase() + 'jobs',

    })
    });
jobs_store.proxy.conn.method = "GET";
var configGridData = new Ext.data.JsonStore( {
    data: {
        elements: []
        },
    root: 'elements',
    fields: ['name', 'value', 'ovalue'],

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
        j ++ ;
    }
    return fo;
}
// All the actions
var refreshCustomJobsAction = new Ext.Action( {
    text: 'status=KILLED',
    handler: function() {
        jobs_store.baseParams.filter = this.text;
        jobs_store.reload();
    },

});
var refreshActiveJobsAction = new Ext.Action( {
    text: 'Active Jobs',
    handler: function() {
        jobs_store.baseParams.filter = 'status=RUNNING';
        jobs_store.reload();
    },

});
var refreshAllJobsAction = new Ext.Action( {
    text: 'All Jobs',
    handler: function() {
        jobs_store.baseParams.filter = '';
        jobs_store.reload();
    },

});
var refreshDoneJobsAction = new Ext.Action( {
    text: 'Done Jobs',
    handler: function() {
        jobs_store.baseParams.filter = 'status=SUCCEEDED;status=KILLED';
        jobs_store.reload();
    },
});

var helpFilterAction = new Ext.Action( {
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

var changeFilterAction = new Ext.Action( {
    text: 'Custom Filter',
    handler: function() {
        Ext.Msg.prompt('Filter Criteria', 'Filter text:', function(btn, text) {
            if (btn == 'ok' && text) {
                refreshCustomJobsAction.setText(text);
                jobs_store.baseParams.filter = text;
                jobs_store.reload();
            }
        });
    }
});
var getSupportedVersions = new Ext.Action( {
    text: 'Checking server for supported versions...',
    handler: function() {
        Ext.Ajax.request( {
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
            },

        })
        },

});
var checkStatus = new Ext.Action( {
    text: 'Status - Unknown',
    handler: function() {
        Ext.Ajax.request( {
            url: getOozieBase() + 'admin/status',
            success: function(response, request) {
                var status = eval("(" + response.responseText + ")");
                if (status.safeMode) {
                    checkStatus.setText("<font color='700000' size='2> Safe Mode - ON </font>");
                } else {
                    checkStatus.setText("<font color='007000' size='2> Status - Normal</font>");
                }
            },

        });
    },

});
var viewConfig = new Ext.Action( {
    text: 'Configuration',
    handler: function() {
        Ext.Ajax.request( {
            url: getOozieBase() + 'admin/configuration',
            success: function(response, request) {
                var configData = getConfigObject(response.responseText);
                configGridData.loadData(configData);
            },

        });
    },

});
var viewInstrumentation = new Ext.Action( {
    text: "&nbsp;&nbsp;&nbsp;",
    icon: 'ext-2/resources/images/default/grid/refresh.gif',
    handler: function() {
        Ext.Ajax.request( {
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
            },

        });
    },

});
var viewSystemDetails = new Ext.Action( {
    text: 'Java System Props',
    handler: function() {
        Ext.Ajax.request( {
            url: getOozieBase() + 'admin/java-sys-properties',
            success: function(response, request) {
                var configData = getConfigObject(response.responseText);
                configGridData.loadData(configData);
            },

        });
    },

});
var viewOSDetails = new Ext.Action( {
    text: 'OS Env',
    handler: function() {
        Ext.Ajax.request( {
            url: getOozieBase() + 'admin/os-env',
            success: function(response, request) {
                var configData = getConfigObject(response.responseText);
                configGridData.loadData(configData);
            },

        });
    },

});
var treeRoot = new Ext.tree.TreeNode( {
    text: "Instrumentation",
    expanded: true,

});
function initConsole() {
    function showJobContextMenu(thisGrid, rowIndex, cellIndex, e) {
        var jobContextMenu = new Ext.menu.Menu('taskContext');
        var workflowId = thisGrid.store.data.items[rowIndex].data.id;
        jobDetailsGridWindow(workflowId);
    }
    var jobs_grid = new Ext.grid.GridPanel( {
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
            icon: 'ext-2/resources/images/default/grid/refresh.gif',
            handler: function() {
                jobs_store.reload();
            }
        }, refreshAllJobsAction, refreshActiveJobsAction, refreshDoneJobsAction, {
            text: 'Custom Filter',
            menu: [refreshCustomJobsAction, changeFilterAction, helpFilterAction ]
            }, {
            xtype: 'tbfill'
        }, checkStatus, ],
        title: 'Workflow Jobs',
        bbar: getPagingBar(jobs_store),
        listeners: {
            cellclick: {
                fn: showJobContextMenu
            }
        },

    });
    var expander = new Ext.grid.RowExpander( {
        tpl: new Ext.Template('<br><p><b>Name:</b> {name}</p>', '<p><b>Value:</b> {value}</p>')
        });
    var adminGrid = new Ext.grid.GridPanel( {
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
        }, checkStatus],
        viewConfig: {
            forceFit: true,

        },
        plugins: expander,
        collapsible: true,
        animCollapse: false,
        title: "System Info"
    });
    var resultArea = new Ext.tree.TreePanel( {
        autoScroll: true,
        useArrows: true,
        height: 300,
        root: treeRoot,
        tbar: [viewInstrumentation, {
            xtype: 'tbfill'
        }, checkStatus],
        title: 'Instrumentation',

    });
    var tabs = new Ext.TabPanel( {
        renderTo: 'oozie-console',
        height: 500,
        width: 1050,
        title: "Oozie Web Console",

    });
    tabs.add(jobs_grid);
    tabs.add(adminGrid);
    tabs.add(resultArea);
    tabs.setActiveTab(jobs_grid);
    checkStatus.execute();
    viewConfig.execute();
    viewInstrumentation.execute();
    var jobId = getReqParam("job");
    if( jobId != "" ){
        jobDetailsGridWindow(jobId);
    }
}
// now the on ready function
Ext.onReady(function() {
    getSupportedVersions.execute();
});
