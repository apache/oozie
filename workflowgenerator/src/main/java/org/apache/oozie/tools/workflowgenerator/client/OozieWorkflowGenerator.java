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

package org.apache.oozie.tools.workflowgenerator.client;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;

import org.apache.oozie.tools.workflowgenerator.client.OozieDiagramController;
import org.apache.oozie.tools.workflowgenerator.client.property.PropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.property.control.WrkflowPropertyTable;
import org.apache.oozie.tools.workflowgenerator.client.widget.NodeWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.EmailActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.FSActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.JavaActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.MapReduceActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.PigActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.PipesActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.SSHActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.ShellActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.StreamingActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.action.SubWFActionWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.control.DecisionNodeWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.control.EndNodeWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.control.ForkNodeWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.control.JoinNodeWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.control.KillNodeWidget;
import org.apache.oozie.tools.workflowgenerator.client.widget.control.StartNodeWidget;

import com.allen_sauer.gwt.dnd.client.PickupDragController;
import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.dom.client.Style.Unit;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.logical.shared.SelectionEvent;
import com.google.gwt.event.logical.shared.SelectionHandler;
import com.google.gwt.user.client.Command;
import com.google.gwt.user.client.Random;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.ui.*;

/**
 * Main class of application
 * <p>
 * implements EntryPoint class which defines <code>onModuleLoad()</code>.
 */
public class OozieWorkflowGenerator implements EntryPoint {

    private static final int ZINDEX_FRONT_OF_GRID = 10;

    private PickupDragController dragController;
    private OozieDiagramController controller;
    private Panel propPanel;
    private List<NodeWidget> widgets;
    private StartNodeWidget start;
    private EndNodeWidget end;
    private KillNodeWidget kill;
    private WrkflowPropertyTable wrkflowtable;
    private NodeWidget node;
    private EnumMap<NodeType, Integer> nodeCount;

    enum NodeType {
        START, END, KILL, FORK, JOIN, DECISION, MAPREDUCE, PIG, FS, JAVA, PIPES, STREAMING, SHELL, SSH, EMAIL, SUBWF
    };

    /**
     * Return a set of created node widgets in a workflow design panel
     *
     * @return
     */
    public List<NodeWidget> getWidgets() {
        return this.widgets;
    }

    /**
     * Set and display the property table of a selected node widget
     *
     * @param table
     */
    public void setPropertyTable(PropertyTable table) {
        propPanel.clear();
        propPanel.add(table);
    }

    /**
     * Return an instance of current diagram controller
     *
     * @return
     */
    public OozieDiagramController getDiagramController() {
        return this.controller;
    }

    /**
     * Return the currently selected node widget
     *
     * @return
     */
    public NodeWidget getNodeWidget() {
        return this.node;
    }

    /**
     * Set the current node widget
     *
     * @param n node widget
     */
    protected void setNodeWidget(NodeWidget n) {
        this.node = n;
    }

    /**
     * onModuleLoad is the entry point method.
     */
    @SuppressWarnings("deprecation")
    public void onModuleLoad() {

        widgets = new ArrayList<NodeWidget>();
        nodeCount = new EnumMap<OozieWorkflowGenerator.NodeType, Integer>(NodeType.class);

        // start DiagramController (gwt-links library)
        controller = new OozieDiagramController(1200, 600);
        controller.showGrid(true); // Display a background grid

        // start PickUpDragContoller (gwt-Drag-and-Drop library)
        dragController = new PickupDragController(controller.getView(), true);

        // register the dragController in GWT-Links
        controller.registerDragController(dragController);

        // panel for Property Table
        propPanel = new AbsolutePanel();

        // stack Layout for left-side tree-view
        StackLayoutPanel stack = new StackLayoutPanel(Unit.EM);

        // create left tree-view panel
        stack.add(initNodeTree(), new HTML("Nodes"), 2);
        stack.add(initWrkflowTree(), new HTML("Workflow"), 2);

        initWidget();

        // Create a three-pane layout with splitters.
        SplitLayoutPanel p = new SplitLayoutPanel();

        Panel east = new AbsolutePanel();

        // Create a top panel under menu to hold button (e.g, generate xml)
        AbsolutePanel btnpanl = new AbsolutePanel();
        Button btn = createXMLButton();
        btnpanl.add(btn);

        p.addNorth(initMenu(), 30);
        p.addEast(east, 250);
        p.addSouth(propPanel, 300);
        p.addWest(stack, 150);
        p.addNorth(btnpanl, 30);
        ((OozieDiagramController) controller).setXmlPanel(east);
        p.add(controller.getView());

        // Attach the LayoutPanel to the RootLayoutPanel.
        RootLayoutPanel rp = RootLayoutPanel.get();
        rp.add(p);

    }

    /**
     * Create a "Generate XML" button
     *
     * @return
     */
    public Button createXMLButton() {
        Button btn = new Button("Generate XML");
        btn.addClickHandler(new ClickHandler() {

            @Override
            public void onClick(ClickEvent event) {
                ((OozieDiagramController) controller).generateXml();
            }
        });
        return btn;
    }

    /**
     * Initialize a default set of node widgets shown in a workflow design
     * panel, such as start, end, kill nodes
     */
    public void initWidget() {

        if (wrkflowtable == null) {
            wrkflowtable = new WrkflowPropertyTable(null);
            ((OozieDiagramController) controller).setWrkflowPropertyTable(wrkflowtable);
        }

        // display Start/End/Kill Nodes as default
        if (start == null) {
            start = new StartNodeWidget(this);
            controller.addWidget(start, 30, 100);
            dragController.makeDraggable(start);
            widgets.add(start);
        }

        if (end == null) {
            end = new EndNodeWidget(this);
            end.setName("End");
            controller.addWidget(end, 600, 100);
            dragController.makeDraggable(end);
            widgets.add(end);
        }

        if (kill == null) {
            kill = new KillNodeWidget(this);
            kill.setName("Kill");
            controller.addWidget(kill, 700, 100);
            dragController.makeDraggable(kill);
            widgets.add(kill);
        }
    }

    /**
     * Initialize workflow tree-view menu on left side
     *
     * @return
     */
    public Tree initWrkflowTree() {
        Tree t = new Tree();
        TreeItem actionTree = new TreeItem("Property");
        t.addItem(actionTree);
        t.addSelectionHandler(new SelectionHandler<TreeItem>() {

            @Override
            public void onSelection(SelectionEvent<TreeItem> event) {
                TreeItem item = event.getSelectedItem();
                String name = item.getText();
                if (name.equals("Property") && wrkflowtable != null) {
                    setPropertyTable(wrkflowtable);
                }
            }
        });

        return t;
    }

    /**
     * Initialize node tree-view menu on left side
     *
     * @return
     */
    public Tree initNodeTree() {

        Tree t = new Tree();

        // Action Node Tree
        TreeItem actionTree = new TreeItem("Action Node");
        TreeItem mrTree = new TreeItem("MapReduce");
        mrTree.addItem("Streaming");
        mrTree.addItem("Pipes");
        actionTree.addItem(mrTree);
        actionTree.addItem("Pig");
        actionTree.addItem("Java");
        actionTree.addItem("FS");
        actionTree.addItem("Subworkflow");
        actionTree.addItem("SSH");
        actionTree.addItem("Shell");
        actionTree.addItem("Email");

        // Control Node Tree
        TreeItem controlTree = new TreeItem("Control Node");
        controlTree.addItem("Start");
        controlTree.addItem("End");
        controlTree.addItem("Kill");
        controlTree.addItem("Fork/Join");
        controlTree.addItem("Decision");

        t.addItem(actionTree);
        t.addItem(controlTree);

        // Event Handler
        t.addSelectionHandler(new SelectionHandler<TreeItem>() {

            @Override
            public void onSelection(SelectionEvent<TreeItem> event) {
                TreeItem item = event.getSelectedItem();
                String name = item.getText();
                if (name.equals("MapReduce")) {
                    MapReduceActionWidget w = new MapReduceActionWidget(OozieWorkflowGenerator.this);
                    w.setName("MR_".concat(nodeCount.get(NodeType.MAPREDUCE) != null ? nodeCount
                            .get(NodeType.MAPREDUCE).toString() : "0"));
                    addWidget(w, 30 + Random.nextInt(30), 30 + Random.nextInt(30));
                }
                else if (name.equals("Streaming")) {
                    StreamingActionWidget w = new StreamingActionWidget(OozieWorkflowGenerator.this);
                    w.setName("Streaming_".concat(nodeCount.get(NodeType.STREAMING) != null ? nodeCount.get(
                            NodeType.STREAMING).toString() : "0"));
                    addWidget(w, 30 + Random.nextInt(30), 30 + Random.nextInt(30));
                }
                else if (name.equals("Pipes")) {
                    PipesActionWidget w = new PipesActionWidget(OozieWorkflowGenerator.this);
                    w.setName("Pipes_".concat(nodeCount.get(NodeType.PIPES) != null ? nodeCount.get(NodeType.PIPES)
                            .toString() : "0"));
                    addWidget(w, 30 + Random.nextInt(30), 30 + Random.nextInt(30));
                }
                else if (name.equals("Pig")) {
                    PigActionWidget w = new PigActionWidget(OozieWorkflowGenerator.this);
                    w.setName("Pig_".concat(nodeCount.get(NodeType.PIG) != null ? nodeCount.get(NodeType.PIG)
                            .toString() : "0"));
                    addWidget(w, 30 + Random.nextInt(30), 30 + Random.nextInt(30));
                }
                else if (name.equals("Java")) {
                    JavaActionWidget w = new JavaActionWidget(OozieWorkflowGenerator.this);
                    w.setName("Java_".concat(nodeCount.get(NodeType.JAVA) != null ? nodeCount.get(NodeType.JAVA)
                            .toString() : "0"));
                    addWidget(w, 30 + Random.nextInt(30), 30 + Random.nextInt(30));
                }
                else if (name.equals("FS")) {
                    FSActionWidget w = new FSActionWidget(OozieWorkflowGenerator.this);
                    w.setName("FS_".concat(nodeCount.get(NodeType.FS) != null ? nodeCount.get(NodeType.FS).toString()
                            : "0"));
                    addWidget(w, 30 + Random.nextInt(30), 30 + Random.nextInt(30));
                }
                else if (name.equals("SSH")) {
                    SSHActionWidget w = new SSHActionWidget(OozieWorkflowGenerator.this);
                    w.setName("SSH_".concat(nodeCount.get(NodeType.SSH) != null ? nodeCount.get(NodeType.SSH)
                            .toString() : "0"));
                    addWidget(w, 30 + Random.nextInt(30), 30 + Random.nextInt(30));

                }
                else if (name.equals("Email")) {
                    EmailActionWidget w = new EmailActionWidget(OozieWorkflowGenerator.this);
                    w.setName("Email_".concat(nodeCount.get(NodeType.EMAIL) != null ? nodeCount.get(NodeType.EMAIL)
                            .toString() : "0"));
                    addWidget(w, 30 + Random.nextInt(30), 30 + Random.nextInt(30));

                }
                else if (name.equals("Shell")) {
                    ShellActionWidget w = new ShellActionWidget(OozieWorkflowGenerator.this);
                    w.setName("Shell_".concat(nodeCount.get(NodeType.SHELL) != null ? nodeCount.get(NodeType.SHELL)
                            .toString() : "0"));
                    addWidget(w, 30 + Random.nextInt(30), 30 + Random.nextInt(30));

                }
                else if (name.equals("Subworkflow")) {
                    SubWFActionWidget w = new SubWFActionWidget(OozieWorkflowGenerator.this);
                    w.setName("SubWF_".concat(nodeCount.get(NodeType.SUBWF) != null ? nodeCount.get(NodeType.SUBWF)
                            .toString() : "0"));
                    addWidget(w, 30 + Random.nextInt(30), 30 + Random.nextInt(30));

                }
                else if (name.equals("Start")) {
                    if (start == null) {
                        StartNodeWidget w = new StartNodeWidget(OozieWorkflowGenerator.this);
                        start = w;
                        addWidget(w, 30 + Random.nextInt(30), 30 + Random.nextInt(30));
                    }
                }
                else if (name.equals("End")) {
                    if (end == null) {
                        EndNodeWidget w = new EndNodeWidget(OozieWorkflowGenerator.this);
                        w.setName("End");
                        end = w;
                        addWidget(w, 30 + Random.nextInt(30), 30 + Random.nextInt(30));
                    }
                }
                else if (name.equals("Kill")) {
                    if (kill == null) {
                        KillNodeWidget w = new KillNodeWidget(OozieWorkflowGenerator.this);
                        w.setName("Kill");
                        kill = w;
                        addWidget(w, 30 + Random.nextInt(30), 30 + Random.nextInt(30));
                    }
                }
                else if (name.equals("Fork/Join")) {
                    ForkNodeWidget fork = new ForkNodeWidget(OozieWorkflowGenerator.this);
                    fork.setName("Fork_".concat(nodeCount.get(NodeType.FORK) != null ? nodeCount.get(NodeType.FORK)
                            .toString() : "0"));
                    addWidget(fork, 30 + Random.nextInt(30), 30 + Random.nextInt(30));

                    JoinNodeWidget join = new JoinNodeWidget(OozieWorkflowGenerator.this);
                    join.setName("Join_".concat(nodeCount.get(NodeType.JOIN) != null ? nodeCount.get(NodeType.JOIN)
                            .toString() : "0"));
                    addWidget(join, 90 + Random.nextInt(30), 30 + Random.nextInt(30));

                }
                else if (name.equals("Decision")) {
                    DecisionNodeWidget w = new DecisionNodeWidget(OozieWorkflowGenerator.this);
                    w.setName("Decision_".concat(nodeCount.get(NodeType.DECISION) != null ? nodeCount.get(
                            NodeType.DECISION).toString() : "0"));
                    addWidget(w, 30 + Random.nextInt(30), 30 + Random.nextInt(30));
                }
            }
        });

        return t;
    }

    /**
     * Initialize menu panel on top
     *
     * @return
     */
    public MenuBar initMenu() {

        // Menu bar
        Command cmd = new Command() {
            public void execute() {
                Window.alert("To be implemented soon");
            }
        };

        Command mr_cmd = new Command() {
            public void execute() {
                initWidget();
                MapReduceActionWidget mr = new MapReduceActionWidget(OozieWorkflowGenerator.this);
                mr.setName("MR_0");
                addWidget(mr, 300, 100);
                ((OozieDiagramController) controller).addConnection(start, mr);
                ((OozieDiagramController) controller).addConnection(mr, end);
                mr.updateOnSelection();
            }
        };

        Command pig_cmd = new Command() {

            public void execute() {
                clear();
                initWidget();
                PigActionWidget pig = new PigActionWidget(OozieWorkflowGenerator.this);
                pig.setName("Pig_0");
                addWidget(pig, 300, 100);
                ((OozieDiagramController) controller).addConnection(start, pig);
                ((OozieDiagramController) controller).addConnection(pig, end);
                pig.updateOnSelection();
            }
        };

        Command java_cmd = new Command() {

            public void execute() {
                clear();
                initWidget();
                JavaActionWidget java = new JavaActionWidget(OozieWorkflowGenerator.this);
                java.setName("Java_0");
                addWidget(java, 300, 100);
                ((OozieDiagramController) controller).addConnection(start, java);
                ((OozieDiagramController) controller).addConnection(java, end);
                java.updateOnSelection();
            }
        };

        Command forkjoin_cmd = new Command() {

            public void execute() {
                clear();
                initWidget();
                ForkNodeWidget fork = new ForkNodeWidget(OozieWorkflowGenerator.this);
                fork.setName("Fork_0");
                addWidget(fork, 150, 100);
                ((OozieDiagramController) controller).addConnection(start, fork);

                MapReduceActionWidget mr = new MapReduceActionWidget(OozieWorkflowGenerator.this);
                mr.setName("MR_0");
                addWidget(mr, 300, 30);
                ((OozieDiagramController) controller).addMultiConnection(fork, mr);

                PigActionWidget pig = new PigActionWidget(OozieWorkflowGenerator.this);
                pig.setName("Pig_0");
                addWidget(pig, 300, 200);
                ((OozieDiagramController) controller).addMultiConnection(fork, pig);

                JoinNodeWidget join = new JoinNodeWidget(OozieWorkflowGenerator.this);
                join.setName("Join_0");
                addWidget(join, 450, 100);
                ((OozieDiagramController) controller).addConnection(mr, join);
                ((OozieDiagramController) controller).addConnection(pig, join);
                ((OozieDiagramController) controller).addConnection(join, end);

                fork.updateOnSelection();
                join.updateOnSelection();
                mr.updateOnSelection();
                pig.updateOnSelection();
            }
        };

        Command clear_cmd = new Command() {

            public void execute() {
                clear();
            }

        };

        MenuBar fileMenu = new MenuBar(true);
        fileMenu.setAutoOpen(true);
        fileMenu.setAnimationEnabled(true);
        fileMenu.addItem("New", cmd);
        fileMenu.addItem("Open", cmd);
        fileMenu.addItem("Load XML", cmd);
        fileMenu.addItem("Save", cmd);
        fileMenu.addItem("Save As..", cmd);
        fileMenu.addItem("Generate XML", cmd);
        fileMenu.addItem("Print", cmd);
        fileMenu.addItem("Quit", cmd);

        MenuBar editMenu = new MenuBar(true);
        editMenu.setAutoOpen(true);
        editMenu.setAnimationEnabled(true);
        editMenu.addItem("Undo", cmd);
        editMenu.addItem("Redo", cmd);
        editMenu.addItem("Copy", cmd);
        editMenu.addItem("Cut", cmd);
        editMenu.addItem("Paste", cmd);
        editMenu.addItem("Duplicate", cmd);
        editMenu.addItem("Delete", cmd);
        editMenu.addItem("Clear Diagram", clear_cmd);

        MenuBar examples = new MenuBar(true);
        examples.setAutoOpen(true);
        examples.setAnimationEnabled(true);
        examples.addItem("wrkflow with MR action", mr_cmd);
        examples.addItem("wrkflow with Pig action", pig_cmd);
        examples.addItem("wrkflow with Java action", java_cmd);
        examples.addItem("wrkflow with Fork/Join ", forkjoin_cmd);

        MenuBar helpMenu = new MenuBar(true);
        helpMenu.setAutoOpen(true);
        helpMenu.setAnimationEnabled(true);

        // TODO this should point to a workflowgenerator's maven site, however there is no maven site available. (Not even in
        // Workspace of the jenkins job at https://builds.apache.org/job/oozie-trunk-precommit-build/ws/workflowgenerator/target/)
        // where client, for example, has target/site/apidocs
        // The ideal place is somewhere under http://oozie.apache.org/docs/ once it is generated.
        Command openOozieTopPageComman = new Command() {
            @Override
            public void execute() {
                Window.open("http://oozie.apache.org/", "_blank", "");
            }
        };
        helpMenu.addItem("Documentation", openOozieTopPageComman);
        helpMenu.addItem("Online Help", openOozieTopPageComman);


        Command aboutCommand = new Command() {
            @Override
            public void execute() {
                // Dialogbox
                final DialogBox d = new DialogBox(false, true);
                d.setGlassEnabled(true);
                d.setText("About Oozie Workflow Generator");
                d.center();

                // Set this to workaround the grid z-index issue https://issues.apache.org/jira/browse/OOZIE-1081
                d.getElement().getStyle().setZIndex(ZINDEX_FRONT_OF_GRID);

                // About text
                VerticalPanel vpanel = new VerticalPanel();
                vpanel.setHorizontalAlignment(HasHorizontalAlignment.ALIGN_CENTER);
                vpanel.setSpacing(10);
                vpanel.setWidth("150");
                vpanel.add(new Label("Oozie Workflow Generator"));
                vpanel.add(new Label("Version 3.4.0-SNAPSHOT")); // TODO how to get a version number from pom?

                // OK button to close
                Button ok = new Button("OK");
                ok.addClickHandler(new ClickHandler(){
                    @Override
                    public void onClick(ClickEvent event) {
                        d.hide();
                    }

                });

                vpanel.add(ok);
                d.setWidget(vpanel);
                d.show();

            }
        };
        helpMenu.addItem("About", aboutCommand);

        MenuBar menu = new MenuBar();
        menu.addItem("File", fileMenu);
        menu.addItem("Edit", editMenu);
        menu.addItem("Example", examples);
        menu.addItem("Help", helpMenu);

        return menu;
    }

    /**
     * Add a new node widget
     *
     * @param w node widget
     * @param x x-coordinate
     * @param y y-coordinate
     */
    public void addWidget(NodeWidget w, int x, int y) {
        controller.addWidget(w, x, y);
        changeNodeCount(w, true);
        dragController.makeDraggable(w);
        widgets.add(w);
    }

    /**
     * Remove a node widget
     *
     * @param w node widget
     */
    public void removeWidget(NodeWidget w) {
        controller.deleteWidget(w);
        widgets.remove(w);
        if (w instanceof StartNodeWidget) {
            start = null;
        }
        else if (w instanceof EndNodeWidget) {
            end = null;
        }
        else if (w instanceof KillNodeWidget) {
            kill = null;
        }
        changeNodeCount(w, false);
    }

    /**
     * Increment/Decrement count of node widget type
     *
     * @param w nodeWidget
     * @param increment true: increment, false: decrement
     */
    protected void changeNodeCount(NodeWidget w, boolean increment) {
        int i = 0;
        if (w instanceof MapReduceActionWidget) {
            if (nodeCount.containsKey(NodeType.MAPREDUCE)) {
                i = nodeCount.get(NodeType.MAPREDUCE);
            }
            updateNodeCountMap(NodeType.MAPREDUCE, increment, i);
        }
        else if (w instanceof PigActionWidget) {
            if (nodeCount.containsKey(NodeType.PIG)) {
                i = nodeCount.get(NodeType.PIG);
            }
            updateNodeCountMap(NodeType.PIG, increment, i);
        }
        else if (w instanceof JavaActionWidget) {
            if (nodeCount.containsKey(NodeType.JAVA)) {
                i = nodeCount.get(NodeType.JAVA);
            }
            updateNodeCountMap(NodeType.JAVA, increment, i);
        }
        else if (w instanceof FSActionWidget) {
            if (nodeCount.containsKey(NodeType.FS)) {
                i = nodeCount.get(NodeType.FS);
            }
            updateNodeCountMap(NodeType.FS, increment, i);
        }
        else if (w instanceof PipesActionWidget) {
            if (nodeCount.containsKey(NodeType.PIPES)) {
                i = nodeCount.get(NodeType.PIPES);
            }
            updateNodeCountMap(NodeType.PIPES, increment, i);
        }
        else if (w instanceof StreamingActionWidget) {
            if (nodeCount.containsKey(NodeType.STREAMING)) {
                i = nodeCount.get(NodeType.STREAMING);
            }
            updateNodeCountMap(NodeType.STREAMING, increment, i);
        }
        else if (w instanceof ShellActionWidget) {
            if (nodeCount.containsKey(NodeType.SHELL)) {
                i = nodeCount.get(NodeType.SHELL);
            }
            updateNodeCountMap(NodeType.SHELL, increment, i);
        }
        else if (w instanceof SSHActionWidget) {
            if (nodeCount.containsKey(NodeType.SSH)) {
                i = nodeCount.get(NodeType.SSH);
            }
            updateNodeCountMap(NodeType.SSH, increment, i);
        }
        else if (w instanceof EmailActionWidget) {
            if (nodeCount.containsKey(NodeType.EMAIL)) {
                i = nodeCount.get(NodeType.EMAIL);
            }
            updateNodeCountMap(NodeType.EMAIL, increment, i);
        }
        else if (w instanceof SubWFActionWidget) {
            if (nodeCount.containsKey(NodeType.SUBWF)) {
                i = nodeCount.get(NodeType.SUBWF);
            }
            updateNodeCountMap(NodeType.SUBWF, increment, i);
        }
        else if (w instanceof StartNodeWidget) {
            if (nodeCount.containsKey(NodeType.START)) {
                i = nodeCount.get(NodeType.START);
            }
            updateNodeCountMap(NodeType.START, increment, i);
        }
        else if (w instanceof EndNodeWidget) {
            if (nodeCount.containsKey(NodeType.END)) {
                i = nodeCount.get(NodeType.END);
            }
            updateNodeCountMap(NodeType.END, increment, i);
        }
        else if (w instanceof KillNodeWidget) {
            if (nodeCount.containsKey(NodeType.KILL)) {
                i = nodeCount.get(NodeType.KILL);
            }
            updateNodeCountMap(NodeType.KILL, increment, i);
        }
        else if (w instanceof ForkNodeWidget) {
            if (nodeCount.containsKey(NodeType.FORK)) {
                i = nodeCount.get(NodeType.FORK);
            }
            updateNodeCountMap(NodeType.FORK, increment, i);
        }
        else if (w instanceof JoinNodeWidget) {
            if (nodeCount.containsKey(NodeType.JOIN)) {
                i = nodeCount.get(NodeType.JOIN);
            }
            updateNodeCountMap(NodeType.JOIN, increment, i);
        }
        else if (w instanceof DecisionNodeWidget) {
            if (nodeCount.containsKey(NodeType.DECISION)) {
                i = nodeCount.get(NodeType.DECISION);
            }
            updateNodeCountMap(NodeType.DECISION, increment, i);
        }
    }

    private void updateNodeCountMap(NodeType type, boolean increment, int i) {
        if (increment) {
            nodeCount.put(type, i + 1);
        }
        else {
            nodeCount.put(type, i - 1);
        }
    }

    /**
     * Remove all node widgets in a workflow design panel
     */
    public void clear() {
        controller.clearDiagram();
        widgets.clear();
        nodeCount.clear();
        start = null;
        end = null;
        kill = null;
    }
}
