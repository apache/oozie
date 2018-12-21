package org.apache.oozie.command;

import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class TestWorkflowHierarchyTraverser {

    private PurgeXCommand.JPAFunction<String, List<String>> noChildren = (String wId) -> { return new ArrayList<String>(); };
    private PurgeXCommand.JPAFunction<List<String>, List<String>> noneSelector = (List<String> jobBeans) -> { return new ArrayList<String>(); };
    private PurgeXCommand.JPAFunction<List<String>, List<String>> allSelector = (List<String> jobBeans) -> { return jobBeans; };
    private PurgeXCommand.JPAFunction<String, List<String>> simpleTree = (String wId) -> {
        switch (wId) {
            case "A":
                return new ArrayList<String>() {{ add("B"); add("C"); }};
            case "B:":
            case "C":
                return new ArrayList<>();
        }
        return new ArrayList<String>();
    };

    @Test
    public void testSingleWorkflow() throws JPAExecutorException {
        PurgeXCommand.WorkflowHierarchyTraverser<String> traverser = new PurgeXCommand.WorkflowHierarchyTraverser("A", noChildren, noneSelector);
        List<String> descendants = traverser.findAllDescendantWorkflowsIfPurgeable();
        assertEquals(Collections.singletonList("A"), descendants);
    }

    @Test
    public void testOneDepthTreeNotSelectedChildren() throws JPAExecutorException {
        PurgeXCommand.WorkflowHierarchyTraverser traverser = new PurgeXCommand.WorkflowHierarchyTraverser("A", simpleTree, noneSelector);
        List<String> descendants = traverser.findAllDescendantWorkflowsIfPurgeable();
        assertEquals(Collections.<String>emptyList(), descendants);
    }

    @Test
    public void testOneDepthTreeSelectedChildren() throws JPAExecutorException {
        PurgeXCommand.WorkflowHierarchyTraverser traverser = new PurgeXCommand.WorkflowHierarchyTraverser("A", simpleTree, allSelector);
        List<String> descendants = traverser.findAllDescendantWorkflowsIfPurgeable();
        assertEquals(Arrays.asList("A", "B", "C"), descendants);
    }

}