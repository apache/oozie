package org.apache.oozie.command;

import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class TestSelectorTreeTraverser {
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private PurgeXCommand.JPAFunction<String, List<String>> noChildren = (String wId) -> new ArrayList<>();
    private PurgeXCommand.JPAFunction<List<String>, List<String>> noneSelector = (List<String> jobBeans) -> new ArrayList<>();
    private PurgeXCommand.JPAFunction<List<String>, List<String>> allSelector = (List<String> jobBeans) -> jobBeans;
    private PurgeXCommand.JPAFunction<String, List<String>> simpleTree = (String wId) -> {
        switch (wId) {
            case "A":
                return Arrays.asList("B", "C");
            case "B:":
            case "C":
                return new ArrayList<>();
        }
        return new ArrayList<>();
    };
    private PurgeXCommand.JPAFunction<String, List<String>> invalidTree = (String wId) -> {
        switch (wId) {
            case "A":
                return Arrays.asList("B", "C", "D");
            case "B:":
            case "C":
                return new ArrayList<>();
            case "D":
                return Collections.singletonList("A");
        }
        return new ArrayList<>();
    };

    @Test
    public void testSingleWorkflow() throws JPAExecutorException {
        PurgeXCommand.SelectorTreeTraverser<String, String> traverser = new PurgeXCommand.SelectorTreeTraverser<>("A", noChildren, noneSelector);
        List<String> descendants = traverser.findAllDescendantNodesIfSelectable();
        assertEquals(Collections.singletonList("A"), descendants);
    }

    @Test
    public void testOneDepthTreeNotSelectedChildren() throws JPAExecutorException {
        PurgeXCommand.SelectorTreeTraverser<String, String> traverser = new PurgeXCommand.SelectorTreeTraverser<>("A", simpleTree, noneSelector);
        List<String> descendants = traverser.findAllDescendantNodesIfSelectable();
        assertEquals(Collections.<String>emptyList(), descendants);
    }

    @Test
    public void testOneDepthTreeSelectedChildren() throws JPAExecutorException {
        PurgeXCommand.SelectorTreeTraverser<String, String> traverser = new PurgeXCommand.SelectorTreeTraverser<>("A", simpleTree, allSelector);
        List<String> descendants = traverser.findAllDescendantNodesIfSelectable();
        assertEquals(Arrays.asList("A", "B", "C"), descendants);
    }

    @Test
    public void testInvalidTree() throws JPAExecutorException {
        PurgeXCommand.SelectorTreeTraverser<String, String> traverser = new PurgeXCommand.SelectorTreeTraverser<>("A", invalidTree, allSelector);
        expectedException.expect(JPAExecutorException.class);
        traverser.findAllDescendantNodesIfSelectable();
    }

}