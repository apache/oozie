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
package org.apache.oozie.util;

import org.apache.oozie.test.XTestCase;

import javax.servlet.jsp.el.ELException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class TestELEvaluator extends XTestCase {

    public static String functionA() {
        assertEquals("A", ELEvaluator.getCurrent().getVariable("a"));
        return "a";
    }

    public String functionB() {
        return "b";
    }

    private static String functionC() {
        return "c";
    }

    public static String functionD(String in1, String in2) {
        return in1 + "::" + in2;
    }

    public static String functionError() throws ELEvaluationException {
        throw new ELEvaluationException("m", null);
    }

    private static Method functionA;
    private static Method functionB;
    private static Method functionC;
    private static Method functionD;
    private static Method functionError;

    static {
        try {
            functionA = TestELEvaluator.class.getMethod("functionA");
            functionB = TestELEvaluator.class.getMethod("functionB");
            functionC = TestELEvaluator.class.getDeclaredMethod("functionC");
            functionD = TestELEvaluator.class.getDeclaredMethod("functionD",
                    String.class, String.class);
            functionError = TestELEvaluator.class.getDeclaredMethod("functionError");
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void testContextVars() throws Exception {
        ELEvaluator.Context support = new ELEvaluator.Context();
        assertNull(support.getVariable("a"));
        support.setVariable("a", "A");
        assertEquals("A", support.getVariable("a"));
        Map<String, Object> vars = new HashMap<String, Object>();
        vars.put("a", "AA");
        vars.put("b", "BB");
        support.setVariables(vars);
        assertEquals("AA", support.getVariable("a"));
        assertEquals("BB", support.getVariable("b"));
        try {
            support.resolveVariable("c");
            fail();
        }
        catch (ELException ex) {
            //nop
        }
    }


    public void testContextFunctions() throws Exception {
        ELEvaluator.Context support = new ELEvaluator.Context();
        support.addFunction("a", "a", functionA);

        try {
            support.addFunction("b", "b", functionB);
            fail();
        }
        catch (IllegalArgumentException ex) {
            //nop
        }

        try {
            support.addFunction("c", "c", functionC);
            fail();
        }
        catch (IllegalArgumentException ex) {
            //nop
        }

        assertEquals(functionA, support.resolveFunction("a", "a"));
    }

    public void testVars() throws Exception {
        ELEvaluator.Context support = new ELEvaluator.Context();
        support.setVariable("a", "A");
        ELEvaluator evaluator = new ELEvaluator(support);
        assertEquals("A", evaluator.getVariable("a"));
        assertEquals("A", evaluator.getContext().getVariable("a"));

        Map<String, Object> vars = new HashMap<String, Object>();
        vars.put("a", "AA");
        vars.put("b", "BB");
        support.setVariables(vars);
        assertEquals("AA", support.getVariable("a"));
        assertEquals("BB", support.getVariable("b"));
        try {
            support.resolveVariable("c");
            fail();
        }
        catch (ELException ex) {
            //nop
        }
    }

    public void testFunctions() throws Exception {
        ELEvaluator.Context support = new ELEvaluator.Context();
        support.addFunction("a", "a", functionA);
        ELEvaluator evaluator = new ELEvaluator(support);
        assertEquals(functionA, evaluator.getContext().resolveFunction("a", "a"));
    }

    public void testEval() throws Exception {
        ELEvaluator.Context support = new ELEvaluator.Context();
        support.setVariable("a", "A");
        support.addFunction("a", "a", functionA);
        ELEvaluator evaluator = new ELEvaluator(support);
        assertEquals("Aa", evaluator.evaluate("${a}${a:a()}", String.class));
    }

    public void testCurrent() throws Exception {
        ELEvaluator.Context support = new ELEvaluator.Context();
        support.setVariable("a", "A");
        support.addFunction("a", "a", functionA);
        ELEvaluator evaluator = new ELEvaluator(support);
        assertNull(ELEvaluator.getCurrent());
        assertEquals("a", evaluator.evaluate("${a:a()}", String.class));
        assertNull(ELEvaluator.getCurrent());
    }

    public void testFunctionELEvaluationError() throws Exception {
        try {
            ELEvaluator.Context support = new ELEvaluator.Context();
            support.addFunction("a", "a", functionError);
            ELEvaluator evaluator = new ELEvaluator(support);
            evaluator.evaluate("${a:a()}", String.class);
            fail();
        }
        catch (ELEvaluationException ex) {
            //nop
        }
        catch (ELException ex) {
            fail();
        }
    }

    public void testCheckForExistence() throws Exception {
        ELEvaluator.Context support = new ELEvaluator.Context();
        support.setVariable("a", "A");
        support.addFunction("a", "a", functionA);
        support.addFunction("a", "d", functionD);
        ELEvaluator evaluator = new ELEvaluator(support);
        assertNull(ELEvaluator.getCurrent());
        assertEquals("a", evaluator.evaluate("${a:a()}", String.class));
        assertEquals("a,a", evaluator.evaluate("${a:a()},${a:a()}", String.class));
        try {
            evaluator.evaluate("${a:a(), a:a()}", String.class);
            fail("Evaluated bad expression");
        } catch (ELException ignore) { }
        assertTrue(evaluator.checkForExistence("${a:a()}${a:a()}!", "!"));
        assertTrue(evaluator.checkForExistence("${a:a()},${a:a()}", ","));
        assertFalse(evaluator.checkForExistence("${a:d('foo', 'bar')}", ","));
        try {
            evaluator.checkForExistence("${a:a(), a:a()}", ",");
            fail("Parsed bad expression");
        } catch (ELException ignore) { }

        assertNull(ELEvaluator.getCurrent());
    }

}
