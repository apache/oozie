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

import org.apache.commons.el.ExpressionEvaluatorImpl;
import org.apache.commons.el.ExpressionString;

import javax.servlet.jsp.el.ELException;
import javax.servlet.jsp.el.ExpressionEvaluator;
import javax.servlet.jsp.el.FunctionMapper;
import javax.servlet.jsp.el.VariableResolver;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

/**
 * JSP Expression Language Evaluator. <p/> It provides a more convenient way of using the JSP EL Evaluator.
 */
public class ELEvaluator {

    /**
     * Provides functions and variables for the EL evaluator. <p/> All functions and variables in the context of an EL
     * evaluator are accessible from EL expressions.
     */
    public static class Context implements VariableResolver, FunctionMapper {
        private Map<String, Object> vars;
        private Map<String, Method> functions;

        /**
         * Create an empty context.
         */
        public Context() {
            vars = new HashMap<String, Object>();
            functions = new HashMap<String, Method>();
        }

        /**
         * Add variables to the context. <p/>
         *
         * @param vars variables to add to the context.
         */
        public void setVariables(Map<String, Object> vars) {
            this.vars.putAll(vars);
        }

        /**
         * Add a variable to the context. <p/>
         *
         * @param name variable name.
         * @param value variable value.
         */
        public void setVariable(String name, Object value) {
            vars.put(name, value);
        }

        /**
         * Return a variable from the context. <p/>
         *
         * @param name variable name.
         * @return the variable value.
         */
        public Object getVariable(String name) {
            return vars.get(name);
        }

        /**
         * Add a function to the context. <p/>
         *
         * @param prefix function prefix.
         * @param functionName function name.
         * @param method method that will be invoked for the function, it must be a static and public method.
         */
        public void addFunction(String prefix, String functionName, Method method) {
            if ((method.getModifiers() & (Modifier.PUBLIC | Modifier.STATIC)) != (Modifier.PUBLIC | Modifier.STATIC)) {
                throw new IllegalArgumentException(XLog.format("Method[{0}] must be public and static", method));
            }
            prefix = (prefix.length() > 0) ? prefix + ":" : "";
            functions.put(prefix + functionName, method);
        }

        /**
         * Resolve a variable name. Used by the EL evaluator implemenation. <p/>
         *
         * @param name variable name.
         * @return the variable value.
         * @throws ELException thrown if the variable is not defined in the context.
         */
        public Object resolveVariable(String name) throws ELException {
            if (!vars.containsKey(name)) {
                throw new ELException(XLog.format("variable [{0}] cannot be resolved", name));
            }
            return vars.get(name);
        }

        /**
         * Resolve a function prefix:name. Used by the EL evaluator implementation. <p/>
         *
         * @param prefix function prefix.
         * @param name function name.
         * @return the method associated to the function.
         */
        public Method resolveFunction(String prefix, String name) {
            if (prefix.length() > 0) {
                name = prefix + ":" + name;
            }
            return functions.get(name);
        }
    }

    private static ThreadLocal<ELEvaluator> current = new ThreadLocal<ELEvaluator>();

    /**
     * If within the scope of a EL evaluation call, it gives access to the ELEvaluator instance performing the EL
     * evaluation. <p/> This is useful for EL function methods to get access to the variables of the Evaluator. Because
     * of this, ELEvaluator variables can be used to pass context to EL function methods (which must be static methods).
     * <p/>
     *
     * @return the ELEvaluator in scope, or <code>null</code> if none.
     */
    public static ELEvaluator getCurrent() {
        return current.get();
    }

    private Context context;

    private ExpressionEvaluatorImpl evaluator = new ExpressionEvaluatorImpl();

    /**
     * Creates an ELEvaluator with no functions and no variables defined.
     */
    public ELEvaluator() {
        this(new Context());
    }

    /**
     * Creates an ELEvaluator with the functions and variables defined in the given {@link ELEvaluator.Context}. <p/>
     *
     * @param context the ELSupport with functions and variables to be available for EL evalution.
     */
    public ELEvaluator(Context context) {
        this.context = context;
    }

    /**
     * Return the context with the functions and variables of the EL evaluator. <p/>
     *
     * @return the context.
     */
    public Context getContext() {
        return context;
    }

    /**
     * Convenience method that sets a variable in the EL evaluator context. <p/>
     *
     * @param name variable name.
     * @param value variable value.
     */
    public void setVariable(String name, Object value) {
        context.setVariable(name, value);
    }

    /**
     * Convenience method that returns a variable from the EL evaluator context. <p/>
     *
     * @param name variable name.
     * @return the variable value, <code>null</code> if not defined.
     */
    public Object getVariable(String name) {
        return context.getVariable(name);
    }

    /**
     * Evaluate an EL expression. <p/>
     *
     * @param expr EL expression to evaluate.
     * @param clazz return type of the EL expression.
     * @return the object the EL expression evaluated to.
     * @throws Exception thrown if an EL function failed due to a transient error or EL expression could not be
     * evaluated.
     */
    @SuppressWarnings({"unchecked", "deprecation"})
    public <T> T evaluate(String expr, Class<T> clazz) throws Exception {
        ELEvaluator existing = current.get();
        try {
            current.set(this);
            return (T) evaluator.evaluate(expr, clazz, context, context);
        }
        catch (ELException ex) {
            if (ex.getRootCause() instanceof Exception) {
                throw (Exception) ex.getRootCause();
            }
            else {
                throw ex;
            }
        }
        finally {
            current.set(existing);
        }
    }

    /**
     * Check if the input expression contains sequence statically. for example
     * identify if "," is present outside of a function invocation in the given
     * expression. Ex "${func('abc')},${func('def'}",
     *
     * @param expr - Expression string
     * @param sequence - char sequence to check in the input expression
     * @return true if present
     * @throws Exception Exception thrown if an EL function failed due to a
     *         transient error or EL expression could not be parsed
     */
    public boolean checkForExistence(String expr, String sequence)
            throws Exception {
        try {
            Object exprString = evaluator.parseExpressionString(expr);
            if (exprString instanceof ExpressionString) {
                for (Object element : ((ExpressionString)exprString).getElements()) {
                    if (element instanceof String &&
                            element.toString().contains(sequence)) {
                        return true;
                    }
                }
            } else if (exprString instanceof String) {
                if (((String)exprString).contains(sequence)) {
                    return true;
                }
            }
            return false;
        } catch (ELException ex) {
            if (ex.getRootCause() instanceof Exception) {
                throw (Exception) ex.getRootCause();
            }
            else {
                throw ex;
            }
        }
    }
}
