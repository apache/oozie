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

package org.apache.oozie.coord.input.logic;

import java.util.Date;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.NamespaceResolver;
import org.apache.commons.lang.StringUtils;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.coord.CoordUtils;
import org.apache.oozie.coord.SyncCoordAction;
import org.apache.oozie.coord.input.dependency.CoordPullInputDependency;
import org.apache.oozie.coord.input.dependency.CoordPushInputDependency;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.JDOMException;

public class CoordInputLogicEvaluatorUtil {

    private CoordinatorActionBean coordAction = null;
    private XLog log = XLog.getLog(getClass());

    public CoordInputLogicEvaluatorUtil(CoordinatorActionBean coordAction) {
        this.coordAction = coordAction;
        LogUtils.setLogInfo(coordAction);

    }

    public CoordInputLogicEvaluatorUtil() {
    }

    /**
     * Check pull missing dependencies.
     *
     * @return true, if successful
     * @throws JDOMException the JDOM exception
     */
    public boolean checkPullMissingDependencies() throws JDOMException {
        JexlEngine jexl = new OozieJexlEngine();

        String expression = CoordUtils.getInputLogic(coordAction.getActionXml().toString());
        if (StringUtils.isEmpty(expression)) {
            return true;
        }
        Expression e = jexl.createExpression(expression);

        JexlContext jc = new OozieJexlParser(jexl, new CoordInputLogicBuilder(new CoordInputLogicEvaluatorPhaseOne(
                coordAction, coordAction.getPullInputDependencies())));
        CoordInputLogicEvaluatorResult result = (CoordInputLogicEvaluatorResult) e.evaluate(jc);
        log.debug("Input logic expression for [{0}] and evaluate result is [{1}]", expression, result.getStatus());

        if (result.isWaiting()) {
            return false;
        }
        return result.isTrue();
    }

    /**
     * Validate input logic.
     *
     * @throws JDOMException the JDOM exception
     * @throws CommandException in case of error
     */
    public void validateInputLogic() throws JDOMException, CommandException {
        JexlEngine jexl = new OozieJexlEngine();
        String expression = CoordUtils.getInputLogic(coordAction.getActionXml().toString());
        if (StringUtils.isEmpty(expression)) {
            return;
        }
        Expression e = jexl.createExpression(expression);
        JexlContext jc = new OozieJexlParser(jexl, new CoordInputLogicBuilder(
                new CoordInputLogicEvaluatorPhaseValidate(coordAction)));
        try {
            Object result = e.evaluate(jc);
            log.debug("Input logic expression is [{0}] and evaluate result is [{1}]", expression, result);

        }
        catch (RuntimeException re) {
            throw new CommandException(ErrorCode.E1028, re.getCause().getMessage());
        }

    }

    /**
     * Get input dependencies.
     *
     * @param name the name
     * @param syncCoordAction the sync coord action
     * @return the string
     * @throws JDOMException the JDOM exception
     */
    public String getInputDependencies(String name, SyncCoordAction syncCoordAction) throws JDOMException {
        JexlEngine jexl = new OozieJexlEngine();

        CoordinatorActionBean coordAction = new CoordinatorActionBean();
        ELEvaluator eval = ELEvaluator.getCurrent();
        coordAction.setId(syncCoordAction.getActionId());
        Element eJob = XmlUtils.parseXml(eval.getVariable(".actionInputLogic").toString());
        String expression = new InputLogicParser().parseWithName(eJob, name);

        Expression e = jexl.createExpression(expression);

        CoordPullInputDependency pull = (CoordPullInputDependency) syncCoordAction.getPullDependencies();
        CoordPushInputDependency push = (CoordPushInputDependency) syncCoordAction.getPushDependencies();

        coordAction.setPushInputDependencies(push);

        coordAction.setPullInputDependencies(pull);

        JexlContext jc = new OozieJexlParser(jexl, new CoordInputLogicBuilder(new CoordInputLogicEvaluatorPhaseThree(
                coordAction, eval)));
        CoordInputLogicEvaluatorResult result = (CoordInputLogicEvaluatorResult) e.evaluate(jc);

        if (result == null || !result.isTrue()) {
            log.debug("Input logic expression for [{0}] is [{1}] and it is not resolved", name, expression);
            return "${coord:dataIn('" + name + "')}";
        }
        else {
            log.debug("Input logic expression for [{0}] is [{1}] and evaluate result is [{2}]", name, expression,
                    result.getStatus());
            return result.getDataSets();
        }

    }

    /**
     * Check push dependencies.
     *
     * @return true, if successful
     * @throws JDOMException the JDOM exception
     */
    public boolean checkPushDependencies() throws JDOMException {
        JexlEngine jexl = new OozieJexlEngine();

        String expression = CoordUtils.getInputLogic(coordAction.getActionXml().toString());
        if (StringUtils.isEmpty(expression)) {
            return true;
        }

        Expression e = jexl.createExpression(expression);
        JexlContext jc = new OozieJexlParser(jexl, new CoordInputLogicBuilder(new CoordInputLogicEvaluatorPhaseOne(
                coordAction, coordAction.getPushInputDependencies())));
        CoordInputLogicEvaluatorResult result = (CoordInputLogicEvaluatorResult) e.evaluate(jc);
        log.debug("Input logic expression for [{0}] and evaluate result is [{1}]", expression, result.getStatus());

        if (result.isWaiting()) {
            return false;
        }
        return result.isTrue();
    }

    /**
     * Check unresolved.
     *
     * @param actualTime the actual time
     * @return true, if successful
     * @throws JDOMException the JDOM exception
     */
    public boolean checkUnResolved(Date actualTime) throws JDOMException {
        JexlEngine jexl = new OozieJexlEngine();

        String expression = CoordUtils.getInputLogic(coordAction.getActionXml().toString());
        if (StringUtils.isEmpty(expression)) {
            return true;
        }

        Expression e = jexl.createExpression(expression);
        JexlContext jc = new OozieJexlParser(jexl, new CoordInputLogicBuilder(new CoordInputLogicEvaluatorPhaseTwo(
                coordAction, actualTime)));
        CoordInputLogicEvaluatorResult result = (CoordInputLogicEvaluatorResult) e.evaluate(jc);
        log.debug("Input logic expression for [{0}] and evaluate result is [{1}]", expression, result.getStatus());

        if (result.isWaiting()) {
            return false;
        }
        return result.isTrue();

    }

    public class OozieJexlParser implements JexlContext, NamespaceResolver {
        private final JexlEngine jexl;
        private final CoordInputLogicBuilder object;

        @Override
        public Object resolveNamespace(String name) {
            return object;
        }

        public OozieJexlParser(JexlEngine engine, CoordInputLogicBuilder wrapped) {
            this.jexl = engine;
            this.object = wrapped;
        }

        public Object get(String name) {
            return jexl.getProperty(object, name);
        }

        public void set(String name, Object value) {
            jexl.setProperty(object, name, value);
        }

        public boolean has(String name) {
            return jexl.getUberspect().getPropertyGet(object, name, null) != null;
        }

    }

}
