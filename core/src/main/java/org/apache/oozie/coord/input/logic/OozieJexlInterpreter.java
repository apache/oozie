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

import org.apache.commons.jexl2.Interpreter;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.oozie.coord.input.logic.CoordInputLogicEvaluatorResult.STATUS;

/**
 * Oozie implementation of jexl Interpreter
 */
public class OozieJexlInterpreter extends Interpreter {

    protected OozieJexlInterpreter(Interpreter base) {
        super(base);
    }

    public Object interpret(JexlNode node) {
        return node.jjtAccept(this, "");
    }

    public OozieJexlInterpreter(JexlEngine jexlEngine, JexlContext jexlContext, boolean strictFlag, boolean silentFlag) {
        super(jexlEngine, jexlContext, strictFlag, silentFlag);
    }

    public Object visit(ASTOrNode node, Object data) {
        CoordInputLogicEvaluatorResult left = (CoordInputLogicEvaluatorResult) node.jjtGetChild(0)
                .jjtAccept(this, data);

        if (left.isTrue()) {
            return left;
        }

        return node.jjtGetChild(1).jjtAccept(this, data);
    }

    /** {@inheritDoc} */
    public Object visit(ASTAndNode node, Object data) {
        CoordInputLogicEvaluatorResult left = (CoordInputLogicEvaluatorResult) node.jjtGetChild(0)
                .jjtAccept(this, data);

        if(left.isWaiting() || !left.isTrue()){
            return left;
        }

        CoordInputLogicEvaluatorResult right = (CoordInputLogicEvaluatorResult) node.jjtGetChild(1).jjtAccept(this,
                data);
        if(right.isWaiting()){
            return right;
        }
        if(left.isPhaseTwoEvaluation() || right.isPhaseTwoEvaluation()){
            return new CoordInputLogicEvaluatorResult(STATUS.PHASE_TWO_EVALUATION);
        }

        if (right.isTrue()) {
            right.appendDataSets(left.getDataSets());
        }
        return right;
    }

}
