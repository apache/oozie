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

/**
 * Oozie implementation of Jexl Engine
 *
 */
public class OozieJexlEngine extends JexlEngine {
    OozieJexlInterpreter oozieInterpreter;

    public OozieJexlEngine() {
    }

    protected Interpreter createInterpreter(JexlContext context, boolean strictFlag, boolean silentFlag) {
        if (oozieInterpreter == null) {
            oozieInterpreter = new OozieJexlInterpreter(this, context == null ? EMPTY_CONTEXT : context, true,
                    silentFlag);
        }
        return oozieInterpreter;
    }

    public OozieJexlInterpreter getOozieInterpreter() {
        return oozieInterpreter;
    }

}
