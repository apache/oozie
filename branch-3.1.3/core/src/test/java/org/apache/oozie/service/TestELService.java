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
package org.apache.oozie.service;

import javax.servlet.jsp.el.ELException;

import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.ELEvaluator;

public class TestELService extends XTestCase {

    public void testELForWorkflow() throws Exception {
        Services services = new Services();
        services.init();
        assertNotNull(services.get(ELService.class));
        ELEvaluator eval = services.get(ELService.class).createEvaluator("workflow");
        assertNotNull(eval.evaluate("${KB}", Long.class));
        assertNotNull(eval.evaluate("${MB}", Long.class));
        assertNotNull(eval.evaluate("${GB}", Long.class));
        assertNotNull(eval.evaluate("${TB}", Long.class));
        assertNotNull(eval.evaluate("${PB}", Long.class));
        assertNotNull(eval.evaluate("${trim(' ')}", String.class));
        assertNotNull(eval.evaluate("${concat('a', 'b')}", String.class));
        assertNotNull(eval.evaluate("${firstNotNull(null, 'b')}", String.class));
        assertNotNull(eval.evaluate("${timestamp()}", String.class));
        assertNotNull(eval.evaluate("${urlEncode('abc')}", String.class));
        services.destroy();
    }

}
