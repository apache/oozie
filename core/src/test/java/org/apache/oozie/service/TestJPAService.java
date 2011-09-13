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

import org.apache.oozie.executor.jpa.JPAExecutor;
import org.apache.oozie.test.XTestCase;
import javax.persistence.EntityManager;

public class TestJPAService extends XTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        new Services().init();
    }

    @Override
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    public static class MyJPAExecutor implements JPAExecutor<String> {
        @Override
        public String getName() {
            return "my";
        }

        @Override
        public String execute(EntityManager em) {
            assertNotNull(em);
            return "ret";
        }
    }
    public void testExecute() throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        String ret = jpaService.execute(new MyJPAExecutor());
        assertEquals("ret", ret);
    }

}
