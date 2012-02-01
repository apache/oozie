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

import org.apache.oozie.service.UUIDService.ApplicationType;
import org.apache.oozie.test.XTestCase;

public class TestUUIDService extends XTestCase {

    public void testConfiguration() throws Exception {
        setSystemProperty(UUIDService.CONF_GENERATOR, "counter");
        Services services = new Services();
        services.init();
        services.destroy();

        setSystemProperty(UUIDService.CONF_GENERATOR, "random");
        services = new Services();
        services.init();
        services.destroy();

        try {
            setSystemProperty(UUIDService.CONF_GENERATOR, "x");
            services = new Services();
            services.init();
            services.destroy();
            fail();
        }
        catch (ServiceException ex) {
            //nop
        }
    }

    public void testPadding() throws Exception {
        setSystemProperty(UUIDService.CONF_GENERATOR, "counter");
        Services services = new Services();
        services.init();
        UUIDService uuid = services.get(UUIDService.class);
        String id = uuid.generateId(ApplicationType.WORKFLOW);
        assertTrue(id.startsWith("0000000-"));
        for (int i = 0; i < 1000; i++) {
            id = uuid.generateId(ApplicationType.WORKFLOW);
        }
        assertTrue(id.startsWith("0001000-"));
        services.destroy();
    }

    public void testChildId() throws Exception {
        setSystemProperty(UUIDService.CONF_GENERATOR, "counter");
        Services services = new Services();
        services.init();
        UUIDService uuid = services.get(UUIDService.class);
        String id = uuid.generateId(ApplicationType.WORKFLOW);
        String childId = uuid.generateChildId(id, "a");
        assertEquals(id, uuid.getId(childId));
        assertEquals("a", uuid.getChildName(childId));
        services.destroy();

        setSystemProperty(UUIDService.CONF_GENERATOR, "random");
        services = new Services();
        services.init();
        uuid = services.get(UUIDService.class);
        id = uuid.generateId(ApplicationType.WORKFLOW);
        childId = uuid.generateChildId(id, "a");
        assertEquals(id, uuid.getId(childId));
        assertEquals("a", uuid.getChildName(childId));
        services.destroy();
    }

}
