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
package org.apache.oozie.servlet;

import org.apache.oozie.service.Services;

import javax.servlet.ServletContextListener;
import javax.servlet.ServletContextEvent;

/**
 * Webapp context listener that initializes Oozie {@link Services}.
 */
public class ServicesLoader implements ServletContextListener {
    private static Services services;

    /**
     * Initialize Oozie services.
     *
     * @param event context event.
     */
    public void contextInitialized(ServletContextEvent event) {
        try {
            services = new Services();
            services.init();
        }
        catch (Throwable ex) {
            System.out.println();
            System.out.println("ERROR: Oozie could not be started");
            System.out.println();
            System.out.println("REASON: " + ex.toString());
            System.out.println();
            System.out.println("Stacktrace:");
            System.out.println("-----------------------------------------------------------------");
            ex.printStackTrace(System.out);
            System.out.println("-----------------------------------------------------------------");
            System.out.println();
            System.exit(1);
        }
    }

    /**
     * Destroy Oozie services.
     *
     * @param event context event.
     */
    public void contextDestroyed(ServletContextEvent event) {
        services.destroy();
    }

}
