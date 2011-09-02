package org.apache.maven.doxia.module.site.manager;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.maven.doxia.module.site.SiteModule;

import java.util.Collection;

/**
 * Handles SiteModule lookups.
 *
 * @author <a href="mailto:jason@maven.org">Jason van Zyl</a>
 * @version $Id: SiteModuleManager.java 562704 2007-08-04 12:20:40Z vsiveton $
 * @since 1.0
 */
public interface SiteModuleManager
{
    /** The Plexus lookup role. */
    String ROLE = SiteModuleManager.class.getName();

    /**
     * Returns a collection of SiteModules.
     *
     * @return The SiteModules.
     */
    Collection getSiteModules();

    /**
     * Returns the SiteModule that corresponds to the given id.
     *
     * @param id The identifier.
     * @return The corresponding SiteModule.
     * @throws SiteModuleNotFoundException if no SiteModule could be found
     * for the given id.
     */
    SiteModule getSiteModule( String id )
        throws SiteModuleNotFoundException;
}
