package org.apache.maven.doxia.module.site;

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

/**
 * An abstract base class that implements the SiteModule interface.
 *
 * @author <a href="mailto:jason@maven.org">Jason van Zyl</a>
 * @version $Id: AbstractSiteModule.java 562704 2007-08-04 12:20:40Z vsiveton $
 * @since 1.0
 * @plexus.component
 */
public abstract class AbstractSiteModule
    implements SiteModule
{
    /** The source directory. */
    private String sourceDirectory;

    /** The default file extension. */
    private String extension;

    /** {@inheritDoc} */
    public String getSourceDirectory()
    {
        return sourceDirectory;
    }

    /** {@inheritDoc} */
    public String getExtension()
    {
        return extension;
    }
}
