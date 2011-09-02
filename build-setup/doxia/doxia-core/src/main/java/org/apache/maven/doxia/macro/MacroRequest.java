package org.apache.maven.doxia.macro;

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

import java.util.Map;
import java.io.File;

/**
 * @author <a href="mailto:jason@maven.org">Jason van Zyl</a>
 * @version $Id: MacroRequest.java 567311 2007-08-18 18:30:54Z vsiveton $
 * @since 1.0
 */
public class MacroRequest
{
    /** The current base directory. */
    private File basedir;

    /** A map of parameters. */
    private Map parameters;

    /**
     * Constructor.
     *
     * @param param A map of parameters.
     * @param base The current base directory.
     */
    public MacroRequest( Map param, File base )
    {
        this.parameters = param;
        this.basedir = base;
    }

    /**
     * Returns the current base directory.
     *
     * @return The base dir.
     */
    public File getBasedir()
    {
        return basedir;
    }

    /**
     * Sets the current base directory.
     *
     * @param base The current base directory.
     */
    public void setBasedir( File base )
    {
        this.basedir = base;
    }

    /**
     * Returns the map of parameters.
     *
     * @return The map of parameters.
     */
    public Map getParameters()
    {
        return parameters;
    }

    /**
     * Returns on object from the map of parameters
     * that corresponds to the given key.
     *
     * @param key The key to lookup the object.
     * @return The value object.
     */
    public Object getParameter( String key )
    {
        return parameters.get( key );
    }
}
