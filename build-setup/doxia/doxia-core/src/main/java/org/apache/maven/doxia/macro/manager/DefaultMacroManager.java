package org.apache.maven.doxia.macro.manager;

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

import org.apache.maven.doxia.macro.Macro;

import java.util.Map;

/**
 * Default implementation of <code>MacroManager</code>
 *
 * @author <a href="mailto:jason@maven.org">Jason van Zyl</a>
 * @version $Id: DefaultMacroManager.java 567311 2007-08-18 18:30:54Z vsiveton $
 * @since 1.0
 * @plexus.component
 */
public class DefaultMacroManager
    implements MacroManager
{
    /** @plexus.requirement role="org.apache.maven.doxia.macro.Macro" */
    private Map macros;

    /** {@inheritDoc} */
    public Macro getMacro( String id )
        throws MacroNotFoundException
    {
        Macro macro = (Macro) macros.get( id );

        if ( macro == null )
        {
            throw new MacroNotFoundException( "Cannot find macro with id = " + id );
        }

        return macro;
    }
}
