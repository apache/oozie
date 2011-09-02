package org.apache.maven.doxia.parser;

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

import java.io.File;

import org.apache.maven.doxia.macro.Macro;
import org.apache.maven.doxia.macro.MacroExecutionException;
import org.apache.maven.doxia.macro.MacroRequest;
import org.apache.maven.doxia.macro.manager.MacroManager;
import org.apache.maven.doxia.macro.manager.MacroNotFoundException;
import org.apache.maven.doxia.sink.Sink;

/**
 * An abstract base class that defines some convenience methods for parsers.
 * Provides a macro mechanism to give dynamic functionalities for the parsing.
 *
 * @author Jason van Zyl
 * @version $Id: AbstractParser.java 564180 2007-08-09 12:15:44Z vsiveton $
 * @since 1.0
 * @plexus.component
 */
public abstract class AbstractParser
    implements Parser
{
    /** Indicates that a second parsing is required. */
    protected boolean secondParsing = false;

    /** @plexus.requirement */
    protected MacroManager macroManager;

    /** {@inheritDoc} */
    public int getType()
    {
        return UNKNOWN_TYPE;
    }

    /**
     * Execute a macro on the given sink.
     *
     * @param macroId An id to lookup the macro.
     * @param request The corresponding MacroRequest.
     * @param sink The sink to receive the events.
     * @throws MacroExecutionException if an error occurred during execution.
     * @throws MacroNotFoundException if the macro could not be found.
     */
    // Made public right now because of the structure of the APT parser and
    // all its inner classes.
    public void executeMacro( String macroId, MacroRequest request, Sink sink )
        throws MacroExecutionException, MacroNotFoundException
    {
        Macro macro = macroManager.getMacro( macroId );

        macro.execute( sink, request );
    }

    /**
     * Returns the current base directory.
     *
     * @return The base directory.
     */
    protected File getBasedir()
    {
        // TODO: This is baaad, it should come in with the request.

        String basedir = System.getProperty( "basedir" );

        if ( basedir != null )
        {
            return new File( basedir );
        }

        return new File( new File( "" ).getAbsolutePath() );
    }

    /**
     * Set <code>secondParsing</code> to true, if we need a second parsing.
     *
     * @param second True for second parsing.
     */
    public void setSecondParsing( boolean second )
    {
        this.secondParsing = second;
    }
}
