package org.apache.maven.doxia;

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

import org.apache.maven.doxia.parser.ParseException;
import org.apache.maven.doxia.parser.Parser;
import org.apache.maven.doxia.parser.manager.ParserManager;
import org.apache.maven.doxia.parser.manager.ParserNotFoundException;
import org.apache.maven.doxia.sink.Sink;

import java.io.Reader;

/**
 * Simple implementation of the Doxia interface:
 * uses a ParserManager to lookup a parser.
 *
 * @author Jason van Zyl
 * @version $Id: DefaultDoxia.java 564180 2007-08-09 12:15:44Z vsiveton $
 * @since 1.0
 * @plexus.component
 */
public class DefaultDoxia
    implements Doxia
{
    /**
     * @plexus.requirement
     */
    private ParserManager parserManager;

    // ----------------------------------------------------------------------
    // This remains because the sinks are not threadsafe which they probably
    // should be. In some places a constructor is used to initialize a sink
    // which can probably be done away with.
    // ----------------------------------------------------------------------

    /** {@inheritDoc} */
    public void parse( Reader source, String parserId, Sink sink )
        throws ParserNotFoundException, ParseException
    {
        Parser parser = parserManager.getParser( parserId );

        parser.parse( source, sink );
    }

    /** {@inheritDoc} */
    public Parser getParser( String parserId )
        throws ParserNotFoundException
    {
        return parserManager.getParser( parserId );
    }
}
