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

import org.apache.maven.doxia.sink.Sink;
import org.apache.maven.doxia.sink.SinkAdapter;
import org.codehaus.plexus.PlexusTestCase;

import java.io.FileReader;
import java.io.Reader;

/**
 * Test the parsing of sample input files
 *
 * @author <a href="mailto:carlos@apache.org">Carlos Sanchez</a>
 * @author <a href="mailto:evenisse@codehaus.org">Emmanuel Venisse</a>
 * @version $Id: AbstractParserTestCase.java 567311 2007-08-18 18:30:54Z vsiveton $
 * @since 1.0
 */
public abstract class AbstractParserTestCase
    extends PlexusTestCase
{
    /**
     * Parser to use to convert input to sink events
     *
     * @return the parser to use
     */
    protected abstract Parser getParser();

    /**
     * Path of the model to test, relative to basedir
     *
     * @return the relative path
     */
    protected abstract String getDocument();

    /**
     * Sink to write the output of the parsing
     *
     * @return a SinkAdapter if not overridden
     */
    protected Sink getSink()
    {
        return new SinkAdapter();
    }

    /**
     * Parse the model in the path specified by {@link #getDocument()},
     * with parser from {@link #getParser()}, and output to sink from {@link #getSink()}
     *
     * @throws Exception
     */
    public void testParser()
        throws Exception
    {
        Reader reader = new FileReader( getTestFile( getBasedir(), getDocument() ) );

        getParser().parse( reader, getSink() );
    }
}
