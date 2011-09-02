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

import org.apache.maven.doxia.AbstractModuleTest;
import org.apache.maven.doxia.WellformednessCheckingSink;

import org.apache.maven.doxia.sink.Sink;
import org.apache.maven.doxia.sink.TextSink;
import org.apache.maven.doxia.parser.ParseException;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

/**
 * Test the parsing of sample input files.
 * <br/>
 * <b>Note</b>: you have to provide a sample "test." + outputExtension()
 * file in the test resources directory if you extend this class.
 *
 * @version $Id: AbstractParserTest.java 567227 2007-08-18 04:33:25Z vsiveton $
 * @since 1.0
 */
public abstract class AbstractParserTest
    extends AbstractModuleTest
{
    /**
     * Create a new instance of the parser to test.
     *
     * @return the parser to test.
     */
    protected abstract Parser createParser();

    /**
     * Returns the directory where all parser test output will go.
     *
     * @return The test output directory.
     */
    protected String getOutputDir()
    {
        return "parser/";
    }

    /**
     * Parse a test document '"test." + outputExtension()'
     * with parser from {@link #createParser()}, and output to a new
     * {@link WellformednessCheckingSink}. Asserts that output is well-formed.
     *
     * @throws IOException if the test document cannot be read.
     * @throws ParseException if the test document cannot be parsed.
     */
    public final void testParser()
        throws IOException, ParseException
    {

        WellformednessCheckingSink sink = new WellformednessCheckingSink();

        Reader reader = null;

        try
        {
            reader = getTestReader( "test", outputExtension() );

            createParser().parse( reader, sink );

            assertTrue( "Parser output not well-formed, last offending element: "
                + sink.getOffender(), sink.isWellformed() );
        }
        finally
        {
            if ( reader != null )
            {
                reader.close();
            }
        }
    }

     /**
     * Parse a test document '"test." + outputExtension()'
     * with parser from {@link #createParser()}, and output to a text file,
     * using the {@link org.apache.maven.doxia.sink.TextSink TextSink}.
     *
     * @throws IOException if the test document cannot be read.
     * @throws ParseException if the test document cannot be parsed.
     */
    public final void testDocument()
        throws IOException, ParseException
    {
        Writer writer = null;
        Reader reader = null;

        try
        {
            writer = getTestWriter( "test", "txt" );

            reader = getTestReader( "test", outputExtension() );

            Sink sink = new TextSink( writer );

            createParser().parse( reader, sink );
        }
        finally
        {
            if ( writer  != null )
            {
                writer.close();
            }

            if ( reader != null )
            {
                reader.close();
            }
        }
    }


}
