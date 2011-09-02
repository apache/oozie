package org.apache.maven.doxia.util;

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

import java.io.StringReader;

import org.apache.maven.doxia.parser.ParseException;
import org.apache.maven.doxia.util.ByLineReaderSource;

import junit.framework.TestCase;


/**
 * Unit test for {@link org.apache.maven.doxia.util.ByLineReaderSource}.
 *
 * @author Juan F. Codagnone
 * @since Nov 1, 2005
 */
public class ByLineReaderSourceTest extends TestCase
{

    /**
     * @throws ParseException on error
     */
    public final void testUse() throws ParseException
    {
        ByLineReaderSource r = new ByLineReaderSource(
            new StringReader( "1 \n2\n3" ) );
        assertEquals( -1, r.getLineNumber() );
        assertEquals( "", r.getName() );

        assertEquals( "1 ", r.getNextLine() );
        assertEquals( "2", r.getNextLine() );
        r.ungetLine();
        assertEquals( "2", r.getNextLine() );
        r.ungetLine();
        try
        {
            r.ungetLine();
            fail();
        }
        catch ( IllegalStateException e )
        {
            // ok;
        }
        assertEquals( "2", r.getNextLine() );
        assertEquals( "3", r.getNextLine() );
        assertEquals( null, r.getNextLine() );
    }
}
