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

import org.codehaus.plexus.PlexusTestCase;

/**
 * Test case for <code>HtmlTools</code>.
 *
 * @author <a href="mailto:vincent.siveton@gmail.com">Vincent Siveton</a>
 * @version $Id: HtmlToolsTest.java 564980 2007-08-11 22:00:50Z dennisl $
 */
public class HtmlToolsTest
    extends PlexusTestCase
{
    /**
     * Verify the expected results.
     */
    public void testEscapeHTML()
    {
        assertEquals( HtmlTools.escapeHTML( "" ), "" );
        assertEquals( HtmlTools.escapeHTML( "<" ), "&lt;" );
        assertEquals( HtmlTools.escapeHTML( ">" ), "&gt;" );
        assertEquals( HtmlTools.escapeHTML( "&" ), "&amp;" );
        assertEquals( HtmlTools.escapeHTML( "\"" ), "&quot;" );
        assertEquals( HtmlTools.escapeHTML( "&amp;" ), "&amp;amp;" );
    }

    /**
     * Verify the expected results
     */
    public void testEncodeId()
    {
        assertEquals( HtmlTools.encodeId( null ), null );
        assertEquals( HtmlTools.encodeId( "" ), "" );
        assertEquals( HtmlTools.encodeId( " " ), "" );
        assertEquals( HtmlTools.encodeId( " _ " ), "a_" );
        assertEquals( HtmlTools.encodeId( "1" ), "a1" );
        assertEquals( HtmlTools.encodeId( "1anchor" ), "a1anchor" );
        assertEquals( HtmlTools.encodeId( "_anchor" ), "a_anchor" );
        assertEquals( HtmlTools.encodeId( "a b-c123 " ), "a_b-c123" );
        assertEquals( HtmlTools.encodeId( "   anchor" ), "anchor" );
        assertEquals( HtmlTools.encodeId( "myAnchor" ), "myAnchor" );
    }

    /**
     * Verify the expected results
     */
    public void testIsId()
    {
        assertFalse( HtmlTools.isId( null ));
        assertFalse( HtmlTools.isId( "" ) );
        assertFalse( HtmlTools.isId( " " ) );
        assertFalse( HtmlTools.isId( " _ " ) );
        assertFalse( HtmlTools.isId( "1" ) );
        assertFalse( HtmlTools.isId( "1anchor" ) );
        assertFalse( HtmlTools.isId( "_anchor" ) );
        assertFalse( HtmlTools.isId( "a b-c123 " ) );
        assertFalse( HtmlTools.isId( "   anchor" ) );
        assertTrue( HtmlTools.isId( "myAnchor" ) );
        assertTrue( HtmlTools.isId( "a_" ) );
        assertTrue( HtmlTools.isId( "a-" ) );
        assertTrue( HtmlTools.isId( "a:" ) );
        assertTrue( HtmlTools.isId( "a." ) );
    }
}
