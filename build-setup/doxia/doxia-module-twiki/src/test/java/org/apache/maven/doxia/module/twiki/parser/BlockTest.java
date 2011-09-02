package org.apache.maven.doxia.module.twiki.parser;

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

import java.util.Arrays;

import junit.framework.TestCase;


/**
 * Generic unit tests for
 * {@link Block}s
 *
 * @author Juan F. Codagnone
 * @since Nov 2, 2005
 */
public class BlockTest extends TestCase
{

    /**
     * @see TextBlock#equals(Object)
     */
    public final void testTextBlockEquals()
    {
        testEquals( new TextBlock( "bar" ), new TextBlock( "bar" ),
                    new TextBlock( "foo" ) );
    }

    /**
     * @see WikiWordBlock#equals(Object)
     */
    public final void testWikiWordBlockEquals()
    {
        testEquals( new WikiWordBlock( "bar" ), new WikiWordBlock( "bar" ),
                    new WikiWordBlock( "foo" ) );

        testEquals( new WikiWordBlock( "bar", "text" ),
                    new WikiWordBlock( "bar", "text" ),
                    new WikiWordBlock( "bar" ) );

        testEquals( new WikiWordBlock( "bar", "text" ),
                    new WikiWordBlock( "bar", "text" ),
                    new WikiWordBlock( "text", "bar" ) );

    }

    /**
     * @see LinkBlock#equals(Object)
     */
    public final void testLinkBlockEquals()
    {
        testEquals( new LinkBlock( "foo", "bar" ), new LinkBlock( "foo", "bar" ),
                    new LinkBlock( "bar", "foo" ) );
    }

    /**
     * @see ListItemBlock#equals(Object)
     */
    public final void testListBlockEquals()
    {
        final Block []blocks = new Block[]{
            new TextBlock( "hello" )
        };

        testEquals( new ListItemBlock( blocks ), new ListItemBlock( blocks ),
                    new ListItemBlock( new Block[]{} ) );
    }

    /**
     * @see ListItemBlock#equals(Object)
     */
    public final void testNestedBlockEquals()
    {

        testEquals(
            new ParagraphBlock( new Block[]{
                new BoldBlock( new Block[]{new TextBlock( "foo" )} )} ),
            new ParagraphBlock( new Block[]{
                new BoldBlock( new Block[]{new TextBlock( "foo" )} )} ),
            new ParagraphBlock( new Block[]{
                new BoldBlock( new Block[]{new TextBlock( "bar" )} )} )
        );
    }


    /**
     * @see AbstractFatherBlock#equals(Object)
     */
    public final void testAbstractFatherBlockEquals()
    {
        assertFalse( Arrays.equals( new Block[]{
            new TextBlock( "mary " ),
            new ItalicBlock( new Block[]{
                new MonospaceBlock( new Block[]{
                    new TextBlock( "has" )
                } )
            } ),
        },
                                    new Block[]{
                                        new TextBlock( "mary " ),
                                        new BoldBlock( new Block[]{
                                            new MonospaceBlock( new Block[]{
                                                new TextBlock( "has" )
                                            } )
                                        } ),
                                    }
        ) );
    }

    /**
     * @see AnchorBlock#equals(Object)
     */
    public final void testAnchorBlockEquals()
    {
        testEquals( new AnchorBlock( "anchor" ), new AnchorBlock( "anchor" ),
                    new AnchorBlock( "anch" ) );
    }

    /**
     * @see HorizontalRuleBlock#equals(Object)
     */
    public final void testHorizontalEquals()
    {
        testEquals( new HorizontalRuleBlock(), new HorizontalRuleBlock(), "foo" );
    }

    /**
     * @param a an object
     * @param b an object that is equals to a
     * @param c a diferent object
     */
    public final void testEquals( final Object a, final Object b,
                                  final Object c )
    {
        assertFalse( a.equals( null ) );
        assertFalse( b.equals( null ) );
        assertFalse( c.equals( null ) );

        assertNotSame( a, b );

        assertEquals( a, a );
        assertEquals( b, b );
        assertEquals( c, c );

        assertEquals( a, b );
        assertEquals( b, a );
        assertFalse( a.equals( c ) );

        assertEquals( a.hashCode(), b.hashCode() );
    }
}
