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


/**
 * Tests the {@link org.apache.maven.doxia.module.twiki.parser.FormatedTextParser}
 *
 * @author Juan F. Codagnone
 * @since Nov 2, 2005
 */
public class FormatedTextTest extends AbstractBlockTestCase
{

    /**
     * test bold text
     */
    public final void testBold()
    {
        String text;
        Block []blocks;

        text = "*bold*";
        blocks = formatTextParser.parse( text );
        assertEquals( 1, blocks.length );
        assertEquals( new BoldBlock( new Block[]{new TextBlock( "bold" )} ),
                      blocks[0] );

        text = "foo *bold* bar";
        blocks = formatTextParser.parse( text );
        assertTrue( Arrays.equals( new Block[]{
            new TextBlock( "foo " ),
            new BoldBlock( new Block[]{new TextBlock( "bold" )} ),
            new TextBlock( " bar" ),
        }, blocks ) );

        text = "\t*bold* bar";
        blocks = formatTextParser.parse( text );
        assertTrue( Arrays.equals( new Block[]{
            new TextBlock( "\t" ),
            new BoldBlock( new Block[]{new TextBlock( "bold" )} ),
            new TextBlock( " bar" ),
        }, blocks ) );

        text = "*nice* foo *bold* bar";
        blocks = formatTextParser.parse( text );
        assertTrue( Arrays.equals( new Block[]{
            new BoldBlock( new Block[]{new TextBlock( "nice" )} ),
            new TextBlock( " foo " ),
            new BoldBlock( new Block[]{new TextBlock( "bold" )} ),
            new TextBlock( " bar" ),
        }, blocks ) );
    }

    /**
     * test italic text
     */
    public final void testItalic()
    {
        String text;
        Block []blocks;

        text = "_italic_";
        blocks = formatTextParser.parse( text );
        assertEquals( 1, blocks.length );
        assertEquals( new ItalicBlock( new Block[]{new TextBlock( "italic" )} ),
                      blocks[0] );

        text = "foo _italic_ bar";
        blocks = formatTextParser.parse( text );
        assertTrue( Arrays.equals( new Block[]{
            new TextBlock( "foo " ),
            new ItalicBlock( new Block[]{new TextBlock( "italic" )} ),
            new TextBlock( " bar" ),
        }, blocks ) );

        text = "_nice_ foo _italic_ bar";
        blocks = formatTextParser.parse( text );
        assertTrue( Arrays.equals( new Block[]{
            new ItalicBlock( new Block[]{new TextBlock( "nice" )} ),
            new TextBlock( " foo " ),
            new ItalicBlock( new Block[]{new TextBlock( "italic" )} ),
            new TextBlock( " bar" ),
        }, blocks ) );
    }

    /**
     * test monospaced text
     */
    public final void testMonospaced()
    {
        String text;
        Block []blocks;

        text = "mary =has= a =little= lamb";
        blocks = formatTextParser.parse( text );
        assertTrue( Arrays.equals( new Block[]{
            new TextBlock( "mary " ),
            new MonospaceBlock( new Block[]{new TextBlock( "has" )} ),
            new TextBlock( " a " ),
            new MonospaceBlock( new Block[]{new TextBlock( "little" )} ),
            new TextBlock( " lamb" ),
        }, blocks ) );
    }

    /**
     * test monospaced text
     */
    public final void testBoldMonospaced()
    {
        String text;
        Block []blocks;

        text = "mary ==has== a ==little== lamb";
        blocks = formatTextParser.parse( text );
        Block [] expected = new Block[]{
            new TextBlock( "mary " ),
            new BoldBlock( new Block[]{
                new MonospaceBlock( new Block[]{
                    new TextBlock( "has" )
                } )
            } ),
            new TextBlock( " a " ),
            new BoldBlock( new Block[]{
                new MonospaceBlock( new Block[]{
                    new TextBlock( "little" )
                } )
            } ),
            new TextBlock( " lamb" ),
        };

        assertTrue( Arrays.equals( expected, blocks ) );
    }

    /**
     * test monospaced text
     */
    public final void testBoldItalic()
    {
        String text;
        Block []blocks;

        text = "mary __has__ a __little__ lamb";
        blocks = formatTextParser.parse( text );
        assertTrue( Arrays.equals( new Block[]{
            new TextBlock( "mary " ),
            new BoldBlock( new Block[]{
                new ItalicBlock( new Block[]{
                    new TextBlock( "has" )
                } )
            } ),
            new TextBlock( " a " ),
            new BoldBlock( new Block[]{
                new ItalicBlock( new Block[]{
                    new TextBlock( "little" )
                } )
            } ),
            new TextBlock( " lamb" ),
        }, blocks ) );
    }

    /**
     * test mixed formats side by side
     */
    public final void testMultiFormatSideBySide()
    {
        String text;
        Block []blocks;
        Block []expected;


        text = "All *work and* =no play= _makes_ Juan a dull *boy*";
        blocks = formatTextParser.parse( text );

        expected = new Block[]{
            new TextBlock( "All " ),
            new BoldBlock( new Block[]{new TextBlock( "work and" )} ),
            new TextBlock( " " ),
            new MonospaceBlock( new Block[]{new TextBlock( "no play" )} ),
            new TextBlock( " " ),
            new ItalicBlock( new Block[]{new TextBlock( "makes" )} ),
            new TextBlock( " Juan a dull " ),
            new BoldBlock( new Block[]{new TextBlock( "boy" )} ),
        };
        assertTrue( Arrays.equals( expected, blocks ) );

    }

    /**
     * test mixed formats recursevily
     */
    public final void testMultiFormatInside()
    {
        String text;
        Block []blocks;
        Block []expected;

        text = "All *work and =no play _makes_ Juan= a dull* boy";
        blocks = formatTextParser.parse( text );

        expected = new Block[]{
            new TextBlock( "All " ),
            new BoldBlock( new Block[]{
                new TextBlock( "work and " ),
                new MonospaceBlock( new Block[]{
                    new TextBlock( "no play " ),
                    new ItalicBlock( new Block[]{new TextBlock( "makes" )} ),
                    new TextBlock( " Juan" ),
                } ),
                new TextBlock( " a dull" ),
            } ),
            new TextBlock( " boy" ),
        };
        assertTrue( Arrays.equals( expected, blocks ) );
    }

    /**
     * test unbonded formats
     */
    public final void testUnboundedFormat()
    {
        testHanging( "All *work and no play makes Juan a dull boy" );
        testHanging( "All __work and no play makes Juan a dull boy" );
        testHanging( "All __work and *no play makes _Juan a = dull boy" );
        testHanging( "*" );
        testHanging( "==" );
        testHanging( "**" ); // hehe
        testHanging( "*  hello   *" );
        testHanging( "*  hello   =*" );
        testHanging( "*=_  hello   _=*" );
    }

    /**
     * @param text unbonded text
     */
    public final void testHanging( final String text )
    {
        assertTrue( Arrays.equals(
            new Block[]{new TextBlock( text )},
            formatTextParser.parse( text ) ) );
    }
}
