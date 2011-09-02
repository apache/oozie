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

import java.io.StringReader;
import java.util.Arrays;

import org.apache.maven.doxia.util.ByLineReaderSource;
import org.apache.maven.doxia.parser.ParseException;


/**
 * Tests the {@link org.apache.maven.doxia.module.twiki.parser.SectionBlockParser}
 *
 * @author Juan F. Codagnone
 * @since Nov 1, 2005
 */
public class SectionTest extends AbstractBlockTestCase
{

    /**
     * @see SectionBlock#SectionBlock(String, int, Block[])
     */
    public final void testSectionBlockWrongArgs()
    {
        final int maxLevel = 5;
        new SectionBlock( "hola", 1, new Block[]{} );
        new SectionBlock( "hola", maxLevel, new Block[]{} );

        try
        {
            new SectionBlock( "hola", maxLevel + 1, new Block[]{} );
            fail();
        }
        catch ( final Throwable e )
        {
            // ok
        }

        try
        {
            new SectionBlock( "hola", 0, new Block[]{} );
            fail();
        }
        catch ( final Throwable e )
        {
            // ok
        }

        try
        {
            new SectionBlock( null, 1, null );
            fail();
        }
        catch ( final Throwable e )
        {
            // ok
        }

        new SectionBlock( "", 1, new Block[]{} );
    }

    /**
     * @see SectionBlockParser#getLevel(String)
     */
    public final void testSectionParserGetLevel()
    {
        assertEquals( 2, SectionBlockParser.getLevel( "++" ) );
        try
        {
            SectionBlockParser.getLevel( "asdasd" );
            fail( "expected exception was not thrown" );
        }
        catch ( IllegalArgumentException e )
        {
            // ok
        }
    }

    /**
     * @see SectionBlockParser
     */
    public final void testSectionParser() throws Exception
    {
        final SectionBlockParser parser = sectionParser;
        assertTrue( parser.accept( "---+ Title1" ) );
        assertTrue( parser.accept( "---++ Title2" ) );
        assertFalse( parser.accept( " ---++ Title3" ) );
        assertTrue( parser.accept( "---+++ Title4" ) );
        assertTrue( parser.accept( "---++++ Title5" ) );
        assertTrue( parser.accept( "---+++++ Title6" ) );

        SectionBlock block;
        block = (SectionBlock) parser.visit( "---++++ Title4",
                                             new ByLineReaderSource( new StringReader( "" ) ) );

        final int level = 4;
        assertEquals( "Title4", block.getTitle() );
        assertEquals( level, block.getLevel() );
        assertEquals( 0, block.getBlocks().length );

        // ejemplo un poco m�s complejo
        block = (SectionBlock) parser.visit( "---+++ Title3",
                                             new ByLineReaderSource( new StringReader(
                                                 "This is *a* parragraph of a section.\n"
                                                     + "Some text.\n"
                                                     + "---+++ Another Title"
                                                     + "... and more text" ) ) );
        final SectionBlock expected = new SectionBlock( "Title3", 3, new Block[]{
            new ParagraphBlock( new Block[]{
                new TextBlock( "This is " ),
                new BoldBlock( new Block[]{new TextBlock( "a" )} ),
                new TextBlock( " parragraph of a section.\nSome text." ),
            } )
        } );
        assertEquals( expected, block );
    }

    /**
     * Test section with several paragraphs (the paragraph are plain text)
     *
     * @throws Exception on error
     */
    public final void testSectionWithParagraphs() throws Exception
    {
        final String text = ""
            + "---++ Title\n"
            + "\n"
            + "hey!\n"
            + "how are\n"
            + "you?\n"
            + "  \n  "
            + "Fine!! thanks";

        final SectionBlockParser parser = sectionParser;
        final ByLineReaderSource source = new ByLineReaderSource(
            new StringReader( text ) );
        final SectionBlock block = (SectionBlock) parser.visit( source
            .getNextLine(), source );
        assertEquals( 2, block.getBlocks().length );
        assertEquals( "hey!\nhow are\nyou?", ( (TextBlock) ( (ParagraphBlock) block
            .getBlocks()[0] ).getBlocks()[0] ).getText() );
        assertEquals( "Fine!! thanks", ( (TextBlock) ( (ParagraphBlock) block
            .getBlocks()[1] ).getBlocks()[0] ).getText() );
    }

    /**
     * @throws ParseException on error
     */
    public final void testSectionAndParaAndHrule() throws ParseException
    {
        Block[] blocks, expected;
        ByLineReaderSource source;

        source = new ByLineReaderSource( new StringReader( ""
            + "---++ Title\n"
            + "Some text\n"
            + "----------- More text\n"
        ) );
        expected = new Block[]{
            new SectionBlock( "Title", 1, new Block[]{
                new ParagraphBlock( new Block[]{
                    new TextBlock( "Some text" )
                } ),
                new HorizontalRuleBlock(),
                new ParagraphBlock( new Block[]{
                    new TextBlock( "More text" )
                } ),
            } ),
        };
        blocks = (Block[]) twikiParser.parse( source ).toArray( new Block[]{} );
        assertTrue( Arrays.equals( expected, blocks ) );
    }
}
