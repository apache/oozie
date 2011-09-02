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
 * Tests the {@link org.apache.maven.doxia.module.twiki.parser.ParagraphBlockParser}
 *
 * @author Juan F. Codagnone
 * @since Nov 1, 2005
 */
public class ParagraphTest extends AbstractBlockTestCase
{

    /**
     * @throws ParseException on error
     */
    public final void testMultiLines() throws ParseException
    {
        final String text = ""
            + "\n\n\n"
            + "para1 -> text1\n"
            + "para1 -> text2\n"
            + "\n"
            + "para2 -> text1\n"
            + "para2 -> text2\n"
            + "   \n   \n  "
            + "para2 -> text1\n"
            + "para2 -> text2\n";

        final ByLineReaderSource source = new ByLineReaderSource(
            new StringReader( text ) );
        final ParagraphBlockParser parser = paraParser;

        ParagraphBlock block;

        block = (ParagraphBlock) parser.visit( source
            .getNextLine(), source );
        assertNotNull( block );
        assertEquals( 1, block.getBlocks().length );
        assertEquals( "para1 -> text1\npara1 -> text2", ( (TextBlock) block
            .getBlocks()[0] ).getText() );

        block = (ParagraphBlock) parser.visit( source
            .getNextLine(), source );
        assertNotNull( block );
        assertEquals( 1, block.getBlocks().length );
        assertEquals( "para2 -> text1\npara2 -> text2", ( (TextBlock) block
            .getBlocks()[0] ).getText() );
    }

    /**
     * @throws ParseException on error
     */
    public final void testParagraphWithList() throws ParseException
    {
        final String text = ""
            + "Description text:\n"
            + "   * item1\n"
            + "   * item2\n"
            + "This is more text in the same paragraph\n"
            + "\n"
            + "Another paragraph";

        final ByLineReaderSource source = new ByLineReaderSource(
            new StringReader( text ) );
        final ParagraphBlockParser parser = paraParser;

        ParagraphBlock block;

        block = (ParagraphBlock) parser.visit( source
            .getNextLine(), source );
        assertNotNull( block );
        final Block[] firstLevelChilds = block.getBlocks();
        final int numberOfChilds = 3;
        assertEquals( numberOfChilds, firstLevelChilds.length );
        assertEquals( TextBlock.class, firstLevelChilds[0].getClass() );
        assertEquals( UnorderedListBlock.class,
                      firstLevelChilds[1].getClass() );
        assertEquals( TextBlock.class, firstLevelChilds[2].getClass() );

        final Block [] listChilds = ( (UnorderedListBlock) firstLevelChilds[1] )
            .getBlocks();
        assertEquals( 2, listChilds.length );
        assertEquals( 1, ( (ListItemBlock) listChilds[0] ).getBlocks().length );
        assertEquals( "item1", ( (TextBlock) ( (ListItemBlock) listChilds[0] )
            .getBlocks()[0] ).getText() );
        assertEquals( "item2", ( (TextBlock) ( (ListItemBlock) listChilds[1] )
            .getBlocks()[0] ).getText() );
    }

    /**
     * tests some valid weired lists
     *
     * @throws ParseException on error
     */
    public final void testParagraphWithStartingList() throws ParseException
    {
        final String text = ""
            + "   * item1\n"
            + "   * item2\n"
            + "This is more text in the same paragraph\n"
            + "\n"
            + "Another paragraph";

        final ByLineReaderSource source = new ByLineReaderSource(
            new StringReader( text ) );
        final ParagraphBlockParser parser = paraParser;

        ParagraphBlock block;

        block = (ParagraphBlock) parser.visit( source
            .getNextLine(), source );
        assertNotNull( block );
        final Block[] firstLevelChilds = block.getBlocks();
        assertEquals( 2, firstLevelChilds.length );
        assertEquals( UnorderedListBlock.class,
                      firstLevelChilds[0].getClass() );
        assertEquals( TextBlock.class, firstLevelChilds[1].getClass() );

        final Block [] listChilds = ( (UnorderedListBlock) firstLevelChilds[0] )
            .getBlocks();
        assertEquals( 2, listChilds.length );
        assertEquals( 1, ( (ListItemBlock) listChilds[0] ).getBlocks().length );
        assertEquals( "item1", ( (TextBlock) ( (ListItemBlock) listChilds[0] )
            .getBlocks()[0] ).getText() );
        assertEquals( "item2", ( (TextBlock) ( (ListItemBlock) listChilds[1] )
            .getBlocks()[0] ).getText() );
    }


    /**
     * @throws ParseException on error
     */
    public final void testHorizontalRule() throws ParseException
    {
        Block block, expected;
        ByLineReaderSource source;

        assertTrue( hruleParser.accept( "---" ) );
        assertFalse( hruleParser.accept( "---+ asdas" ) );

        source = new ByLineReaderSource( new StringReader( "" ) );
        expected = new HorizontalRuleBlock();
        block = hruleParser.visit( "---", source );
        assertNull( source.getNextLine() );
        assertEquals( expected, block );

        source = new ByLineReaderSource( new StringReader( "" ) );
        expected = new HorizontalRuleBlock();
        block = hruleParser.visit( "--- Some text ---- And some more", source );
        assertEquals( expected, block );
        expected = new ParagraphBlock( new Block[]{
            new TextBlock( "Some text ---- And some more" )
        } );
        block = paraParser.visit( source.getNextLine(), source );
        assertEquals( expected, block );
    }

    /**
     * @throws ParseException on error
     */
    public final void testHorizontalRuleAndParagraph() throws ParseException
    {
        Block[] blocks, expected;
        ByLineReaderSource source;

        source = new ByLineReaderSource( new StringReader( ""
            + "Some text\n"
            + "-----------\n"
            + "More text"
        ) );
        expected = new Block[]{
            new ParagraphBlock( new Block[]{new TextBlock( "Some text" )} ),
            new HorizontalRuleBlock(),
            new ParagraphBlock( new Block[]{new TextBlock( "More text" )} ),
        };
        blocks = (Block[]) twikiParser.parse( source ).toArray( new Block[]{} );
        assertTrue( Arrays.equals( expected, blocks ) );
    }
}
