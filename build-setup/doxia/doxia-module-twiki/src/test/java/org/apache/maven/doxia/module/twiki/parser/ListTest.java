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

import org.apache.maven.doxia.util.ByLineReaderSource;
import org.apache.maven.doxia.util.ByLineSource;
import org.apache.maven.doxia.parser.ParseException;
import org.apache.maven.doxia.sink.Sink;


/**
 * Units tests for Lists
 *
 * @author Juan F. Codagnone
 * @since Nov 1, 2005
 */
public class ListTest extends AbstractBlockTestCase
{

    /**
     * unit test for recurrent enumeration
     *
     * @throws ParseException on error
     */
    public final void testList() throws ParseException
    {
        final BlockParser parser = listParser;

        final String text = ""
            + "      * item1.1 \n"
            + "   * item2\n"
            + "      * item2.1\n"
            + "   * item3\n"
            + "      * item3.1\n"
            + "      * item3.2\n"
            + "         * item3.2.1\n"
            + "         * item3.2.2\n"
            + "         * item3.2.3\n"
            + "      * item3.3\n"
            + "         * item3.3.1\n"
            + "   * item4";

        final ByLineSource source = new ByLineReaderSource( new StringReader(
            text ) );
        final Block b = parser.visit( source.getNextLine(), source );
        final Block []firstLevelBlocks = ( (UnorderedListBlock) b ).getBlocks();
        final int numberOfChild = 4;
        assertEquals( numberOfChild, firstLevelBlocks.length );
        
        for ( int i = 0; i < firstLevelBlocks.length; i++ )
        {
            Block block = firstLevelBlocks[i];
            assertEquals( ListItemBlock.class, block.getClass() );
        }

        ListBlock list;
        ListItemBlock item;
        Block [] blocks;

        item = (ListItemBlock) firstLevelBlocks[1];
        blocks = item.getBlocks();
        assertEquals( 1, blocks.length );
        assertEquals( "item2", ( (TextBlock) blocks[0] ).getText() );
        list = item.getInnerList();
        assertNotNull( list );
        blocks = list.getBlocks();
        assertEquals( blocks.length, 1 );
        item = (ListItemBlock) blocks[0];
        assertEquals( 1, item.getBlocks().length );
        assertEquals( "item2.1", ( (TextBlock) item.getBlocks()[0] ).getText() );
    }

    /**
     * @throws ParseException on error
     */
    public final void testNumeringDecimal() throws ParseException
    {
        final String text = ""
            + "   1. item1\n"
            + "   1. item2\n"
            + "   1. item3";

        final ByLineSource source = new ByLineReaderSource( new StringReader(
            text ) );
        Block blocks, expected;
        expected = new NumeratedListBlock( Sink.NUMBERING_DECIMAL,
                                           new ListItemBlock[]{
                                               new ListItemBlock( new Block[]{new TextBlock( "item1" )} ),
                                               new ListItemBlock( new Block[]{new TextBlock( "item2" )} ),
                                               new ListItemBlock( new Block[]{new TextBlock( "item3" )} ),
                                           }
        );
        blocks = listParser.visit( source.getNextLine(), source );
        assertEquals( expected, blocks );
    }

    /**
     * @throws ParseException on error
     */
    public final void testHetero() throws ParseException
    {
        final String text = ""
            + "   A. item1\n"
            + "      * item1.1\n"
            + "      * item1.2\n"
            + "   B. item2\n"
            + "      i. item2.1\n"
            + "      i. item2.2\n"
            + "   C. item3";

        final ByLineSource source = new ByLineReaderSource( new StringReader(
            text ) );
        Block blocks, expected;
        expected = new NumeratedListBlock( Sink.NUMBERING_UPPER_ALPHA,
                                           new ListItemBlock[]{
                                               new ListItemBlock( new Block[]{new TextBlock( "item1" )},
                                                                  new UnorderedListBlock( new ListItemBlock[]{
                                                                      new ListItemBlock( new Block[]{
                                                                          new TextBlock( "item1.1" )} ),
                                                                      new ListItemBlock( new Block[]{
                                                                          new TextBlock( "item1.2" )} ),
                                                                  } ) ),
                                               new ListItemBlock( new Block[]{new TextBlock( "item2" )},
                                                                  new NumeratedListBlock(
                                                                      Sink.NUMBERING_LOWER_ROMAN,
                                                                      new ListItemBlock[]{
                                                                          new ListItemBlock( new Block[]{
                                                                              new TextBlock( "item2.1" )} ),
                                                                          new ListItemBlock( new Block[]{
                                                                              new TextBlock( "item2.2" )} ),
                                                                      } ) ),
                                               new ListItemBlock( new Block[]{new TextBlock( "item3" )} ),
                                           }
        );
        blocks = listParser.visit( source.getNextLine(), source );
        assertEquals( expected, blocks );
    }
}
