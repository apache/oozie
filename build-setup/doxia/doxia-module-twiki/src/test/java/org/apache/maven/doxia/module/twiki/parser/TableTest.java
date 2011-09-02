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


/**
 * Tests the {@link org.apache.maven.doxia.module.twiki.parser.TableBlockParser}
 *
 * @author Juan F. Codagnone
 * @since Nov 9, 2005
 */
public class TableTest extends AbstractBlockTestCase
{

    /**
     * unit test the regex
     */
    public final void testRegex()
    {
        assertTrue( tableParser.accept( "  | cell1 | cell2|   " ) );
        assertFalse( tableParser.accept( "  | cell1 | cell" ) );
    }

    /**
     * @throws ParseException on error
     */
    public final void testTable() throws ParseException
    {
        final StringReader sw = new StringReader( ""
            + "  |cell1|cell2|  \n"
            + "|cell3|cell4|\n"
        );

        final ByLineSource source = new ByLineReaderSource( sw );

        Block block, expected;
        expected = new TableBlock( new Block[]{
            new TableRowBlock( new Block[]{
                new TableCellBlock( new Block[]{new TextBlock( "cell1" )} ),
                new TableCellBlock( new Block[]{new TextBlock( "cell2" )} ),
            } ),
            new TableRowBlock( new Block[]{
                new TableCellBlock( new Block[]{new TextBlock( "cell3" )} ),
                new TableCellBlock( new Block[]{new TextBlock( "cell4" )} ),
            } ),
        } );

        block = tableParser.visit( source.getNextLine(), source );
        assertEquals( block, expected );
    }

    /**
     * @throws ParseException on error
     */
    public final void testTableHeader() throws ParseException
    {
        final StringReader sw = new StringReader( "|*cell1*|*cell2*|\n"
        );

        final ByLineSource source = new ByLineReaderSource( sw );

        Block block, expected;
        expected = new TableBlock( new Block[]{
            new TableRowBlock( new Block[]{
                new TableCellHeaderBlock( new Block[]{new TextBlock( "cell1" )} ),
                new TableCellHeaderBlock( new Block[]{new TextBlock( "cell2" )} ),
            } ),
        } );

        block = tableParser.visit( source.getNextLine(), source );
        assertEquals( block, expected );
    }
}
