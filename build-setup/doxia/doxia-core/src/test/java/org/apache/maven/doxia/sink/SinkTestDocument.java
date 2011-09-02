package org.apache.maven.doxia.sink;

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

import java.io.UnsupportedEncodingException;

import org.apache.maven.doxia.parser.Parser;
import org.apache.maven.doxia.sink.Sink;

/**
 * Static methods to generate standard Doxia sink events.
 */
public class SinkTestDocument
{

    /** Private constructor. */
    private SinkTestDocument()
    {
        // do not instantiate
    }

    /**
     * Dumps a full model that mimics aptconvert's test.apt,
     * into the specified sink. The sink is flushed but not closed.
     *
     * @param sink The sink to receive the events.
     */
    public static void generate( Sink sink )
    {
        generateHead( sink );

        sink.body();

        // TODO: what is this supposed to do?
        //sink.sectionTitle();
        //sink.text( "Section Title" );
        //sink.sectionTitle_();

        sink.paragraph();
        sink.text( "Paragraph 1, line 1. Paragraph 1, line 2." );
        sink.paragraph_();

        sink.paragraph();
        sink.text( "Paragraph 2, line 1. Paragraph 2, line 2." );
        sink.paragraph_();

        sink.section1();
        sink.sectionTitle1();
        sink.text( "Section title" );
        sink.sectionTitle1_();

        sink.section2();
        sink.sectionTitle2();
        sink.text( "Sub-section title" );
        sink.sectionTitle2_();

        sink.section3();
        sink.sectionTitle3();
        sink.text( "Sub-sub-section title" );
        sink.sectionTitle3_();

        sink.section4();
        sink.sectionTitle4();
        sink.text( "Sub-sub-sub-section title" );
        sink.sectionTitle4_();

        sink.section5();
        sink.sectionTitle5();
        sink.text( "Sub-sub-sub-sub-section title" );
        sink.sectionTitle5_();

        generateList( sink );

        sink.verbatim( true );
        sink.text( "Verbatim text not contained in list item 3" );
        sink.verbatim_();

        generateNumberedList( sink );

        sink.paragraph();
        sink.text( "List numbering schemes: [[1]], [[a]], [[A]], [[i]], [[I]]." );
        sink.paragraph_();

        generateDefinitionList( sink );

        sink.paragraph();
        sink.text( "--- instead of +-- suppresses the box around verbatim text." );
        sink.paragraph_();

        generateFigure( sink );

        generateTable( sink );

        sink.paragraph();
        sink.text( "No grid, no caption:" );
        sink.paragraph_();

        generateNoGridTable( sink );

        generateHeaderTable( sink );

        generateHorizontalRule( sink );

        generatePageBreak( sink );

        generateFonts( sink );

        generateAnchors( sink );

        generateLineBreak( sink );

        generateNonBreakingSpace( sink );

        generateSpecialCharacters( sink );

        sink.section5_();
        sink.section4_();
        sink.section3_();
        sink.section2_();
        sink.section1_();

        sink.body_();

        sink.flush();
    }

    /**
     * Dumps a header with title, author and date elements
     * into the specified sink. The sink is not flushed or closed.
     *
     * @param sink The sink to receive the events.
     */
    public static void generateHead( Sink sink )
    {
        sink.head();

        sink.title();
        sink.text( "Title" );
        sink.title_();

        sink.author();
        sink.text( "Author" );
        sink.author_();

        sink.date();
        sink.text( "Date" );
        sink.date_();

        sink.head_();
    }

    /**
     * Dumps a list into the specified sink. The sink is not flushed or closed.
     *
     * @param sink The sink to receive the events.
     */
    public static void generateList( Sink sink )
    {
        sink.list();

        sink.listItem();
        sink.paragraph();
        sink.text( "List item 1." );
        sink.paragraph_();
        sink.listItem_();

        sink.listItem();
        sink.paragraph();
        sink.text( "List item 2." );
        sink.paragraph_();
        sink.paragraph();
        sink.text( "Paragraph contained in list item 2." );
        sink.paragraph_();

        sink.list();

        sink.listItem();
        sink.paragraph();
        sink.text( "Sub-list item 1." );
        sink.paragraph_();
        sink.listItem_();

        sink.listItem();
        sink.paragraph();
        sink.text( "Sub-list item 2." );
        sink.paragraph_();
        sink.listItem_();

        sink.list_();

        sink.listItem_();

        sink.listItem();
        sink.paragraph();
        sink.text( "List item 3. Force end of list:" );
        sink.paragraph_();
        sink.listItem_();

        sink.list_();
    }

    /**
     * Dumps a numbered list into the specified sink.
     * The sink is not flushed or closed.
     *
     * @param sink The sink to receive the events.
     */
    public static void generateNumberedList( Sink sink )
    {
        sink.numberedList( Sink.NUMBERING_DECIMAL );

        sink.numberedListItem();
        sink.paragraph();
        sink.text( "Numbered item 1." );
        sink.paragraph_();

        sink.numberedList( Sink.NUMBERING_UPPER_ALPHA );

        sink.numberedListItem();
        sink.paragraph();
        sink.text( "Numbered item A." );
        sink.paragraph_();
        sink.numberedListItem_();

        sink.numberedListItem();
        sink.paragraph();
        sink.text( "Numbered item B." );
        sink.paragraph_();
        sink.numberedListItem_();

        sink.numberedList_();

        sink.numberedListItem_();

        sink.numberedListItem();
        sink.paragraph();
        sink.text( "Numbered item 2." );
        sink.paragraph_();
        sink.numberedListItem_();

        sink.numberedList_();
    }

    /**
     * Dumps a definition list into the specified sink.
     * The sink is not flushed or closed.
     *
     * @param sink The sink to receive the events.
     */
    public static void generateDefinitionList( Sink sink )
    {
        String EOL = System.getProperty( "line.separator" );

        sink.definitionList();

        sink.definitionListItem();
        sink.definedTerm();
        sink.text( "Defined term 1" );
        sink.definedTerm_();
        sink.definition();
        sink.paragraph();
        sink.text( "of definition list." );
        sink.paragraph_();
        sink.definition_();
        sink.definitionListItem_();

        sink.definitionListItem();
        sink.definedTerm();
        sink.text( "Defined term 2" );
        sink.definedTerm_();
        sink.definition();
        sink.paragraph();
        sink.text( "of definition list." );
        sink.paragraph_();
        sink.verbatim( true );
        sink.text( "Verbatim text" + EOL + "                        in a box        " );
        sink.verbatim_();
        sink.definition_();
        sink.definitionListItem_();

        sink.definitionList_();
    }

    /**
     * Dumps a figure with figure caption into the specified sink.
     * The sink is not flushed or closed.
     *
     * @param sink The sink to receive the events.
     */
    public static void generateFigure( Sink sink )
    {
        sink.figure();

        sink.figureGraphics( "figure" );

        sink.figureCaption();
        sink.text( "Figure caption" );
        sink.figureCaption_();

        sink.figure_();
    }

    /**
     * Dumps a table with grid and caption into the specified sink.
     * The sink is not flushed or closed.
     *
     * @param sink The sink to receive the events.
     */
    public static void generateTable( Sink sink )
    {
        int[] justify =
        {
             Parser.JUSTIFY_CENTER, Parser.JUSTIFY_LEFT, Parser.JUSTIFY_RIGHT
        };

        sink.table();

        sink.tableRows( justify, true );

        sink.tableRow();
        sink.tableCell();
        sink.text( "Centered" );
        sink.lineBreak();
        sink.text( "cell 1,1" );
        sink.tableCell_();
        sink.tableCell();
        sink.text( "Left-aligned" );
        sink.lineBreak();
        sink.text( "cell 1,2" );
        sink.tableCell_();
        sink.tableCell();
        sink.text( "Right-aligned" );
        sink.lineBreak();
        sink.text( "cell 1,3" );
        sink.tableCell_();
        sink.tableRow_();

        sink.tableRow();
        sink.tableCell();
        sink.text( "cell 2,1" );
        sink.tableCell_();
        sink.tableCell();
        sink.text( "cell 2,2" );
        sink.tableCell_();
        sink.tableCell();
        sink.text( "cell 2,3" );
        sink.tableCell_();
        sink.tableRow_();

        sink.tableRows_();

        sink.tableCaption();
        sink.text( "Table caption" );
        sink.tableCaption_();

        sink.table_();
    }

    /**
     * Dumps a table without grid into the specified sink.
     * The sink is not flushed or closed.
     *
     * @param sink The sink to receive the events.
     */
    public static void generateNoGridTable( Sink sink )
    {
        int[] justify =
        {
             Parser.JUSTIFY_CENTER, Parser.JUSTIFY_CENTER
        };

        sink.table();

        sink.tableRows( justify, false );

        sink.tableRow();
        sink.tableCell();
        sink.text( "cell" );
        sink.tableCell_();
        sink.tableCell();
        sink.text( "cell" );
        sink.tableCell_();
        sink.tableRow_();

        sink.tableRow();
        sink.tableCell();
        sink.text( "cell" );
        sink.tableCell_();
        sink.tableCell();
        sink.text( "cell" );
        sink.tableCell_();
        sink.tableRow_();

        sink.tableRows_();

        sink.table_();
    }

    /**
     * Dumps a table with a header row into the specified sink.
     * The sink is not flushed or closed.
     *
     * @param sink The sink to receive the events.
     */
    public static void generateHeaderTable( Sink sink )
    {
        int[] justify =
        {
             Parser.JUSTIFY_CENTER, Parser.JUSTIFY_CENTER
        };

        sink.table();

        sink.tableRows( justify, true );

        sink.tableRow();
        sink.tableHeaderCell();
        sink.text( "header" );
        sink.tableHeaderCell_();
        sink.tableHeaderCell();
        sink.text( "header" );
        sink.tableHeaderCell_();
        sink.tableRow_();

        sink.tableRow();
        sink.tableCell();
        sink.text( "cell" );
        sink.tableCell_();
        sink.tableCell();
        sink.text( "cell" );
        sink.tableCell_();
        sink.tableRow_();

        sink.tableRows_();

        sink.table_();
    }



    /**
     * Dumps a paragraph with italic, bold and monospaced text
     * into the specified sink. The sink is not flushed or closed.
     *
     * @param sink The sink to receive the events.
     */
    public static void generateFonts( Sink sink )
    {
        sink.paragraph();

        sink.italic();
        sink.text( "Italic" );
        sink.italic_();
        sink.text( " font. " );

        sink.bold();
        sink.text( "Bold" );
        sink.bold_();
        sink.text( " font. " );

        sink.monospaced();
        sink.text( "Monospaced" );
        sink.monospaced_();
        sink.text( " font." );

        sink.paragraph_();
    }

    /**
     * Dumps a paragraph with anchor and link elements
     * into the specified sink. The sink is not flushed or closed.
     *
     * @param sink The sink to receive the events.
     */
    public static void generateAnchors( Sink sink )
    {
        sink.paragraph();

        sink.anchor( "Anchor" );
        sink.text( "Anchor" );
        sink.anchor_();

        sink.text( ". Link to " );
        sink.link( "Anchor" );
        sink.text( "Anchor" );
        sink.link_();

        sink.text( ". Link to " );
        sink.link( "http://www.pixware.fr" );
        sink.text( "http://www.pixware.fr" );
        sink.link_();

        sink.text( ". Link to " );
        sink.link( "Anchor" );
        sink.text( "showing alternate text" );
        sink.link_();

        sink.text( ". Link to " );
        sink.link( "http://www.pixware.fr" );
        sink.text( "Pixware home page" );
        sink.link_();

        sink.text( "." );

        sink.paragraph_();
    }

    /**
     * Dumps a horizontal rule block into the specified sink.
     * The sink is not flushed or closed.
     *
     * @param sink The sink to receive the events.
     */
    public static void generateHorizontalRule( Sink sink )
    {
        sink.paragraph();
        sink.text( "Horizontal line:" );
        sink.paragraph_();
        sink.horizontalRule();
    }

    /**
     * Dumps a pageBreak block into the specified sink.
     * The sink is not flushed or closed.
     *
     * @param sink The sink to receive the events.
     */
    public static void generatePageBreak( Sink sink )
    {
        sink.pageBreak();
        sink.paragraph();
        sink.text( "New page." );
        sink.paragraph_();
    }

    /**
     * Dumps a lineBreak block into the specified sink.
     * The sink is not flushed or closed.
     *
     * @param sink The sink to receive the events.
     */
    public static void generateLineBreak( Sink sink )
    {
        sink.paragraph();
        sink.text( "Force line" );
        sink.lineBreak();
        sink.text( "break." );
        sink.paragraph_();
    }

    /**
     * Dumps a nonBreakingSpace block into the specified sink.
     * The sink is not flushed or closed.
     *
     * @param sink The sink to receive the events.
     */
    public static void generateNonBreakingSpace( Sink sink )
    {
        sink.paragraph();
        sink.text( "Non" );
        sink.nonBreakingSpace();
        sink.text( "breaking" );
        sink.nonBreakingSpace();
        sink.text( "space." );
        sink.paragraph_();
    }

    /**
     * Dumps a special character block into the specified sink.
     * The sink is not flushed or closed.
     *
     * @param sink The sink to receive the events.
     */
    public static void generateSpecialCharacters( Sink sink )
    {
        sink.paragraph();
        sink.text( "Escaped special characters: ~, =, -, +, *, [, ], <, >, {, }, \\." );
        sink.paragraph_();

        sink.paragraph();
        String copyright = String.valueOf( '\u00a9' );
        sink.text( "Copyright symbol: " + copyright + ", "
            + copyright + ", " + copyright + "." );
        sink.paragraph_();
    }
}
