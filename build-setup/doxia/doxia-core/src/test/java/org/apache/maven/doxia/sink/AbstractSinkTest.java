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

import org.apache.maven.doxia.AbstractModuleTest;
import org.apache.maven.doxia.parser.Parser;
import org.apache.maven.doxia.sink.Sink;

import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.Writer;

/**
 * Abstract base class to test sinks.
 *
 * @version $Id: AbstractSinkTest.java 567231 2007-08-18 04:50:31Z vsiveton $
 * @since 1.0
 */
public abstract class AbstractSinkTest
    extends AbstractModuleTest
{
    private final CharArrayWriter writer = new CharArrayWriter();
    private Sink sink;

    /**
     * Resets the writer and creates a new sink with it.
     *
     * {@inheritDoc}
     */
    protected void setUp()
        throws Exception
    {
        super.setUp();

        writer.reset();
        sink = createSink( writer );
    }

    // ---------------------------------------------------------------------
    // Common test cases
    // ----------------------------------------------------------------------

    /**
     * Tests that the current sink is able to render the common test document.
     * @see SinkTestDocument
     * @throws IOException If the target test document could not be generated.
     */
    public final void testTestDocument() throws IOException
    {
        Sink testSink = createSink( getTestWriter( "testDocument" ) );

        SinkTestDocument.generate( testSink );

        testSink.close();
    }

    /**
     * Checks that the sequence <code>[title(), text( title ), title_()]</code>,
     * invoked on the current sink, produces the same result as
     * {@link #getTitleBlock getTitleBlock}( title ).
     * NewLines are ignored.
     */
    public void testTitle()
    {
        String title = "Grodek";
        sink.title();
        sink.text( title );
        sink.title_();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong title!",
            expected, noNewLine( getTitleBlock( title ) ) );
    }

    /**
     * Checks that the sequence <code>[author(), text( author ), author_()]
     * </code>, invoked on the current sink, produces the same result as
     * {@link #getAuthorBlock getAuthorBlock}( author ).
     * NewLines are ignored.
     */
    public void testAuthor()
    {
        String author = "Georg Trakl";
        sink.author();
        sink.text( author );
        sink.author_();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong author!",
            expected, noNewLine( getAuthorBlock( author ) ) );
    }

    /**
     * Checks that the sequence <code>[date(), text( date ), date_()]</code>,
     * invoked on the current sink, produces the same result as
     * {@link #getDateBlock getDateBlock}( date ). NewLines are ignored.
     */
    public void testDate()
    {
        String date = "1914";
        sink.date();
        sink.text( date );
        sink.date_();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong date!",
            expected, noNewLine( getDateBlock( date ) ) );
    }

    /**
     * Checks that the sequence <code>[head(), head_()]</code>,
     * invoked on the current sink, produces the same result as
     * {@link #getHeadBlock getHeadBlock()}. NewLines are ignored.
     */
    public void testHead()
    {
        sink.head();
        sink.head_();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong head!", expected, noNewLine( getHeadBlock() ) );
    }

    /**
     * Checks that the sequence <code>[body(), body_()]</code>,
     * invoked on the current sink, produces the same result as
     * {@link #getBodyBlock getBodyBlock()}. NewLines are ignored.
     */
    public void testBody()
    {
        sink.body();
        sink.body_();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong body!", expected, noNewLine( getBodyBlock() ) );
    }

    /**
     * Checks that the sequence <code>[sectionTitle(), text( title ),
     * sectionTitle_()]</code>, invoked on the current sink, produces
     * the same result as
     * {@link #getSectionTitleBlock getSectionTitleBlock}( title ).
     * NewLines are ignored.
     */
    public void testSectionTitle()
    {
        String title = "Title";
        sink.sectionTitle();
        sink.text( title );
        sink.sectionTitle_();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong sectionTitle!",
            expected, noNewLine( getSectionTitleBlock( title ) ) );
    }

    /**
     * Checks that the sequence <code>[section1(), sectionTitle1(),
     * text( title ), sectionTitle1_(), section1_()]</code>,
     * invoked on the current sink, produces the same result as
     * {@link #getSection1Block getSection1Block}( title ).
     * NewLines are ignored.
     */
    public void testSection1()
    {
        String title = "Title1";
        sink.section1();
        sink.sectionTitle1();
        sink.text( title );
        sink.sectionTitle1_();
        sink.section1_();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong section1 block!",
            expected, noNewLine( getSection1Block( title ) ) );
    }

    /**
     * Checks that the sequence <code>[section2(), sectionTitle2(),
     * text( title ), sectionTitle2_(), section2_()]</code>,
     * invoked on the current sink, produces the same result as
     * {@link #getSection2Block getSection2Block}( title ).
     * NewLines are ignored.
     */
    public void testSection2()
    {
        String title = "Title2";
        sink.section2();
        sink.sectionTitle2();
        sink.text( title );
        sink.sectionTitle2_();
        sink.section2_();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong section2 block!",
            expected, noNewLine( getSection2Block( title ) ) );
    }

    /**
     * Checks that the sequence <code>[section3(), sectionTitle3(),
     * text( title ), sectionTitle3_(), section3_()]</code>,
     * invoked on the current sink, produces the same result as
     * {@link #getSection3Block getSection3Block}( title ).
     * NewLines are ignored.
     */
    public void testSection3()
    {
        String title = "Title3";
        sink.section3();
        sink.sectionTitle3();
        sink.text( title );
        sink.sectionTitle3_();
        sink.section3_();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong section3 block!",
            expected, noNewLine( getSection3Block( title ) ) );
    }

    /**
     * Checks that the sequence <code>[section4(), sectionTitle4(),
     * text( title ), sectionTitle4_(), section4_()]</code>,
     * invoked on the current sink, produces the same result as
     * {@link #getSection4Block getSection4Block}( title ).
     * NewLines are ignored.
     */
    public void testSection4()
    {
        String title = "Title4";
        sink.section4();
        sink.sectionTitle4();
        sink.text( title );
        sink.sectionTitle4_();
        sink.section4_();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong section4 block!",
            expected, noNewLine( getSection4Block( title ) ) );
    }

    /**
     * Checks that the sequence <code>[section5(), sectionTitle5(),
     * text( title ), sectionTitle5_(), section5_()]</code>,
     * invoked on the current sink, produces the same result as
     * {@link #getSection5Block getSection5Block}( title ).
     * NewLines are ignored.
     */
    public void testSection5()
    {
        String title = "Title5";
        sink.section5();
        sink.sectionTitle5();
        sink.text( title );
        sink.sectionTitle5_();
        sink.section5_();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong section5 block!",
            expected, noNewLine( getSection5Block( title ) ) );
    }

    /**
     * Checks that the sequence <code>[list(), listItem(), text( item ),
     * listItem_(), list_()]</code>, invoked on the current sink, produces
     * the same result as {@link #getListBlock getListBlock}( item ).
     * NewLines are ignored.
     */
    public void testList()
    {
        String item = "list item";
        sink.list();
        sink.listItem();
        sink.text( item );
        sink.listItem_();
        sink.list_();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong list!",
            expected, noNewLine( getListBlock( item ) ) );
    }

    /**
     * Checks that the sequence <code>
     * [numberedList( Sink.NUMBERING_LOWER_ROMAN ), numberedListItem(),
     * text( item ), numberedListItem_(), numberedList_()]</code>,
     * invoked on the current sink, produces the same result as
     * {@link #getNumberedListBlock getNumberedListBlock}( item ).
     * NewLines are ignored.
     */
    public void testNumberedList()
    {
        String item = "numbered list item";
        sink.numberedList( Sink.NUMBERING_LOWER_ROMAN );
        sink.numberedListItem();
        sink.text( item );
        sink.numberedListItem_();
        sink.numberedList_();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong numbered list!",
            expected, noNewLine( getNumberedListBlock( item ) ) );
    }

    /**
     * Checks that the sequence <code>[definitionList(), definitionListItem(),
     * definedTerm(), text( definum ), definedTerm_(), definition(),
     * text( definition ), definition_(), definitionListItem_(),
     * definitionList_()]</code>, invoked on the current sink, produces the same
     * result as {@link #getDefinitionListBlock getDefinitionListBlock}
     * ( definum, definition ). NewLines are ignored.
     */
    public void testDefinitionList()
    {
        String definum = "definum";
        String definition = "definition";
        sink.definitionList();
        sink.definitionListItem();
        sink.definedTerm();
        sink.text( definum );
        sink.definedTerm_();
        sink.definition();
        sink.text( definition );
        sink.definition_();
        sink.definitionListItem_();
        sink.definitionList_();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong definition list!", expected,
            noNewLine( getDefinitionListBlock( definum, definition ) ) );
    }

    /**
     * Checks that the sequence <code>[figure(), figureGraphics( source ),
     * figureCaption(), text( caption ), figureCaption_(), figure_()]</code>,
     * invoked on the current sink, produces the same result as
     * {@link #getFigureBlock getFigureBlock}( source, caption ).
     * NewLines are ignored.
     */
    public void testFigure()
    {
        String source = "figure";
        String caption = "Figure caption";
        sink.figure();
        sink.figureGraphics( source );
        sink.figureCaption();
        sink.text( caption );
        sink.figureCaption_();
        sink.figure_();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong figure!", expected,
            noNewLine( getFigureBlock( source, caption ) ) );
    }

    /**
     * Checks that the sequence <code>[table(),
     * tableRows( Parser.JUSTIFY_CENTER, false ), tableRow(), tableCell(),
     * text( cell ), tableCell_(), tableRow_(), tableRows_(), tableCaption(),
     * text( caption ), tableCaption_(), table_()]</code>,
     * invoked on the current sink, produces the same result as
     * {@link #getTableBlock getTableBlock}( cell, caption ).
     * NewLines are ignored.
     */
    public void testTable()
    {
        String cell = "cell";
        String caption = "Table caption";
        int[] justify = { Parser.JUSTIFY_CENTER };
        sink.table();
        sink.tableRows( justify, false );
        sink.tableRow();
        sink.tableCell();
        sink.text( cell );
        sink.tableCell_();
        sink.tableRow_();
        sink.tableRows_();
        sink.tableCaption();
        sink.text( caption );
        sink.tableCaption_();
        sink.table_();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong table!", expected,
            noNewLine( getTableBlock( cell, caption ) ) );
    }

    /**
     * Checks that the sequence <code>[paragraph(), text( text ),
     * paragraph_()]</code>, invoked on the current sink, produces
     * the same result as {@link #getParagraphBlock getParagraphBlock}( text ).
     * NewLines are ignored.
     */
    public void testParagraph()
    {
        String text = "Text";
        sink.paragraph();
        sink.text( text );
        sink.paragraph_();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong paragraph!",
            expected, noNewLine( getParagraphBlock( text ) ) );
    }

    /**
     * Checks that the sequence <code>[verbatim( true ), text( text ),
     * verbatim_()]</code>, invoked on the current sink, produces the
     * same result as {@link #getVerbatimBlock getVerbatimBlock}( text ).
     * NewLines are ignored.
     */
    public void testVerbatim()
    {
        String text = "Text";
        sink.verbatim( true );
        sink.text( text );
        sink.verbatim_();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong verbatim!",
            expected, noNewLine( getVerbatimBlock( text ) ) );
    }

    /**
     * Checks that the sequence <code>[horizontalRule()]</code>,
     * invoked on the current sink, produces the same result as
     * {@link #getHorizontalRuleBlock getHorizontalRuleBlock()}.
     * NewLines are ignored.
     */
    public void testHorizontalRule()
    {
        sink.horizontalRule();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong horizontal rule!",
            expected, noNewLine( getHorizontalRuleBlock() ) );
    }

    /**
     * Checks that the sequence <code>[pageBreak()]</code>,
     * invoked on the current sink, produces the same result as
     * {@link #getPageBreakBlock getPageBreakBlock()}. NewLines are ignored.
     */
    public void testPageBreak()
    {
        sink.pageBreak();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong pageBreak!",
            expected, noNewLine( getPageBreakBlock() ) );
    }

    /**
     * Checks that the sequence <code>[anchor( anchor ), text( anchor ),
     * anchor_()]</code>, invoked on the current sink, produces the same
     * result as {@link #getAnchorBlock getAnchorBlock}( anchor ).
     * NewLines are ignored.
     */
    public void testAnchor()
    {
        String anchor = "Anchor";
        sink.anchor( anchor );
        sink.text( anchor );
        sink.anchor_();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong anchor!",
            expected, noNewLine( getAnchorBlock( anchor ) ) );
    }

    /**
     * Checks that the sequence <code>[link( link ), text( text ),
     * link_()]</code>, invoked on the current sink, produces the same
     * result as {@link #getLinkBlock getLinkBlock}( link, text ).
     * NewLines are ignored.
     */
    public void testLink()
    {
        String link = "Link";
        String text = "Text";
        sink.link( link );
        sink.text( text );
        sink.link_();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong link!",
            expected, noNewLine( getLinkBlock( link, text ) ) );
    }

    /**
     * Checks that the sequence <code>[italic(), text( text ), italic_()]</code>,
     * invoked on the current sink, produces the same result as
     * {@link #getItalicBlock getItalicBlock}( text ). NewLines are ignored.
     */
    public void testItalic()
    {
        String text = "Italic";
        sink.italic();
        sink.text( text );
        sink.italic_();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong italic!",
            expected, noNewLine( getItalicBlock( text ) ) );
    }

    /**
     * Checks that the sequence <code>[bold(), text( text ), bold_()]</code>,
     * invoked on the current sink, produces the same result as
     * {@link #getBoldBlock getBoldBlock}( text ). NewLines are ignored.
     */
    public void testBold()
    {
        String text = "Bold";
        sink.bold();
        sink.text( text );
        sink.bold_();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong bold!",
            expected, noNewLine( getBoldBlock( text ) ) );
    }

    /**
     * Checks that the sequence <code>[monospaced(), text( text ),
     * monospaced_()]</code>, invoked on the current sink, produces the same
     * result as {@link #getMonospacedBlock getMonospacedBlock}( text ).
     * NewLines are ignored.
     */
    public void testMonospaced()
    {
        String text = "Monospaced";
        sink.monospaced();
        sink.text( text );
        sink.monospaced_();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong monospaced!",
            expected, noNewLine( getMonospacedBlock( text ) ) );
    }

    /**
     * Checks that the sequence <code>[lineBreak()]</code>,
     * invoked on the current sink, produces the same result as
     * {@link #getLineBreakBlock getLineBreakBlock()}. NewLines are ignored.
     */
    public void testLineBreak()
    {
        sink.lineBreak();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong lineBreak!",
            expected, noNewLine( getLineBreakBlock() ) );
    }

    /**
     * Checks that the sequence <code>[nonBreakingSpace()]</code>,
     * invoked on the current sink, produces the same result as
     * {@link #getNonBreakingSpaceBlock getNonBreakingSpaceBlock()}.
     * NewLines are ignored.
     */
    public void testNonBreakingSpace()
    {
        sink.nonBreakingSpace();
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong nonBreakingSpace!",
            expected, noNewLine( getNonBreakingSpaceBlock() ) );
    }

    /**
     * Checks that the sequence <code>[text( text )]</code>,
     * invoked on the current sink, produces the same result as
     * {@link #getTextBlock getTextBlock()}. NewLines are ignored.
     */
    public void testText()
    {
        String text = "~, =, -, +, *, [, ], <, >, {, }, \\";
        sink.text( text );
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong text!",
            expected, noNewLine( getTextBlock( text ) ) );
    }

    /**
     * Checks that the sequence <code>[rawText( text )]</code>,
     * invoked on the current sink, produces the same result as
     * {@link #getRawTextBlock getRawTextBlock}( text ). NewLines are ignored.
     */
    public void testRawText()
    {
        String text = "~, =, -, +, *, [, ], <, >, {, }, \\";
        sink.rawText( text );
        sink.flush();

        String expected = noNewLine( writer.toString() );
        assertEquals( "Wrong rawText!",
            expected, noNewLine( getRawTextBlock( text ) ) );
    }


    // ----------------------------------------------------------------------
    // Utility methods
    // ----------------------------------------------------------------------

    protected String noNewLine( String text )
    {
        String EOL = System.getProperty( "line.separator" );
        return text.replaceAll( EOL, "" );
    }

    /**
     * Returns the sink that is currently being tested.
     * @return The current test sink.
     */
    protected Sink getSink()
    {
        return sink;
    }

    /**
     * Returns the directory where all sink test output will go.
     * @return The test output directory.
     */
    protected String getOutputDir()
    {
        return "sink/";
    }


    // ----------------------------------------------------------------------
    // Abstract methods the individual SinkTests must provide
    // ----------------------------------------------------------------------

    /**
     * Return a new instance of the sink that is being tested.
     * @param writer The writer for the sink.
     * @return A new sink.
     */
    protected abstract Sink createSink( Writer writer );

    /**
     * Returns a title block generated by this sink.
     * @param title The title to use.
     * @return The result of invoking a title block on the current sink.
     * @see #testTitle()
     */
    protected abstract String getTitleBlock( String title );

    /**
     * Returns an author block generated by this sink.
     * @param author The author to use.
     * @return The result of invoking an author block on the current sink.
     * @see #testAuthor()
     */
    protected abstract String getAuthorBlock( String author );

    /**
     * Returns a date block generated by this sink.
     * @param date The date to use.
     * @return The result of invoking a date block on the current sink.
     * @see #testDate()
     */
    protected abstract String getDateBlock( String date );

    /**
     * Returns a head block generated by this sink.
     * @return The result of invoking a head block on the current sink.
     * @see #testHead()
     */
    protected abstract String getHeadBlock();

    /**
     * Returns a body block generated by this sink.
     * @return The result of invoking a body block on the current sink.
     * @see #testBody()
     */
    protected abstract String getBodyBlock();

    /**
     * Returns a SectionTitle block generated by this sink.
     * @param title The title to use.
     * @return The result of invoking a SectionTitle block on the current sink.
     * @see #testSectionTitle()
     */
    protected abstract String getSectionTitleBlock( String title );

    /**
     * Returns a Section1 block generated by this sink.
     * @param title The title to use.
     * @return The result of invoking a Section1 block on the current sink.
     * @see #testSection1()
     */
    protected abstract String getSection1Block( String title );

    /**
     * Returns a Section2 block generated by this sink.
     * @param title The title to use.
     * @return The result of invoking a Section2 block on the current sink.
     * @see #testSection2()
     */
    protected abstract String getSection2Block( String title );

    /**
     * Returns a Section3 block generated by this sink.
     * @param title The title to use.
     * @return The result of invoking a Section3 block on the current sink.
     * @see #testSection3()
     */
    protected abstract String getSection3Block( String title );

    /**
     * Returns a Section4 block generated by this sink.
     * @param title The title to use.
     * @return The result of invoking a Section4 block on the current sink.
     * @see #testSection4()
     */
    protected abstract String getSection4Block( String title );

    /**
     * Returns a Section5 block generated by this sink.
     * @param title The title to use.
     * @return The result of invoking a Section5 block on the current sink.
     * @see #testSection5()
     */
    protected abstract String getSection5Block( String title );

    /**
     * Returns a list block generated by this sink.
     * @param item The item to use.
     * @return The result of invoking a list block on the current sink.
     * @see #testList()
     */
    protected abstract String getListBlock( String item );

    /**
     * Returns a NumberedList block generated by this sink.
     * @param item The item to use.
     * @return The result of invoking a NumberedList block on the current sink.
     * @see #testNumberedList()
     */
    protected abstract String getNumberedListBlock( String item );

    /**
     * Returns a DefinitionList block generated by this sink.
     * @param definum The term to define.
     * @param definition The definition.
     * @return The result of invoking a DefinitionList block on the current sink.
     * @see #testDefinitionList()
     */
    protected abstract String getDefinitionListBlock( String definum,
        String definition );

    /**
     * Returns a Figure block generated by this sink.
     * @param source The figure source string.
     * @param caption The caption to use (may be null).
     * @return The result of invoking a Figure block on the current sink.
     * @see #testFigure()
     */
    protected abstract String getFigureBlock( String source, String caption );

    /**
     * Returns a Table block generated by this sink.
     * @param cell A tabel cell to use.
     * @param caption The caption to use (may be null).
     * @return The result of invoking a Table block on the current sink.
     * @see #testTable()
     */
    protected abstract String getTableBlock( String cell, String caption );

    /**
     * Returns a Paragraph block generated by this sink.
     * @param text The text to use.
     * @return The result of invoking a Paragraph block on the current sink.
     * @see #testParagraph()
     */
    protected abstract String getParagraphBlock( String text );

    /**
     * Returns a Verbatim block generated by this sink.
     * @param text The text to use.
     * @return The result of invoking a Verbatim block on the current sink.
     * @see #testVerbatim()
     */
    protected abstract String getVerbatimBlock( String text );

    /**
     * Returns a HorizontalRule block generated by this sink.
     * @return The result of invoking a HorizontalRule block on the current sink.
     * @see #testHorizontalRule()
     */
    protected abstract String getHorizontalRuleBlock();

    /**
     * Returns a PageBreak block generated by this sink.
     * @return The result of invoking a PageBreak block on the current sink.
     * @see #testPageBreak()
     */
    protected abstract String getPageBreakBlock();

    /**
     * Returns a Anchor block generated by this sink.
     * @param anchor The anchor to use.
     * @return The result of invoking a Anchor block on the current sink.
     * @see #testAnchor()
     */
    protected abstract String getAnchorBlock( String anchor );

    /**
     * Returns a Link block generated by this sink.
     * @param link The link to use.
     * @param text The link text.
     * @return The result of invoking a Link block on the current sink.
     * @see #testLink()
     */
    protected abstract String getLinkBlock( String link, String text );

    /**
     * Returns a Italic block generated by this sink.
     * @param text The text to use.
     * @return The result of invoking a Italic block on the current sink.
     * @see #testItalic()
     */
    protected abstract String getItalicBlock( String text );

    /**
     * Returns a Bold block generated by this sink.
     * @param text The text to use.
     * @return The result of invoking a Bold block on the current sink.
     * @see #testBold()
     */
    protected abstract String getBoldBlock( String text );

    /**
     * Returns a Monospaced block generated by this sink.
     * @param text The text to use.
     * @return The result of invoking a Monospaced block on the current sink.
     * @see #testMonospaced()
     */
    protected abstract String getMonospacedBlock( String text );

    /**
     * Returns a LineBreak block generated by this sink.
     * @return The result of invoking a LineBreak block on the current sink.
     * @see #testLineBreak()
     */
    protected abstract String getLineBreakBlock();

    /**
     * Returns a NonBreakingSpace block generated by this sink.
     * @return The result of invoking a NonBreakingSpace block
     * on the current sink.
     * @see #testNonBreakingSpace()
     */
    protected abstract String getNonBreakingSpaceBlock();

    /**
     * Returns a Text block generated by this sink.
     * @param text The text to use.
     * @return The result of invoking a Text block on the current sink.
     * @see #testText()
     */
    protected abstract String getTextBlock( String text );

    /**
     * Returns a RawText block generated by this sink.
     * @param text The text to use.
     * @return The result of invoking a RawText block on the current sink.
     * @see #testRawText()
     */
    protected abstract String getRawTextBlock( String text );

}
