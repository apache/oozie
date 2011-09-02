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

import java.io.IOException;
import java.io.Writer;

/**
 * A simple text-based implementation of the <code>Sink</code> interface.
 * Useful for testing purposes.
 */
public class TextSink
    implements Sink
{

    /** System-dependent end-of-line string. */
    private static final String EOL = System.getProperty( "line.separator" );

    /** For writing the result. */
    private final Writer out;

    /** Constructor.
     * @param writer The writer for writing the result.
     */
    public TextSink( Writer writer )
    {
        this.out = writer;
    }


    /** {@inheritDoc} */
    public void head()
    {
        writeln( "begin:head" );
    }

    /** {@inheritDoc} */
    public void head_()
    {
        writeln( "end:head" );
    }

    /** {@inheritDoc} */
    public void body()
    {
        writeln( "begin:body" );
    }

    /** {@inheritDoc} */
    public void body_()
    {
        writeln( "end:body" );
    }

    /** {@inheritDoc} */
    public void section1()
    {
        write( "begin:section1" );
    }

    /** {@inheritDoc} */
    public void section1_()
    {
        writeln( "end:section1" );
    }

    /** {@inheritDoc} */
    public void section2()
    {
        write( "begin:section2" );
    }

    /** {@inheritDoc} */
    public void section2_()
    {
        writeln( "end:section2" );
    }

    /** {@inheritDoc} */
    public void section3()
    {
        write( "begin:section3" );
    }

    /** {@inheritDoc} */
    public void section3_()
    {
        writeln( "end:section3" );
    }

    /** {@inheritDoc} */
    public void section4()
    {
        write( "begin:section4" );
    }

    /** {@inheritDoc} */
    public void section4_()
    {
        writeln( "end:section4" );
    }

    /** {@inheritDoc} */
    public void section5()
    {
        write( "begin:section5" );
    }

    /** {@inheritDoc} */
    public void section5_()
    {
        writeln( "end:section5" );
    }

    /** {@inheritDoc} */
    public void list()
    {
        writeln( "begin:list" );
    }

    /** {@inheritDoc} */
    public void list_()
    {
        writeln( "end:list" );
    }

    /** {@inheritDoc} */
    public void listItem()
    {
        write( "begin:listItem" );
    }

    /** {@inheritDoc} */
    public void listItem_()
    {
        writeln( "end:listItem" );
    }

    /** {@inheritDoc} */
    public void numberedList( int numbering )
    {
        writeln( "begin:numberedList, numbering: " + numbering );
    }

    /** {@inheritDoc} */
    public void numberedList_()
    {
        writeln( "end:numberedList" );
    }

    /** {@inheritDoc} */
    public void numberedListItem()
    {
        write( "begin:numberedListItem" );
    }

    /** {@inheritDoc} */
    public void numberedListItem_()
    {
        writeln( "end:numberedListItem" );
    }

    /** {@inheritDoc} */
    public void definitionList()
    {
        writeln( "begin:definitionList" );
    }

    /** {@inheritDoc} */
    public void definitionList_()
    {
        writeln( "end:definitionList" );
    }

    /** {@inheritDoc} */
    public void definitionListItem()
    {
        write( "begin:definitionListItem" );
    }

    /** {@inheritDoc} */
    public void definitionListItem_()
    {
        writeln( "end:definitionListItem" );
    }

    /** {@inheritDoc} */
    public void definition()
    {
        write( "begin:definition" );
    }

    /** {@inheritDoc} */
    public void definition_()
    {
        writeln( "end:definition" );
    }

    /** {@inheritDoc} */
    public void figure()
    {
        write( "begin:figure" );
    }

    /** {@inheritDoc} */
    public void figure_()
    {
        writeln( "end:figure" );
    }

    /** {@inheritDoc} */
    public void table()
    {
        writeln( "begin:table" );
    }

    /** {@inheritDoc} */
    public void table_()
    {
        writeln( "end:table" );
    }

    /** {@inheritDoc} */
    public void tableRows( int[] justification, boolean grid )
    {
        writeln( "begin:tableRows" );
    }

    /** {@inheritDoc} */
    public void tableRows_()
    {
        writeln( "end:tableRows" );
    }

    /** {@inheritDoc} */
    public void tableRow()
    {
        write( "begin:tableRow" );
    }

    /** {@inheritDoc} */
    public void tableRow_()
    {
        writeln( "end:tableRow" );
    }

    /** {@inheritDoc} */
    public void title()
    {
        write( "begin:title" );
    }

    /** {@inheritDoc} */
    public void title_()
    {
        writeln( "end:title" );
    }

    /** {@inheritDoc} */
    public void author()
    {
        write( "begin:author" );
    }

    /** {@inheritDoc} */
    public void author_()
    {
        writeln( "end:author" );
    }

    /** {@inheritDoc} */
    public void date()
    {
        write( "begin:date" );
    }

    /** {@inheritDoc} */
    public void date_()
    {
        writeln( "end:date" );
    }

    /** {@inheritDoc} */
    public void sectionTitle()
    {
        write( "begin:sectionTitle" );
    }

    /** {@inheritDoc} */
    public void sectionTitle_()
    {
        writeln( "end:sectionTitle" );
    }

    /** {@inheritDoc} */
    public void sectionTitle1()
    {
        write( "begin:sectionTitle1" );
    }

    /** {@inheritDoc} */
    public void sectionTitle1_()
    {
        writeln( "end:sectionTitle1" );
    }

    /** {@inheritDoc} */
    public void sectionTitle2()
    {
        write( "begin:sectionTitle2" );
    }

    /** {@inheritDoc} */
    public void sectionTitle2_()
    {
        writeln( "end:sectionTitle2" );
    }

    /** {@inheritDoc} */
    public void sectionTitle3()
    {
        write( "begin:sectionTitle3" );
    }

    /** {@inheritDoc} */
    public void sectionTitle3_()
    {
        writeln( "end:sectionTitle3" );
    }

    /** {@inheritDoc} */
    public void sectionTitle4()
    {
        write( "begin:sectionTitle4" );
    }

    /** {@inheritDoc} */
    public void sectionTitle4_()
    {
        writeln( "end:sectionTitle4" );
    }

    /** {@inheritDoc} */
    public void sectionTitle5()
    {
        write( "begin:sectionTitle5" );
    }

    /** {@inheritDoc} */
    public void sectionTitle5_()
    {
        writeln( "end:sectionTitle5" );
    }

    /** {@inheritDoc} */
    public void paragraph()
    {
        write( "begin:paragraph" );
    }

    /** {@inheritDoc} */
    public void paragraph_()
    {
        writeln( "end:paragraph" );
    }

    /** {@inheritDoc} */
    public void verbatim( boolean boxed )
    {
        write( "begin:verbatim, boxed: " + boxed );
    }

    /** {@inheritDoc} */
    public void verbatim_()
    {
        writeln( "end:verbatim" );
    }

    /** {@inheritDoc} */
    public void definedTerm()
    {
        write( "begin:definedTerm" );
    }

    /** {@inheritDoc} */
    public void definedTerm_()
    {
        writeln( "end:definedTerm" );
    }

    /** {@inheritDoc} */
    public void figureCaption()
    {
        write( "begin:figureCaption" );
    }

    /** {@inheritDoc} */
    public void figureCaption_()
    {
        writeln( "end:figureCaption" );
    }

    /** {@inheritDoc} */
    public void tableCell()
    {
        write( "begin:tableCell" );
    }

    /** {@inheritDoc} */
    public void tableCell( String width )
    {
        write( "begin:tableCell, width: " + width );
    }

    /** {@inheritDoc} */
    public void tableCell_()
    {
        writeln( "end:tableCell" );
    }

    /** {@inheritDoc} */
    public void tableHeaderCell()
    {
        write( "begin:tableHeaderCell" );
    }

    /** {@inheritDoc} */
    public void tableHeaderCell( String width )
    {
        write( "begin:tableHeaderCell, width: " + width );
    }

    /** {@inheritDoc} */
    public void tableHeaderCell_()
    {
        writeln( "end:tableHeaderCell" );
    }

    /** {@inheritDoc} */
    public void tableCaption()
    {
        write( "begin:tableCaption" );
    }

    /** {@inheritDoc} */
    public void tableCaption_()
    {
        writeln( "end:tableCaption" );
    }

    /** {@inheritDoc} */
    public void figureGraphics( String name )
    {
        write( "figureGraphics, name: " + name );
    }

    /** {@inheritDoc} */
    public void horizontalRule()
    {
        write( "horizontalRule" );
    }

    /** {@inheritDoc} */
    public void pageBreak()
    {
        write( "pageBreak" );
    }

    /** {@inheritDoc} */
    public void anchor( String name )
    {
        write( "begin:anchor, name: " + name  );
    }

    /** {@inheritDoc} */
    public void anchor_()
    {
        writeln( "end:anchor" );
    }

    /** {@inheritDoc} */
    public void link( String name )
    {
        write( "begin:link, name: " + name  );
    }

    /** {@inheritDoc} */
    public void link_()
    {
        writeln( "end:link" );
    }

    /** {@inheritDoc} */
    public void italic()
    {
        write( "begin:italic" );
    }

    /** {@inheritDoc} */
    public void italic_()
    {
        writeln( "end:italic" );
    }

    /** {@inheritDoc} */
    public void bold()
    {
        write( "begin:bold" );
    }

    /** {@inheritDoc} */
    public void bold_()
    {
        writeln( "end:bold" );
    }

    /** {@inheritDoc} */
    public void monospaced()
    {
        write( "begin:monospaced" );
    }

    /** {@inheritDoc} */
    public void monospaced_()
    {
        writeln( "end:monospaced" );
    }

    /** {@inheritDoc} */
    public void lineBreak()
    {
        write( "lineBreak" );
    }

    /** {@inheritDoc} */
    public void nonBreakingSpace()
    {
        write( "nonBreakingSpace" );
    }

    /** {@inheritDoc} */
    public void text( String text )
    {
        write( "text: " + text );
    }

    /** {@inheritDoc} */
    public void rawText( String text )
    {
        write( "rawText: " + text );
    }

    /** {@inheritDoc} */
    public void flush()
    {
        try
        {
            out.flush();
        }
        catch ( IOException e )
        {
            // TODO: log
        }
    }

    /** {@inheritDoc} */
    public void close()
    {
        try
        {
            out.close();
        }
        catch ( IOException e )
        {
            // TODO: log
        }
    }


    /**
     * Writes the given string + EOL.
     *
     * @param text The text to write.
     */
    private void write( String text )
    {
        try
        {
            out.write( text + EOL );
        }
        catch ( IOException e )
        {
            // TODO: log
        }
    }

    /**
     * Writes the given string + two EOLs.
     *
     * @param text The text to write.
     */
    private void writeln( String text )
    {
        write( text );
        write( EOL );
    }


}
