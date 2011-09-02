package org.apache.maven.doxia;

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

import org.apache.maven.doxia.sink.Sink;

import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

/**
 * This sink is used for testing purposes in order to check wether
 * the input of some parser is well-formed.
 *
 * @author <a href="mailto:lars@trieloff.net">Lars Trieloff</a>
 * @version $Id: WellformednessCheckingSink.java 496703 2007-01-16 14:27:31Z vsiveton $
 */
public class WellformednessCheckingSink
    implements Sink
{
    private Stack elements = new Stack();

    private List errors = new LinkedList();

    public void head()
    {
        startElement( "head" );
    }

    public void head_()
    {
        checkWellformedness( "head" );
    }

    public void body()
    {
        startElement( "body" );
    }

    public void body_()
    {
        checkWellformedness( "body" );

    }

    public void section1()
    {
        startElement( "section1" );
    }

    public void section1_()
    {
        checkWellformedness( "section1" );

    }

    public void section2()
    {
        startElement( "section2" );
    }

    public void section2_()
    {
        checkWellformedness( "section2" );

    }

    public void section3()
    {
        startElement( "section3" );
    }

    public void section3_()
    {
        checkWellformedness( "section3" );

    }

    public void section4()
    {
        startElement( "section4" );
    }

    public void section4_()
    {
        checkWellformedness( "section4" );

    }

    public void section5()
    {
        startElement( "section5" );
    }

    public void section5_()
    {
        checkWellformedness( "section5" );

    }

    public void list()
    {
        startElement( "list" );
    }

    public void list_()
    {
        checkWellformedness( "list" );

    }

    public void listItem()
    {
        startElement( "listItem" );
    }

    public void listItem_()
    {
        checkWellformedness( "listItem" );

    }

    public void numberedList( int numbering )
    {
        startElement( "numberedList" );

    }

    public void numberedList_()
    {
        checkWellformedness( "numberedList" );

    }

    public void numberedListItem()
    {
        startElement( "numberedListItem" );
    }

    public void numberedListItem_()
    {
        checkWellformedness( "numberedListItem" );

    }

    public void definitionList()
    {
        startElement( "definitionList" );

    }

    public void definitionList_()
    {
        checkWellformedness( "definitionList" );

    }

    public void definitionListItem()
    {
        startElement( "definitionListItem" );

    }

    public void definitionListItem_()
    {
        checkWellformedness( "definitionListItem" );

    }

    public void definition()
    {
        startElement( "definition" );

    }

    public void definition_()
    {
        checkWellformedness( "definition" );

    }

    public void figure()
    {
        startElement( "figure" );
    }

    public void figure_()
    {
        checkWellformedness( "figure" );

    }

    public void table()
    {
        startElement( "table" );
    }

    public void table_()
    {
        checkWellformedness( "table" );
    }

    public void tableRows( int[] justification, boolean grid )
    {
        startElement( "tableRows" );
    }

    public void tableRows_()
    {
        checkWellformedness( "tableRows" );

    }

    public void tableRow()
    {
        startElement( "tableRow" );
    }

    public void tableRow_()
    {
        checkWellformedness( "tableRow" );
    }

    public void title()
    {
        startElement( "title" );
    }

    public void title_()
    {
        checkWellformedness( "title" );
    }

    public void author()
    {
        startElement( "author" );
    }

    public void author_()
    {
        checkWellformedness( "author" );
    }

    public void date()
    {
        startElement( "date" );
    }

    public void date_()
    {
        checkWellformedness( "date" );
    }

    public void sectionTitle()
    {
        startElement( "sectionTitle" );
    }

    public void sectionTitle_()
    {
        checkWellformedness( "sectionTitle" );
    }

    public void sectionTitle1()
    {
        startElement( "sectionTitle1" );
    }

    public void sectionTitle1_()
    {
        checkWellformedness( "sectionTitle1" );
    }

    public void sectionTitle2()
    {
        startElement( "sectionTitle2" );
    }

    public void sectionTitle2_()
    {
        checkWellformedness( "sectionTitle2" );
    }

    public void sectionTitle3()
    {
        startElement( "sectionTitle3" );
    }

    public void sectionTitle3_()
    {
        checkWellformedness( "sectionTitle3" );
    }

    public void sectionTitle4()
    {
        startElement( "sectionTitle4" );
    }

    public void sectionTitle4_()
    {
        checkWellformedness( "sectionTitle4" );
    }

    public void sectionTitle5()
    {
        startElement( "sectionTitle5" );
    }

    public void sectionTitle5_()
    {
        checkWellformedness( "sectionTitle5" );
    }

    public void paragraph()
    {
        startElement( "paragraph" );
    }

    public void paragraph_()
    {
        checkWellformedness( "paragraph" );
    }

    public void verbatim( boolean boxed )
    {
        startElement( "verbatim" );
    }

    public void verbatim_()
    {
        checkWellformedness( "verbatim" );
    }

    public void definedTerm()
    {
        startElement( "definedTerm" );
    }

    public void definedTerm_()
    {
        checkWellformedness( "definedTerm" );
    }

    public void figureCaption()
    {
        startElement( "figureCaption" );
    }

    public void figureCaption_()
    {
        checkWellformedness( "figureCaption" );
    }

    public void tableCell()
    {
        startElement( "tableCell" );
    }

    public void tableCell( String width )
    {
        startElement( "tableCell" );
    }

    public void tableCell_()
    {
        checkWellformedness( "tableCell" );
    }

    public void tableHeaderCell()
    {
        startElement( "tableHeaderCell" );
    }

    public void tableHeaderCell( String width )
    {
        startElement( "tableHeaderCell" );
    }

    public void tableHeaderCell_()
    {
        checkWellformedness( "tableHeaderCell" );
    }

    public void tableCaption()
    {
        startElement( "tableCaption" );
    }

    public void tableCaption_()
    {
        checkWellformedness( "tableCaption" );
    }

    public void figureGraphics( String name )
    {
    }

    public void horizontalRule()
    {
    }

    public void pageBreak()
    {
    }

    public void anchor( String name )
    {
        startElement( "anchor" );
    }

    public void anchor_()
    {
        checkWellformedness( "anchor" );
    }

    public void link( String name )
    {
        startElement( "link" );
    }

    public void link_()
    {
        checkWellformedness( "link" );
    }

    public void italic()
    {
        startElement( "italic" );
    }

    public void italic_()
    {
        checkWellformedness( "italic" );
    }

    public void bold()
    {
        startElement( "bold" );
    }

    public void bold_()
    {
        checkWellformedness( "bold" );
    }

    public void monospaced()
    {
        startElement( "monospaced" );
    }

    public void monospaced_()
    {
        checkWellformedness( "monospaced" );
    }

    public void lineBreak()
    {
    }

    public void nonBreakingSpace()
    {
    }

    public void text( String text )
    {
    }

    public void rawText( String text )
    {
    }

    public void flush()
    {
    }

    public void close()
    {
    }

    /**
     * Finds out wether the wellformedness-contraints of the model have been
     * violated.
     *
     * @return false for non-wellformed models
     */
    public boolean isWellformed()
    {
        return errors.size() == 0;
    }

    /**
     * Gets the offending element that breaks the wellformedness as well
     * as the exepected element.
     *
     * @return the expected and acual elements
     */
    public String getOffender()
    {
        if ( isWellformed() )
        {
            return null;
        }

        return (String) errors.get( errors.size() - 1 );
    }

    /**
     * Gets the list of errors found during wellformedness-check
     *
     * @return a list of String of error messages
     */
    public List getOffenders()
    {
        return errors;
    }

    /**
     * Checks wether a newly encountered end-tag breaks the wellformedness
     * of the model.
     *
     * @param actual the local-name of the encountered element
     */
    private void checkWellformedness( String actual )
    {
        String expected = (String) elements.pop();

        if ( !expected.equals( actual ) )
        {
            errors.add( "Encountered closing: " + actual + ", expected " + expected );
        }
    }

    /**
     * Starts a new element and puts it on the stack in order to calculate
     * wellformedness of the model at a later point of time.
     *
     * @param string the local-name of the start-tag
     */
    private void startElement( String string )
    {
        elements.push( string );
    }
}
