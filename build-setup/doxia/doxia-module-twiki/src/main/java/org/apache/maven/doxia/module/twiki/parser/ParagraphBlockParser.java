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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.maven.doxia.util.ByLineSource;
import org.apache.maven.doxia.parser.ParseException;


/**
 * Parse paragraphs.
 *
 * @author Juan F. Codagnone
 * @since Nov 1, 2005
 */
public class ParagraphBlockParser implements BlockParser
{
    /**
     * pattern used to dectect end of paragraph
     */
    private final Pattern paragraphSeparator = Pattern.compile( "^(\\s*)$" );
    /**
     * {@link SectionBlockParser} to use. injected
     */
    private SectionBlockParser sectionParser;
    /**
     * {@link ListBlockParser} to use. injected
     */
    private GenericListBlockParser listParser;
    /**
     * {@link FormatedTextParser} to use. injected
     */
    private FormatedTextParser textParser;
    /**
     * {@link HRuleBlockParser} to use. injected
     */
    private HRuleBlockParser hrulerParser;
    /**
     * {@link TableBlockParser} to use. injected
     */
    private TableBlockParser tableBlockParser;

    /**
     * no operation block
     */
    private static final NopBlock NOP = new NopBlock();

    /**
     * @see BlockParser#accept(String)
     */
    public final boolean accept( final String line )
    {
        return (State.isVerbatimMode()) || (!sectionParser.accept( line ) && !hrulerParser.accept( line ));
    }

    /**
     * @see BlockParser#visit(String, ByLineSource)
     */
    public final List getBlocklist( final String line, final ByLineSource source, List childs )
        throws ParseException
    {
        StringBuffer sb = new StringBuffer();
        
        if(childs == null) {
            childs = new ArrayList();
        }
        
        boolean sawText = false;

        /*
        * 1. Skip begininig new lines
        * 2. Get the text, while \n\n is not found
        */
        String l = line;
        do
        {
            Matcher m = paragraphSeparator.matcher( l );

            if ( m.lookingAt() )
            {
                if ( !sawText )
                {
                    // ignore 
                }
                else
                {
                    break;
                }
            }
            else
            {
                sawText = true;

                /* be able to parse lists / enumerations */
                if ( listParser.accept( l ) )
                {
                    if ( sb.length() != 0 )
                    {
                        childs.addAll( Arrays.asList( textParser
                            .parse( sb.toString().trim() ) ) );
                        sb = new StringBuffer();
                    }
                    childs.add( listParser.visit( l, source ) );
                }
                else if ( tableBlockParser.accept( l ) && !State.isVerbatimMode())
                {
                    childs.add( tableBlockParser.visit( l, source ) );
                }
                else
                {
                    sb.append( l );
                    sb.append( "\n" );
                }
            }
        }
        while ( ( l = source.getNextLine() ) != null && accept( l ) );

        if ( line != null )
        {
            source.ungetLine();
        }

        if ( sb.length() != 0 )
        {
            if(State.isVerbatimMode()) {
                childs.addAll( Arrays.asList( textParser
                    .parse( sb.toString() ) ) );
            } else {
                childs.addAll( Arrays.asList( textParser
                    .parse( sb.toString().trim() ) ) );
            }
            sb = new StringBuffer();
        }

        if(State.isVerbatimMode()) {
            return getBlocklist( l, source, childs );
        } else {
            return childs;
        }

    }

    public final Block visit( final String line, final ByLineSource source)
        throws ParseException {

        List childs = getBlocklist(line, source, null);
        if ( childs.size() == 0 )
        {
            return NOP;
        }
        else
        {
            return new ParagraphBlock( (Block[]) childs.toArray( new Block[]{} ) );
        }

    }


    /**
     * Sets the sectionParser.
     *
     * @param sectionParser <code>SectionBlockParser</code> with the sectionParser.
     */
    public final void setSectionParser( final SectionBlockParser sectionParser )
    {
        if ( sectionParser == null )
        {
            throw new IllegalArgumentException( "arg can't be null" );
        }
        this.sectionParser = sectionParser;
    }


    /**
     * Sets the listParser.
     *
     * @param listParser <code>ListBlockParser</code> with the listParser.
     */
    public final void setListParser( final GenericListBlockParser listParser )
    {
        if ( listParser == null )
        {
            throw new IllegalArgumentException( "arg can't be null" );
        }

        this.listParser = listParser;
    }


    /**
     * Sets the formatTextParser.
     *
     * @param textParser <code>FormatedTextParser</code>
     *                   with the formatTextParser.
     */
    public final void setTextParser( final FormatedTextParser textParser )
    {
        if ( textParser == null )
        {
            throw new IllegalArgumentException( "arg can't be null" );
        }
        this.textParser = textParser;
    }


    /**
     * Sets the hrulerParser.
     *
     * @param hrulerParser <code>HRuleBlockParser</code> with the hrulerParser.
     */
    public final void setHrulerParser( final HRuleBlockParser hrulerParser )
    {
        if ( hrulerParser == null )
        {
            throw new IllegalArgumentException( "arg can't be null" );
        }

        this.hrulerParser = hrulerParser;
    }

    /**
     * @param tableBlockParser Table parser to use
     */
    public final void setTableBlockParser(
        final TableBlockParser tableBlockParser )
    {
        if ( tableBlockParser == null )
        {
            throw new IllegalArgumentException( "arg can't be null" );
        }

        this.tableBlockParser = tableBlockParser;
    }
}
