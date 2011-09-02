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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.maven.doxia.util.ByLineSource;
import org.apache.maven.doxia.parser.ParseException;


/**
 * Parse looking for sections
 *
 * @author Juan F. Codagnone
 * @since Nov 1, 2005
 */
public class SectionBlockParser implements BlockParser
{
    /**
     * '---++ Header', '---## Header'
     */
    private static final Pattern HEADER_DA = Pattern
        .compile( "^---([+]+)\\s*(.+)\\s*$" );

    /**
     * {@link ParagraphBlockParser} to use. injected
     */
    private ParagraphBlockParser paraParser;
    /**
     * {@link ParagraphBlockParser} to use. injected
     */
    private HRuleBlockParser hrulerParser;

    /**
     * @see BlockParser#accept(String)
     */
    public final boolean accept( final String s )
    {

        return HEADER_DA.matcher( s ).lookingAt();
    }

    /**
     * @see BlockParser#visit(String)
     */
    public final Block visit( final String line, final ByLineSource source )
        throws ParseException
    {
        final Matcher m = HEADER_DA.matcher( line );

        if ( !m.lookingAt() )
        {
            throw new IllegalArgumentException( "don't know how to handle: "
                + line );
        }

        String newLine;
        final ArrayList blocks = new ArrayList();

        while ( ( newLine = source.getNextLine() ) != null && !accept( newLine ) )
        {
            if ( hrulerParser.accept( newLine ) )
            {
                blocks.add( hrulerParser.visit( newLine, source ) );
            }
            else
            {
                blocks.add( paraParser.visit( newLine, source ) );
            }
        }

        if ( newLine != null )
        {
            source.ungetLine();
        }

        return new SectionBlock( m.group( 2 ), getLevel( m.group( 1 ) ),
                                 (Block[]) blocks.toArray( new Block[]{} ) );
    }

    /**
     * @param s "++"
     * @return tha level of the section
     * @throws IllegalArgumentException on error
     */
    static int getLevel( final String s ) throws IllegalArgumentException
    {
        for ( int i = 0, n = s.length(); i < n; i++ )
        {
            if ( s.charAt( i ) != '+' )
            {
                throw new IllegalArgumentException( "the argument must have only"
                    + " '+' characters" );
            }
        }
        return s.length();
    }

    /**
     * Sets the paraParser.
     *
     * @param paraParser <code>ParagraphBlockParser</code> with the paraParser.
     */
    public final void setParaParser( final ParagraphBlockParser paraParser )
    {
        if ( paraParser == null )
        {
            throw new IllegalArgumentException( "argument can't be null" );
        }
        this.paraParser = paraParser;
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
            throw new IllegalArgumentException( "argument can't be null" );
        }
        this.hrulerParser = hrulerParser;
    }

}
