package org.apache.maven.doxia.module.twiki;

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

import org.apache.maven.doxia.module.twiki.parser.*;
import org.apache.maven.doxia.parser.AbstractTextParser;
import org.apache.maven.doxia.parser.ParseException;
import org.apache.maven.doxia.sink.Sink;
import org.apache.maven.doxia.util.ByLineReaderSource;
import org.apache.maven.doxia.util.ByLineSource;
import org.codehaus.plexus.util.IOUtil;

import java.io.Reader;
import java.io.StringWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Parse the <a href="http://twiki.org/cgi-bin/view/TWiki/TextFormattingRules">
 * twiki file format</a>
 *
 * @author Juan F. Codagnone
 * @version $Id: TWikiParser.java 564180 2007-08-09 12:15:44Z vsiveton $
 * @since 1.0
 * @plexus.component role="org.apache.maven.doxia.parser.Parser" role-hint="twiki"
 */
public class TWikiParser
    extends AbstractTextParser
{
    /**
     * paragraph parser. stateless
     */
    private final ParagraphBlockParser paraParser = new ParagraphBlockParser();
    /**
     * section parser. stateless
     */
    private final SectionBlockParser sectionParser = new SectionBlockParser();
    /**
     * enumeration parser. stateless
     */
    private final GenericListBlockParser listParser =
        new GenericListBlockParser();
    /**
     * Text parser. stateless
     */
    private final FormatedTextParser formatTextParser =
        new FormatedTextParser();
    /**
     * text parser. stateless
     */
    private final TextParser textParser = new TextParser();
    /**
     * hruler parser. stateless
     */
    private final HRuleBlockParser hrulerParser = new HRuleBlockParser();
    /**
     * table parser
     */
    private final TableBlockParser tableParser = new TableBlockParser();

    /**
     * list of parsers to try to apply to the toplevel
     */
    private final BlockParser [] parsers;

    /**
     * Creates the TWikiParser.
     */
    public TWikiParser()
    {
        paraParser.setSectionParser( sectionParser );
        paraParser.setListParser( listParser );
        paraParser.setTextParser( formatTextParser );
        paraParser.setHrulerParser( hrulerParser );
        paraParser.setTableBlockParser( tableParser );
        sectionParser.setParaParser( paraParser );
        sectionParser.setHrulerParser( hrulerParser );
        listParser.setTextParser( formatTextParser );
        formatTextParser.setTextParser( textParser );
        tableParser.setTextParser( formatTextParser );

        parsers = new BlockParser[]{
            sectionParser,
            hrulerParser,
            paraParser,
        };
    }

    /**
     * @param source source to parse
     * @return the blocks that represent source
     * @throws ParseException on error
     */
    public final List parse( final ByLineSource source )
        throws ParseException
    {
        final List ret = new ArrayList();

        String line;
        while ( ( line = source.getNextLine() ) != null )
        {
            boolean accepted = false;

            for ( int i = 0; i < parsers.length; i++ )
            {
                final BlockParser parser = parsers[i];

                if ( parser.accept( line ) )
                {
                    accepted = true;
                    ret.add( parser.visit( line, source ) );
                    break;
                }
            }
            if ( !accepted )
            {
                throw new ParseException( "don't  know how to handle line: "
                    + source.getLineNumber() + ": " + line );
            }
        }

        return ret;
    }

    /** {@inheritDoc} */
    public final synchronized void parse( final Reader reader, final Sink sink )
        throws ParseException
    {

        String sourceContent;
        try
        {
            StringWriter contentWriter = new StringWriter();
            IOUtil.copy(reader, contentWriter );
            sourceContent = contentWriter.toString();
        }
        catch ( IOException e )
        {
            throw new ParseException( "IOException: " + e.getMessage(), e );
        }


        textParser.setSourceContent(sourceContent);
        textParser.setParent(this);
        State.init();

        List blocks;
        final ByLineSource source = new StringLineSource( sourceContent );

        try
        {
            blocks = parse( source );
        }
        catch ( final ParseException e )
        {
            throw e;
        }
        catch ( final Exception e )
        {
            throw new ParseException( e, source.getName(),
                                      source.getLineNumber() );
        }

        SectionBlock.clearLevelStack();
        sink.head();
        sink.head_();
        sink.body();
        for ( Iterator it = blocks.iterator(); it.hasNext(); )
        {
            final Block block = (Block) it.next();

            block.traverse( sink );
        }
        SectionBlock.closeAllSections(sink);
        sink.body_();

    }



    public boolean isSecondParsing() {
        return secondParsing;
    }

}
