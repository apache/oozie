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
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.maven.doxia.util.ByLineSource;
import org.apache.maven.doxia.parser.ParseException;


/**
 * Parse tables
 *
 * @author Juan F. Codagnone
 * @since Nov 9, 2005
 */
public class TableBlockParser implements BlockParser
{
    /**
     * pattern to detect tables
     */
    private static final Pattern TABLE_PATTERN =
        Pattern.compile( "^\\s*([|].*[|])+\\s*$" );
    /**
     * text parser
     */
    private FormatedTextParser textParser;

    /**
     * @see BlockParser#accept(String)
     */
    public final boolean accept( final String line )
    {
        return TABLE_PATTERN.matcher( line ).lookingAt();
    }

    /**
     * @see BlockParser#visit(String, ByLineSource)
     */
    public final Block visit( final String line, final ByLineSource source )
        throws ParseException
    {

        if ( !accept( line ) )
        {
            throw new IllegalAccessError( "call accept before this ;)" );
        }

        final List rows = new ArrayList();
        String l = line;

        do
        {
            final Matcher m = TABLE_PATTERN.matcher( l );
            if ( m.lookingAt() )
            {
                final List cells = new ArrayList();

                /* for each cell... */
                for ( int lh = l.indexOf( '|' ) + 1, rh;
                      ( rh = l.indexOf( '|', lh ) ) != -1;
                      lh = rh + 1 )
                {
                    final Block [] bs = textParser.parse( l.substring( lh, rh )
                        .trim() );
                    if ( bs.length == 1 && bs[0] instanceof BoldBlock )
                    {
                        final Block []tmp = ( (BoldBlock) bs[0] ).getBlocks();
                        cells.add( new TableCellHeaderBlock( tmp ) );
                    }
                    else
                    {
                        cells.add( new TableCellBlock( bs ) );
                    }
                }
                rows.add( new TableRowBlock( (Block[]) cells.toArray( new Block[]{} ) ) );
            }

        }
        while ( ( l = source.getNextLine() ) != null && accept( l ) );

        assert rows.size() >= 1;

        return new TableBlock( (Block[]) rows.toArray( new Block[]{} ) );
    }


    /**
     * @param textParser text parser to be set
     */
    public final void setTextParser( final FormatedTextParser textParser )
    {
        if ( textParser == null )
        {
            throw new IllegalArgumentException( "argument can't be null" );
        }

        this.textParser = textParser;
    }

}
