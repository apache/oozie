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

import java.util.*;


/**
 * Parse looking for formated text (bold, italic, ...)
 *
 * @author Juan F. Codagnone
 * @since Nov 2, 2005
 */
public class FormatedTextParser
{
    /**
     * parser used to parse text...
     */
    private TextParser textParser;

    /**
     * map used to create blocks dependening on the text format
     */
    private static final Map FACTORY_MAP = new HashMap();

    /**
     * creates bold blocks
     */
    private static final FormatBlockFactory BOLD_FACTORY =
        new FormatBlockFactory()
        {
            public Block createBlock( final Block [] childrens )
            {
                return new BoldBlock( childrens );
            }
        };
    /**
     * creates italic blocks
     */
    private static final FormatBlockFactory ITALIC_FACTORY =
        new FormatBlockFactory()
        {
            public Block createBlock( final Block [] childrens )
            {
                return new ItalicBlock( childrens );
            }
        };
    /**
     * creates monospaced blocks
     */
    private static final FormatBlockFactory MONOSPACED_FACTORY =
        new FormatBlockFactory()
        {
            public Block createBlock( final Block [] childrens )
            {
                return new MonospaceBlock( childrens );
            }
        };
    /**
     * creates bold italic blocks
     */
    private static final FormatBlockFactory BOLDITALIC_FACTORY =
        new FormatBlockFactory()
        {
            public Block createBlock( final Block [] childrens )
            {
                return new BoldBlock( new Block[]{new ItalicBlock( childrens )} );
            }
        };
    /**
     * creates bold monospace blocks
     */
    private static final FormatBlockFactory BOLDMONO_FACTORY =
        new FormatBlockFactory()
        {
            public Block createBlock( final Block [] childrens )
            {
                return new BoldBlock( new Block[]{
                    new MonospaceBlock( childrens )
                } );
            }
        };

    /**
     * format characters
     */
    private static final String [] SPECIAL_CHAR = new String []{
        "__", "==", "*", "_", "="
    };

    static
    {
        FACTORY_MAP.put( "*", BOLD_FACTORY );
        FACTORY_MAP.put( "_", ITALIC_FACTORY );
        FACTORY_MAP.put( "=", MONOSPACED_FACTORY );
        FACTORY_MAP.put( "__", BOLDITALIC_FACTORY );
        FACTORY_MAP.put( "==", BOLDMONO_FACTORY );
    }

    /**
     * @param line line to parse
     * @return TextBlock, ItalicBlock, BoldBlock, MonospacedBlock, ...
     */
    public final Block []parse( final String line )
    {
        return (Block[]) parseFormat( line ).toArray( new Block []{} );
    }

    /**
     * @param c character to test
     * @return <code>true</code> if c is a space character
     */
    private static boolean isSpace( final char c )
    {
        return c == ' ' || c == '\t';
    }

    /**
     * @param c character to test
     * @return <code>true</code> if c is a character that limits the formats
     */
    private static boolean isSpecial( final char c )
    {
        boolean ret = false;

        for ( int i = 0; !ret && i < SPECIAL_CHAR.length; i++ )
        {
            if ( SPECIAL_CHAR[i].charAt( 0 ) == c )
            {
                ret = true;
            }
        }

        return ret;
    }

    /**
     * Parse text format (bold, italic...)
     * <p/>
     * TODO too many lines!!
     *
     * @param line line to parse
     * @return list of blocks
     */
    private List parseFormat( final String line )
    {
        final List ret = new ArrayList();
        final int []lhOffsets = new int[SPECIAL_CHAR.length];
        final int []rhOffsets = new int[SPECIAL_CHAR.length];

        // for each text format markers... 
        for ( int i = 0; i < SPECIAL_CHAR.length; i++ )
        {
            final int specialLen = SPECIAL_CHAR[i].length();
            int t = 0;
            // search the nearset instance of this marker...
            while ( t != -1 && ( t = line.indexOf( SPECIAL_CHAR[i], t ) ) != -1 )
            {
                // and check if it at the begining of a word. 
                if ( t == 0 || isSpace( line.charAt( t - 1 ) ) )
                {
                    // if it is, and if, check to avoid going beyond the string 
                    if ( t + specialLen < line.length() )
                    {
                        // and if character after the format marker is another
                        // marker, is an error, and should be ignored
                        if ( isSpecial( line.charAt( t + specialLen ) ) )
                        {
                            t += specialLen;
                        }
                        else
                        {
                            // else we find a starter!
                            break;
                        }
                    }
                    else
                    {
                        t = -1;
                    }
                }
                else
                {
                    t += specialLen;
                }
            }
            lhOffsets[i] = t;
        }

        // for each text format markers...
        for ( int i = 0; i < lhOffsets.length; i++ )
        {
            final int specialLen = SPECIAL_CHAR[i].length();
            // if we found a text format beginning
            if ( lhOffsets[i] != -1 )
            {
                int t = lhOffsets[i] + specialLen;
                // search for a text format ending
                while ( ( t = line.indexOf( SPECIAL_CHAR[i], t ) ) != -1 )
                {
                    // must be side by side to a word
                    final char c = line.charAt( t - 1 );
                    if ( t > 0 && !isSpace( c ) && !isSpecial( c ) )
                    {
                        break;
                    }
                    else
                    {
                        t += specialLen;
                    }
                }
                rhOffsets[i] = t;
            }
        }

        // find the nearest index
        int minIndex = -1;
        int charType = 0;
        for ( int i = 0; i < lhOffsets.length; i++ )
        {
            if ( lhOffsets[i] != -1 && rhOffsets[i] != 1 )
            {
                if ( minIndex == -1 || lhOffsets[i] < minIndex )
                {
                    if ( rhOffsets[i] > lhOffsets[i] )
                    {
                        // ej: "mary *has a little lamb"
                        minIndex = lhOffsets[i];
                        charType = i;
                    }
                }
            }
        }

        if ( minIndex == -1 || State.isVerbatimMode())
        {
            ret.addAll( textParser.parse( line ) );
        }
        else
        {
            int len = SPECIAL_CHAR[charType].length();
            ret.addAll( parseFormat( line.substring( 0, minIndex ) ) );
            ret.add( ( (FormatBlockFactory) FACTORY_MAP.get( SPECIAL_CHAR[charType] ) ).createBlock(
                (Block[]) parseFormat(
                    line.substring( minIndex + len, rhOffsets[charType] )
                ).toArray( new Block[]{} )
            ) );
            ret.addAll( parseFormat( line.substring( rhOffsets[charType] + len ) ) );
        }

        // profit
        return ret;
    }

    /**
     * Sets the formatTextParser.
     *
     * @param textParser text parser to use
     *                   <code>TextParser</code> with the formatTextParser.
     */
    public final void setTextParser( final TextParser textParser )
    {
        if ( textParser == null )
        {
            throw new IllegalArgumentException( "argument can't be null" );
        }

        this.textParser = textParser;
    }
}

/**
 * @author Juan F. Codagnone
 * @since Nov 3, 2005
 */
interface FormatBlockFactory
{
    /**
     * factory method of format <code>Block</code>
     *
     * @param childrens children of the format block
     * @return a format block
     */
    Block createBlock( final Block[] childrens );
}
