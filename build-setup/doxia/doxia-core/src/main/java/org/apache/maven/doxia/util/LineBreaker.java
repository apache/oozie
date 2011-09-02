package org.apache.maven.doxia.util;

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

import org.codehaus.plexus.util.IOUtil;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;

/** Allows to specify the line-length of an output writer. */
public class LineBreaker
{
    /** The default maximal line length. */
    public static final int DEFAULT_MAX_LINE_LENGTH = 78;

    /** The system dependent EOL. */
    private static final String EOL = System.getProperty( "line.separator" );

    /** The destination writer. */
    private Writer destination;

    /** The writer to use. */
    private BufferedWriter writer;

    /** The maximal line length. */
    private int maxLineLength;

    /** The current line length. */
    private int lineLength = 0;

    /** The string buffer to store the current text. */
    private StringBuffer word = new StringBuffer( 1024 );

    /**
     * Constructs a new LineBreaker with DEFAULT_MAX_LINE_LENGTH.
     *
     * @param out The writer to use.
     */
    public LineBreaker( Writer out )
    {
        this( out, DEFAULT_MAX_LINE_LENGTH );
    }

    /**
     * Constructs a new LineBreaker with the given max line length.
     *
     * @param out The writer to use.
     * @param max The maximal line length.
     */
    public LineBreaker( Writer out, int max )
    {
        if ( max <= 0 )
        {
            throw new IllegalArgumentException( "maxLineLength <= 0" );
        }

        destination = out;
        this.maxLineLength = max;
        writer = new BufferedWriter( out );
    }

    /**
     * Returns the current destination writer.
     *
     * @return The destination.
     */
    public Writer getDestination()
    {
        return destination;
    }

    /**
     * Writes the given text to the writer. White space is not preserved.
     *
     * @param text The text to write.
     * @throws IOException if there's a problem writing the text.
     */
    public void write( String text )
        throws IOException
    {
        write( text, /*preserveSpace*/false );
    }

    /**
     * Writes the given text to the writer.
     *
     * @param text The text to write.
     * @param preserveSpace True to preserve white space.
     */
    public void write( String text, boolean preserveSpace )
    {
        int length = text.length();

        try
        {
            for ( int i = 0; i < length; ++i )
            {
                char c = text.charAt( i );
                String os = System.getProperty( "os.name" ).toLowerCase();

                switch ( c )
                {
                    case ' ':
                        if ( preserveSpace )
                        {
                            word.append( c );
                        }
                        else
                        {
                            writeWord();
                        }
                        break;

                    case '\r':
                        // if \r\n (windows) then just pass along \n
                        if ( os.indexOf( "windows" ) != -1 )
                        {
                            break;
                        }

                    case '\n':
                        writeWord();
                        writer.write( EOL );
                        lineLength = 0;
                        break;

                    default:
                        word.append( c );
                }

            }
        }
        catch ( Exception e )
        {
            // TODO: log
        }
    }

    /**
     * Write out the current StringBuffer and flush the writer.
     * Any IOException will be swallowed.
     */
    public void flush()
    {
        try
        {
            writeWord();
            writer.flush();
        }
        catch ( IOException e )
        {
            // TODO: log
        }
    }

    /**
     * Writes the current StringBuffer to the writer.
     *
     * @throws IOException if an exception occurs during writing.
     */
    private void writeWord()
        throws IOException
    {
        int length = word.length();
        if ( length > 0 )
        {
            if ( lineLength > 0 )
            {
                if ( lineLength + 1 + length > maxLineLength )
                {
                    writer.write( EOL );
                    lineLength = 0;
                }
                else
                {
                    writer.write( ' ' );
                    ++lineLength;
                }
            }

            writer.write( word.toString() );
            word.setLength( 0 );

            lineLength += length;
        }
    }

    /** Close the writer. */
    public void close()
    {
        IOUtil.close( writer );
    }
}
