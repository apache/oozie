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

import java.io.UnsupportedEncodingException;

import org.apache.maven.doxia.sink.StructureSink;

/**
 * The <code>HtmlTools</code> class defines methods to HTML handling.
 *
 * @author <a href="mailto:vincent.siveton@gmail.com">Vincent Siveton</a>
 * @version $Id: HtmlTools.java 566741 2007-08-16 15:01:27Z ltheussl $
 * @since 1.0
 */
public class HtmlTools
{
    /**
     * Escape special characters in a text in HTML.
     *
     * <pre>
     * < becomes <code>&</code>lt;
     * > becomes <code>&</code>gt;
     * & becomes <code>&</code>amp;
     * " becomes <code>&</code>quot;
     * </pre>
     *
     * @param text the String to escape, may be null
     * @return the text escaped, "" if null String input
     */
    public static String escapeHTML( String text )
    {
        if ( text == null )
        {
            return "";
        }

        int length = text.length();
        StringBuffer buffer = new StringBuffer( length );

        for ( int i = 0; i < length; ++i )
        {
            char c = text.charAt( i );
            switch ( c )
            {
                case '<':
                    buffer.append( "&lt;" );
                    break;
                case '>':
                    buffer.append( "&gt;" );
                    break;
                case '&':
                    buffer.append( "&amp;" );
                    break;
                case '\"':
                    buffer.append( "&quot;" );
                    break;
                default:
                    buffer.append( c );
            }
        }

        return buffer.toString();
    }

    /**
     * Encode an url
     *
     * @param url the String to encode, may be null
     * @return the text encoded, null if null String input
     */
    public static String encodeURL( String url )
    {
        if ( url == null )
        {
            return null;
        }

        StringBuffer encoded = new StringBuffer();
        int length = url.length();

        char[] unicode = new char[1];

        for ( int i = 0; i < length; ++i )
        {
            char c = url.charAt( i );

            switch ( c )
            {
                case ';':
                case '/':
                case '?':
                case ':':
                case '@':
                case '&':
                case '=':
                case '+':
                case '$':
                case ',':
                case '[':
                case ']': // RFC 2732 (IPV6)
                case '-':
                case '_':
                case '.':
                case '!':
                case '~':
                case '*':
                case '\'':
                case '(':
                case ')':
                case '#': // XLink mark
                    encoded.append( c );
                    break;
                default:
                    if ( ( c >= 'a' && c <= 'z' ) || ( c >= 'A' && c <= 'Z' ) || ( c >= '0' && c <= '9' ) )
                    {
                        encoded.append( c );
                    }
                    else
                    {
                        byte[] bytes;

                        try
                        {
                            unicode[0] = c;
                            bytes = ( new String( unicode, 0, 1 ) ).getBytes( "UTF8" );
                        }
                        catch ( UnsupportedEncodingException cannotHappen )
                        {
                            bytes = new byte[0];
                        }

                        for ( int j = 0; j < bytes.length; ++j )
                        {
                            String hex = Integer.toHexString( bytes[j] & 0xFF );

                            encoded.append( '%' );
                            if ( hex.length() == 1 )
                            {
                                encoded.append( '0' );
                            }
                            encoded.append( hex );
                        }
                    }
            }
        }

        return encoded.toString();
    }

    /**
     * Replace all characters in a text.
     *
     * <pre>
     * HtmlTools.encodeFragment( null ) = null
     * HtmlTools.encodeFragment( "" ) = ""
     * HtmlTools.encodeFragment( "http://www.google.com" ) = "httpwwwgooglecom"
     * </pre>
     *
     * @param text the String to check, may be null
     * @return the text with only letter and digit, null if null String input
     */
    public static String encodeFragment( String text )
    {
        if ( text == null )
        {
            return null;
        }
        return encodeURL( StructureSink.linkToKey( text ) );
    }

    /**
     * Construct a valid id.
     * <p>
     * According to the <a href="http://www.w3.org/TR/html4/types.html#type-name">
     * HTML 4.01 specification section 6.2 SGML basic types</a>:
     * </p>
     * <p>
     * <i>ID and NAME tokens must begin with a letter ([A-Za-z]) and may be
     * followed by any number of letters, digits ([0-9]), hyphens ("-"),
     * underscores ("_"), colons (":"), and periods (".").</i>
     * </p>
     *
     * <p>
     * According to <a href="http://www.w3.org/TR/xhtml1/#C_8">XHTML 1.0
     * section C.8. Fragment Identifiers</a>:
     * </p>
     * <p>
     * <i>When defining fragment identifiers to be backward-compatible, only
     * strings matching the pattern [A-Za-z][A-Za-z0-9:_.-]* should be used.</i>
     * </p>
     *
     * <p>
     * To achieve this we need to convert the <i>id</i> String. Two conversions
     * are necessary and one is done to get prettier ids:
     * </p>
     * <ol>
     * <li>If the first character is not a letter, prepend the id with the
     * letter 'a'</li>
     * <li>A space is replaced with an underscore '_'</li>
     * <li>Remove whitespace at the start and end before starting to process</li>
     * </ol>
     *
     * <p>
     * For letters, the case is preserved in the conversion.
     * </p>
     * 
     * <p>
     * Here are some examples:
     * </p>
     * <pre>
     * HtmlTools.encodeId( null )        = null
     * HtmlTools.encodeId( "" )          = ""
     * HtmlTools.encodeId( " _ " )       = "a_"
     * HtmlTools.encodeId( "1" )         = "a1"
     * HtmlTools.encodeId( "1anchor" )   = "a1anchor"
     * HtmlTools.encodeId( "_anchor" )   = "a_anchor"
     * HtmlTools.encodeId( "a b-c123 " ) = "a_b-c123"
     * HtmlTools.encodeId( "   anchor" ) = "anchor"
     * HtmlTools.encodeId( "myAnchor" )  = "myAnchor"
     * </pre>
     *
     * @param id The id to be encoded
     * @return The id trimmed and encoded
     */
    public static String encodeId( String id )
    {
        if ( id == null )
        {
            return null;
        }

        id = id.trim();
        int length = id.length();
        StringBuffer buffer = new StringBuffer( length );

        for ( int i = 0; i < length; ++i )
        {
            char c = id.charAt( i );
            if ( ( i == 0 ) && ( !Character.isLetter( c ) ) )
            {
                buffer.append( "a" );
            }
            if ( c == ' ' )
            {
                buffer.append( "_" );
            }
            else if ( ( Character.isLetterOrDigit( c ) ) || ( c == '-' ) || ( c == '_' ) || ( c == ':' ) || ( c == '.' ) )
            {
                buffer.append( c );
            }
        }

        return buffer.toString();
    }

    /**
     * Determines if the specified text is a valid id according to the rules
     * laid out in encodeId(String).
     *
     * @see #encodeId(String)
     * @param text The text to be tested
     * @return <code>true</code> if the text is a valid id, otherwise <code>false</code>
     */
    public static boolean isId( String text )
    {
        if ( text == null || text.length() == 0 )
        {
            return false;
        }

        for ( int i = 0; i < text.length(); ++i )
        {
            char c = text.charAt( i );
            if ( i == 0 && !Character.isLetter( c ) )
            {
                return false;
            }
            if ( c == ' ' )
            {
                return false;
            }
            else if ( !Character.isLetterOrDigit( c ) && c != '-' && c != '_' && c != ':' && c != '.' )
            {
                return false;
            }
        }

        return true;
    }
}
