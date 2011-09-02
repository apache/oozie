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

/** Utility methods for Sinks. */
public class StructureSink
{
    /**
     * Checks if the given string corresponds to an external URI,
     * ie is not a link within the same document. 
     *
     * @param link The link to check.
     * @return True if the link (ignoring case) starts with either of the
     * following: "http:/", "https:/", "ftp:/", "mailto:", "file:/",
     * "../" or "./". Note that Windows style separators "\" are not allowed
     * for URIs, see  http://www.ietf.org/rfc/rfc2396.txt , section 2.4.3.
     */
    public static boolean isExternalLink( String link )
    {
        String text = link.toLowerCase();

        return ( text.indexOf( "http:/" ) == 0 || text.indexOf( "https:/" ) == 0
            || text.indexOf( "ftp:/" ) == 0 || text.indexOf( "mailto:" ) == 0
            || text.indexOf( "file:/" ) == 0 || text.indexOf( "../" ) == 0
            || text.indexOf( "./" ) == 0 );
    }

    /**
     * Transforms the given text such that it can be used as a link.
     *
     * @param text The text to transform.
     * @return A text with escaped special characters.
     * @todo This is apt specific, need to clarify general use.
     */
    public static String linkToKey( String text )
    {
        int length = text.length();
        StringBuffer buffer = new StringBuffer( length );

        for ( int i = 0; i < length; ++i )
        {
            char c = text.charAt( i );
            if ( Character.isLetterOrDigit( c ) )
            {
                buffer.append( Character.toLowerCase( c ) );
            }
        }

        return buffer.toString();
    }
}
