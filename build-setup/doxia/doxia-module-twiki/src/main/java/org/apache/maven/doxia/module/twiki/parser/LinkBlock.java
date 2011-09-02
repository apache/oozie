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

import org.apache.maven.doxia.sink.Sink;


/**
 * Block that represents a link.
 *
 * @author Juan F. Codagnone
 * @since Nov 4, 2005
 */
public class LinkBlock implements Block
{
    /**
     * link reference
     */
    private final String reference;
    /**
     * link text
     */
    private final String text;

    /**
     * Creates the LinkBlock.
     *
     * @param reference reference anchor
     * @param text      text to show
     * @throws IllegalArgumentException if any argument is <code>null</code>
     */
    public LinkBlock( final String reference, final String text )
        throws IllegalArgumentException
    {
        if ( reference == null || text == null )
        {
            throw new IllegalArgumentException( "arguments can't be null" );
        }
        this.reference = reference;
        this.text = text;
    }

    /**
     * @see Block#traverse(org.apache.maven.doxia.sink.Sink)
     */
    public final void traverse( final Sink sink )
    {
        String referenceValue;
        if(isExternalLink(reference)) {
            referenceValue = reference;
        } else {
            /* For Wiki Words */
            /*Find index of # first */
            int indexOfHash = reference.indexOf("#");
            if(indexOfHash > 0) {
                 referenceValue = "./" + reference.substring(0, indexOfHash) + ".html"
                         + reference.substring(indexOfHash);
            } else {
                referenceValue = "./" + reference + ".html";
            }
        }
        sink.link( referenceValue );
        sink.text( text );
        sink.link_();

    }

    /**
     * @see Object#equals(Object)
     */
    
    public final boolean equals( final Object obj )
    {
        boolean ret = false;

        if ( obj == this )
        {
            ret = true;
        }
        else if ( obj instanceof LinkBlock )
        {
            final LinkBlock l = (LinkBlock) obj;
            ret = reference.equals( l.reference )
                && text.equals( l.text );
        }

        return ret;
    }

    /**
     * @see Object#hashCode()
     */
    
    public final int hashCode()
    {
        final int magic1 = 17;
        final int magic2 = 37;

        return magic1 + magic2 * reference.hashCode()
            + magic2 * text.hashCode();
    }

    /**
     * tests if this is an external link or not
     * @param link
     * @return
     */
    private boolean isExternalLink( String link )
    {
        String text = link.toLowerCase();

        return ( text.indexOf( "http:/" ) == 0 || text.indexOf( "https:/" ) == 0
            || text.indexOf( "ftp:/" ) == 0 || text.indexOf( "mailto:" ) == 0
            || text.indexOf( "file:/" ) == 0 || text.indexOf( "../" ) == 0
            || text.indexOf( "./" ) == 0 );
    }

}
