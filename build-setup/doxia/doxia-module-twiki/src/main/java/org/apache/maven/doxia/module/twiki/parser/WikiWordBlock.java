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
 * Represent a WikiWord
 *
 * @author Juan F. Codagnone
 * @since Nov 4, 2005
 */
public class WikiWordBlock implements Block
{
    /**
     * the wiki word
     */
    private final String wikiword;
    /**
     * text to show in the wiki word link
     */
    private final String showText;

    /**
     * @see #WikiWordBlock(String, String)
     */
    public WikiWordBlock( final String wikiword )
    {
        this( wikiword, wikiword );
    }

    /**
     * Creates the WikiWordBlock.
     *
     * @param wikiword the wiki word
     * @param showText text to show in the wiki link
     * @throws IllegalArgumentException if the wikiword is <code>null</code>
     */
    public WikiWordBlock( final String wikiword, final String showText )
        throws IllegalArgumentException
    {
        if ( wikiword == null || showText == null )
        {
            throw new IllegalArgumentException( "arguments can't be null" );
        }
        this.wikiword = wikiword;
        this.showText = showText;
    }

    /**
     * @see Block#traverse(org.apache.maven.doxia.sink.Sink)
     */
    public final void traverse( final Sink sink )
    {
        sink.link("./" + wikiword + ".html");
        sink.text( showText );
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
        else if ( obj instanceof WikiWordBlock )
        {
            final WikiWordBlock w = (WikiWordBlock) obj;
            ret = wikiword.equals( w.wikiword )
                && showText.equals( w.showText );
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

        return magic1 + magic2 * wikiword.hashCode()
            + magic2 * showText.hashCode();
    }
}
