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
 * Block that holds plain text
 *
 * @author Juan F. Codagnone
 * @since Nov 1, 2005
 */
public class TextBlock implements Block
{
    /**
     * the text
     */
    private final String text;

    /**
     * Creates the TextBlock.
     *
     * @param text some text. can't ben <code>null</code>
     * @throws IllegalArgumentException if parameters are not in the domain
     */
    public TextBlock( final String text ) throws IllegalArgumentException
    {
        if ( text == null )
        {
            throw new IllegalArgumentException( "argument can't be null" );
        }

        this.text = text;
    }

    /**
     * @see Block#traverse(org.apache.maven.doxia.sink.Sink)
     */
    public final void traverse( final Sink sink )
    {
        sink.text( text );
    }

    /**
     * @see Object#toString()
     */
    
    public final String toString()
    {
        return getClass().getName() + ": [" + text.replaceAll( "\n", "\\n" ) + "]";
    }


    /**
     * Returns the text.
     *
     * @return <code>String</code> with the text.
     */
    public final String getText()
    {
        return text;
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
        else if ( obj instanceof TextBlock )
        {
            final TextBlock textBlock = (TextBlock) obj;
            ret = text.equals( textBlock.text );
        }

        return ret;
    }

    /**
     * @see Object#hashCode()
     */
    
    public final int hashCode()
    {
        return text.hashCode();
    }
}
