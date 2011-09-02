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
 * Represents an anchor
 *
 * @author Juan F. Codagnone
 * @since Nov 5, 2005
 */
public class AnchorBlock implements Block
{
    /**
     * anchor name
     */
    private final String name;

    /**
     * Creates the AnchorBlock.
     *
     * @param name name of the anchor
     * @throws IllegalArgumentException if the arguments are <code>null</code>
     */
    public AnchorBlock( final String name ) throws IllegalArgumentException
    {
        if ( name == null )
        {
            throw new IllegalArgumentException( "argument can't be null" );
        }
        this.name = name;
    }

    /**
     * @see Block#traverse(org.apache.maven.doxia.sink.Sink)
     */
    public final void traverse( final Sink sink )
    {
        sink.anchor( name );
        sink.anchor_();
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
        else if ( obj instanceof AnchorBlock )
        {
            final AnchorBlock a = (AnchorBlock) obj;

            ret = name.equals( a.name );
        }

        return ret;
    }

    /**
     * @see Object#hashCode()
     */
    public final int hashCode()
    {
        return name.hashCode();
    }
}
