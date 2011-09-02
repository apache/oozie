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
 * Block that represents the item in a list
 *
 * @author Juan F. Codagnone
 * @since Nov 1, 2005
 */
public class ListItemBlock extends AbstractFatherBlock
{
    /** */
    private final ListBlock innerList;

    /**
     * @see #ListItemBlock(Block[], ListBlock)
     */
    public ListItemBlock( final Block [] blocks )
        throws IllegalArgumentException
    {
        this( blocks, null );
    }

    /**
     * Creates the ListItemBlock.
     *
     * @param blocks    text
     * @param innerList child list
     * @throws IllegalArgumentException if textBlocks is null
     */
    public ListItemBlock( final Block [] blocks, final ListBlock innerList )
        throws IllegalArgumentException
    {
        super( blocks );
        this.innerList = innerList;
    }

    /**
     * @see AbstractFatherBlock#before(org.apache.maven.doxia.sink.Sink)
     */
    
    public final void before( final Sink sink )
    {
        sink.listItem();
    }

    /**
     * @see AbstractFatherBlock#after(org.apache.maven.doxia.sink.Sink)
     */
    
    public final void after( final Sink sink )
    {
        if ( innerList != null )
        {
            innerList.traverse( sink );
        }
        sink.listItem_();
    }

    /**
     * Returns the innerList.
     *
     * @return <code>UnorderedListBlock</code> with the innerList.
     */
    public final ListBlock getInnerList()
    {
        return innerList;
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
        else if ( obj == null || this == null )
        {
            ret = false;
        }
        else if ( obj instanceof ListItemBlock )
        {
            final ListItemBlock li = (ListItemBlock) obj;
            if ( this.innerList == null && li.innerList == null )
            {
                ret = super.equals( li );
            }
            else if ( this.innerList == null && li.innerList != null )
            {
                ret = false;
            }
            else
            {
                ret = this.innerList.equals( li.innerList ) && super.equals( li );
            }
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

        return magic1 + magic2 * super.hashCode()
            + ( innerList == null ? 0 : magic2 * innerList.hashCode() );
    }
}
