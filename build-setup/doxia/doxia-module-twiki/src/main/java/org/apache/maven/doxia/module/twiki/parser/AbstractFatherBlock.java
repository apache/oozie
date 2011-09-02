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

import java.util.Arrays;

import org.apache.maven.doxia.sink.Sink;


/**
 * Generic Block for the Block that have child blocks.
 *
 * @author Juan F. Codagnone
 * @since Nov 1, 2005
 */
public abstract class AbstractFatherBlock implements Block
{
    /**
     * @see AbstractFatherBlock#AbstractFatherBlock(Block[])
     */
    private final Block[] childBlocks;

    /**
     * method called before traversing the childs
     *
     * @param sink a sink to fill
     */
    public abstract void before( Sink sink );

    /**
     * method called after traversing the childs
     *
     * @param sink a sink to fill
     */
    public abstract void after( Sink sink );

    /**
     * Creates the AbstractFatherBlock.
     *
     * @param childBlocks child blocks
     */
    public AbstractFatherBlock( final Block[] childBlocks )
    {
        if ( childBlocks == null )
        {
            throw new IllegalArgumentException( "argument can't be null" );
        }

        for ( int i = 0; i < childBlocks.length; i++ )
        {
            if ( childBlocks[i] == null )
            {
                throw new IllegalArgumentException( "bucket " + i
                    + " can't be null" );
            }
        }
        this.childBlocks = childBlocks;
    }

    /**
     * @see Block#traverse(org.apache.maven.doxia.sink.Sink)
     */
    public final void traverse( final Sink sink )
    {
        before( sink );
        for ( int i = 0; i < childBlocks.length; i++ )
        {
            Block block = childBlocks[i];
            
            block.traverse( sink );
        }
        after( sink );
    }

    /**
     * Returns the childBlocks.
     *
     * @return <code>Block[]</code> with the childBlocks.
     */
    public final Block [] getBlocks()
    {
        return childBlocks;
    }

    /**
     * @see Object#equals(Object)
     */
    // CHECKSTYLE:DESIGN:OFF
    public boolean equals( final Object obj )
    {
        boolean ret = false;

        if ( obj == this )
        {
            ret = true;
        }
        else if ( obj == null )
        {
            ret = false;
        }
        else if ( obj.getClass().equals( this.getClass() ) )
        {
            if ( obj instanceof AbstractFatherBlock )
            {
                final AbstractFatherBlock a = (AbstractFatherBlock) obj;
                ret = Arrays.equals( a.childBlocks, this.childBlocks );
            }
        }

        return ret;
    }

    /**
     * @see Object#hashCode()
     */
    public int hashCode()
    {
        int result = 1;
        if ( childBlocks != null )
        {
            for ( int i = 0; i < childBlocks.length; i++ )
            {
                result += childBlocks[i].hashCode();
            }
        }
        
        return result;
    }
    // CHECKSTYLE:DESIGN:ON
}
