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

import org.apache.maven.doxia.parser.Parser;
import org.apache.maven.doxia.sink.Sink;


/**
 * Represents a table
 *
 * @author Juan F. Codagnone
 * @since Nov 10, 2005
 */
public class TableBlock extends AbstractFatherBlock
{

    /**
     * Creates the TableBlock.
     *
     * @param childBlocks child blocks
     */
    public TableBlock( final Block [] childBlocks )
    {
        super( childBlocks );
    }


    /**
     * @see AbstractFatherBlock#before(org.apache.maven.doxia.sink.Sink)
     */
    
    public final void before( final Sink sink )
    {
        sink.table();
        sink.tableRows( getJustification(), false );
    }

    /**
     * @see AbstractFatherBlock#after(org.apache.maven.doxia.sink.Sink)
     */
    
    public final void after( final Sink sink )
    {
        sink.tableRows_();
        sink.table_();
    }
    
    
    private final int [] getJustification() {
        int[] justification = new int[((AbstractFatherBlock)getBlocks()[0]).
                                      getBlocks().length];
        for ( int i = 0; i < justification.length; i++ )
        {
            justification[i] = Parser.JUSTIFY_LEFT;
        }
        
        return justification;
    }
}
