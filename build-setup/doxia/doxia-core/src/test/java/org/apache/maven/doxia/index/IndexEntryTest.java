package org.apache.maven.doxia.index;

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

import junit.framework.TestCase;

/**
 * @author <a href="mailto:trygve.laugstol@objectware.no">Trygve Laugst&oslash;l</a>
 * @version $Id: IndexEntryTest.java 496552 2007-01-16 00:43:05Z vsiveton $
 */
public class IndexEntryTest
    extends TestCase
{
    public void testIndexEntry()
    {
        IndexEntry root = new IndexEntry( null );

        assertIndexEntry( root, null, 0, null, null );

        // -----------------------------------------------------------------------
        // Chapter 1
        // -----------------------------------------------------------------------

        IndexEntry chapter1 = new IndexEntry( root, "chapter-1" );

        assertIndexEntry( root, null, 1, null, null );

        assertIndexEntry( chapter1, root, 0, null, null );

        // -----------------------------------------------------------------------
        // Chapter 2
        // -----------------------------------------------------------------------

        IndexEntry chapter2 = new IndexEntry( root, "chapter-2" );

        assertIndexEntry( root, null, 2, null, null );

        assertIndexEntry( chapter1, root, 0, null, chapter2 );
        assertIndexEntry( chapter2, root, 0, chapter1, null );
    }

    private void assertIndexEntry( IndexEntry entry, IndexEntry parent, int childCount, IndexEntry prevEntry, IndexEntry nextEntry )
    {
        assertEquals( parent, entry.getParent() );

        assertEquals( childCount, entry.getChildEntries().size() );

        assertEquals( prevEntry, entry.getPrevEntry() );

        assertEquals( nextEntry, entry.getNextEntry() );
    }
}
