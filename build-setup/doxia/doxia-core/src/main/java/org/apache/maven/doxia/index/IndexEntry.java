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

import org.codehaus.plexus.util.StringUtils;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Collections;

/**
 * @author <a href="mailto:trygvis@inamo.no">Trygve Laugst&oslash;l</a>
 * @version $Id: IndexEntry.java 559578 2007-07-25 20:12:56Z ltheussl $
 */
public class IndexEntry
{
    /** The parent entry. */
    private IndexEntry parent;

    /** The id of the entry. */
    private String id;

    /** The entry title. */
    private String title;

    /** The child entries. */
    private List childEntries = new ArrayList();

    /** System-dependent EOL. */
    private static final String EOL = System.getProperty( "line.separator" );

    /**
     * Constructor.
     *
     * @param newId The id.
     */
    public IndexEntry( String newId )
    {
        this.id = newId;
    }

    /**
     * Constructor.
     *
     * @param newParent The parent. Cannot be null.
     * @param newId The id. Cannot be null.
     */
    public IndexEntry( IndexEntry newParent, String newId )
    {
        if ( newParent == null )
        {
            throw new NullPointerException( "parent cannot be null." );
        }

        if ( newId == null )
        {
            throw new NullPointerException( "id cannot be null." );
        }

        this.parent = newParent;
        this.id = newId;

        parent.childEntries.add( this );
    }

    /**
     * Returns the parent entry.
     *
     * @return the parent entry.
     */
    public IndexEntry getParent()
    {
        return parent;
    }

    /**
     * Returns the id.
     *
     * @return the id.
     */
    public String getId()
    {
        return id;
    }

    /**
     * Returns the title.
     *
     * @return the title.
     */
    public String getTitle()
    {
        return title;
    }

    /**
     * Sets the title.
     *
     * @param newTitle the title.
     */
    public void setTitle( String newTitle )
    {
        this.title = newTitle;
    }

    /**
     * Returns an unmodifiableList of the child entries.
     *
     * @return child entries.
     */
    public List getChildEntries()
    {
        return Collections.unmodifiableList( childEntries );
    }

    /**
     * Sets the child entriesor creates a new ArrayList if entries == null.
     *
     * @param entries the entries.
     */
    public void setChildEntries( List entries )
    {
        if ( entries == null )
        {
            childEntries = new ArrayList();
        }

        this.childEntries = entries;
    }

    // -----------------------------------------------------------------------
    // Utils
    // -----------------------------------------------------------------------

    /**
     * Returns the next entry.
     *
     * @return the next entry, or null if there is none.
     */
    public IndexEntry getNextEntry()
    {
        if ( parent == null )
        {
            return null;
        }

        List entries = parent.getChildEntries();

        int index = entries.indexOf( this );

        if ( index + 1 >= entries.size() )
        {
            return null;
        }

        return (IndexEntry) entries.get( index + 1 );
    }

    /**
     * Returns the previous entry.
     *
     * @return the previous entry, or null if there is none.
     */
    public IndexEntry getPrevEntry()
    {
        if ( parent == null )
        {
            return null;
        }

        List entries = parent.getChildEntries();

        int index = entries.indexOf( this );

        if ( index == 0 )
        {
            return null;
        }

        return (IndexEntry) entries.get( index - 1 );
    }

    /**
     * Returns the first entry.
     *
     * @return the first entry, or null if there is none.
     */
    public IndexEntry getFirstEntry()
    {
        List entries = getChildEntries();

        if ( entries.size() == 0 )
        {
            return null;
        }
        else
        {
            return (IndexEntry) entries.get( 0 );
        }
    }

    /**
     * Returns the last entry.
     *
     * @return the last entry, or null if there is none.
     */
    public IndexEntry getLastEntry()
    {
        List entries = getChildEntries();

        if ( entries.size() == 0 )
        {
            return null;
        }
        else
        {
            return (IndexEntry) entries.get( entries.size() - 1 );
        }
    }

    /**
     * Returns the root entry.
     *
     * @return the root entry, or null if there is none.
     */
    public IndexEntry getRootEntry()
    {
        List entries = getChildEntries();

        if ( entries.size() == 0 )
        {
            return null;
        }
        else if ( entries.size() > 1 )
        {
            throw new RuntimeException( "This index has more than one root entry" );
        }
        else
        {
            return (IndexEntry) entries.get( 0 );
        }
    }

    // -----------------------------------------------------------------------
    // Object Overrides
    // -----------------------------------------------------------------------

    /**
     * Returns a string representation of the object.
     *
     * @return A string.
     */
    public String toString()
    {
        return toString( 0 );
    }

    /**
     * Returns a string representation of all objects to the given depth.
     *
     * @param depth The depth to descent to.
     * @return A string.
     */
    public String toString( int depth )
    {
        StringBuffer message = new StringBuffer();

        message.append( "Id: " ).append( id );

        if ( StringUtils.isNotEmpty( title ) )
        {
            message.append( ", title: " ).append( title );
        }

        message.append( EOL );

        String indent = "";

        for ( int i = 0; i < depth; i++ )
        {
            indent += " ";
        }

        for ( Iterator it = getChildEntries().iterator(); it.hasNext(); )
        {
            IndexEntry entry = (IndexEntry) it.next();

            message.append( indent ).append( entry.toString( depth + 1 ) );
        }

        return message.toString();
    }
}
