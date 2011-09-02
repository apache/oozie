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

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

import org.apache.maven.doxia.sink.Sink;
import org.apache.maven.doxia.index.IndexingSink;


/**
 * Block that represents a section
 *
 * @author Juan F. Codagnone
 * @since Nov 1, 2005
 */
public class SectionBlock extends AbstractFatherBlock
{
    /**
     * @see #SectionBlock(String, int, Block[])
     */
    private final String title;
    private final String rawTitle;
    /**
     private final String title;
     * @see #SectionBlock(String, int, Block[])
     */
    private final int level;

    private static Stack levelStack = new Stack();

    /**
     * Creates the SectionBlock.
     * <p/>
     * No parameter can be <code>null</code>
     *
     * @param title  the section title.
     * @param level  the section level: 0 < level < 6
     * @param blocks child blocks
     * @throws IllegalArgumentException if the parameters are not in the domain
     */
    public SectionBlock( final String title, final int level,
                         final Block []blocks ) throws IllegalArgumentException
    {
        super( blocks );
        final int maxLevel = 5;
        if ( title == null )
        {
            throw new IllegalArgumentException( "title cant be null" );
        }
        else if ( level < 1 || level > maxLevel )
        {
            throw new IllegalArgumentException( "invalid level: " + level );
        }
        if(title.trim().startsWith("!!")) {
            this.title = title.trim().substring(2);
        } else {
            this.title = title.trim();
        }
        this.rawTitle = title;
        this.level = level;
    }

    /**
     * @see AbstractFatherBlock#before(org.apache.maven.doxia.sink.Sink)
     */
    
    public final void before( final Sink sink )
    {
            String titleAnchor = title;
            while(State.isInTitleList(sink, titleAnchor)) {
                titleAnchor += "_";
            }
            State.addToTitleList(sink, titleAnchor);

            sink.anchor(encodeId(titleAnchor));
            sink.anchor_();
            sectionStart( sink );
            sectionTitle( sink );
            if (sink instanceof IndexingSink) {
                sink.text( rawTitle );
            } else {
                sink.text( title );
            }
            sectionTitle_( sink );
    }

    /**
     * @see AbstractFatherBlock#after(org.apache.maven.doxia.sink.Sink)
     */
    
    public final void after( final Sink sink )
    {
        sectionEnd( sink );
    }

    /**
     * call to sink.section<Level>()
     *
     * @param sink sink
     */
    private void sectionStart( final Sink sink )
    {
        while(!levelStack.isEmpty() && level <= ((Integer)levelStack.peek()).intValue()) {
            int outLevel = ((Integer)levelStack.pop()).intValue();
            invokeVoidVoid( sink, "section" + outLevel + "_" );
        }
        levelStack.push(new Integer(level));
        invokeVoidVoid( sink, "section" + level );
    }

    /**
     * call to sink.section<Level>_()
     *
     * @param sink sink
     */
    private void sectionEnd( final Sink sink )
    {
        //invokeVoidVoid( sink, "section" + level + "_" );
    }


    /**
     * Let you call sink's methods that returns <code>null</code> and have
     * no parameters.
     *
     * @param sink the Sink
     * @param name the name of the method to call
     * @throws IllegalArgumentException on error
     */
    private static void invokeVoidVoid( final Sink sink, final String name )
        throws IllegalArgumentException
    {
        try
        {
            final Method m = sink.getClass().getMethod( name, new Class[]{} );
            m.invoke( sink, Collections.EMPTY_LIST.toArray() );
        }
        catch ( Exception e )
        {
            // FIXME
            throw new IllegalArgumentException( "invoking sink's " + name
                + " method: " + e.getMessage() );
        }
    }

    /**
     * Returns the level.
     *
     * @return <code>int</code> with the level.
     */
    public final int getLevel()
    {
        return level;
    }

    /**
     * Returns the title.
     *
     * @return <code>String</code> with the title.
     */
    public final String getTitle()
    {
        return title;
    }

    /**
     * @see Object#toString()
     */
    
    public final String toString()
    {
        final StringBuffer sb = new StringBuffer();

        sb.append( "Section  {title: '" );
        sb.append( getTitle() );
        sb.append( "' level: " );
        sb.append( getLevel() );
        sb.append( "}: [" );
        for ( int i = 0; i < getBlocks().length; i++ )
        {
            final Block block = getBlocks()[i];

            sb.append( block.toString() );
            sb.append( ", " );
        }
        sb.append( "]" );
        return sb.toString();
    }

    /** @param sink */
    private void sectionTitle( final Sink sink ) 
    {
        invokeVoidVoid( sink, "sectionTitle" + level );
    }
    
    /** @param sink */
    private void sectionTitle_( final Sink sink ) 
    {
        invokeVoidVoid( sink, "sectionTitle" + level + "_" );
    }

    public static void clearLevelStack() {
        while(!levelStack.isEmpty()) {
             levelStack.pop();
        }
    }

    public static void closeAllSections(Sink sink) {
         while(!levelStack.isEmpty()) {
             int outLevel = ((Integer)levelStack.pop()).intValue();
             invokeVoidVoid( sink, "section" + outLevel + "_" );
         }
    }

                              /**
     * Construct a valid id.
     * <p>
     * According to the <a href="http://www.w3.org/TR/html4/types.html#type-name">
     * HTML 4.01 specification section 6.2 SGML basic types</a>:
     * </p>
     * <p>
     * <i>ID and NAME tokens must begin with a letter ([A-Za-z]) and may be
     * followed by any number of letters, digits ([0-9]), hyphens ("-"),
     * underscores ("_"), colons (":"), and periods (".").</i>
     * </p>
     *
     * <p>
     * According to <a href="http://www.w3.org/TR/xhtml1/#C_8">XHTML 1.0
     * section C.8. Fragment Identifiers</a>:
     * </p>
     * <p>
     * <i>When defining fragment identifiers to be backward-compatible, only
     * strings matching the pattern [A-Za-z][A-Za-z0-9:_.-]* should be used.</i>
     * </p>
     *
     * <p>
     * To achieve this we need to convert the <i>id</i> String. Two conversions
     * are necessary and one is done to get prettier ids:
     * </p>
     * <ol>
     * <li>If the first character is not a letter, prepend the id with the
     * letter 'a'</li>
     * <li>A space is replaced with an underscore '_'</li>
     * <li>Remove whitespace at the start and end before starting to process</li>
     * </ol>
     *
     * <p>
     * For letters, the case is preserved in the conversion.
     * </p>
     *
     * <p>
     * Here are some examples:
     * </p>
     * <pre>
     * HtmlTools.encodeId( null )        = null
     * HtmlTools.encodeId( "" )          = ""
     * HtmlTools.encodeId( " _ " )       = "a_"
     * HtmlTools.encodeId( "1" )         = "a1"
     * HtmlTools.encodeId( "1anchor" )   = "a1anchor"
     * HtmlTools.encodeId( "_anchor" )   = "a_anchor"
     * HtmlTools.encodeId( "a b-c123 " ) = "a_b-c123"
     * HtmlTools.encodeId( "   anchor" ) = "anchor"
     * HtmlTools.encodeId( "myAnchor" )  = "myAnchor"
     * </pre>
     *
     * @param id The id to be encoded
     * @return The id trimmed and encoded
     */
    public static String encodeId( String id )
    {
        if ( id == null )
        {
            return null;
        }

        id = id.trim();
        int length = id.length();
        StringBuffer buffer = new StringBuffer( length );

        for ( int i = 0; i < length; ++i )
        {
            char c = id.charAt( i );
            if ( ( i == 0 ) && ( !Character.isLetter( c ) ) )
            {
                buffer.append( "a" );
            }
            if ( c == ' ' )
            {
                buffer.append( "_" );
            }
            else if ( ( Character.isLetterOrDigit( c ) ) || ( c == '-' ) || ( c == '_' ) || ( c == ':' ) || ( c == '.' ) )
            {
                buffer.append( c );
            }
        }

        return buffer.toString();
    }
}
