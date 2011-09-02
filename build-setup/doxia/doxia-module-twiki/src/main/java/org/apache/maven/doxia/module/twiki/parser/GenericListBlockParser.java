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

import org.apache.maven.doxia.util.ByLineSource;
import org.apache.maven.doxia.parser.ParseException;
import org.apache.maven.doxia.sink.Sink;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Generic list parser
 *
 * @author Juan F. Codagnone
 * @since Nov 9, 2005
 */
public class GenericListBlockParser implements BlockParser
{
    static final String EOL = System.getProperty( "line.separator" );

    /**
     * parser used to create text blocks
     */
    private FormatedTextParser formatedTextParser;
    /**
     * supported patterns
     */
    private final Pattern [] patterns = new Pattern[TYPES.length];

    /**
     * Creates the GenericListBlockParser. *
     */
    public GenericListBlockParser()
    {
        for ( int i = 0; i < TYPES.length; i++ )
        {
            patterns[i] = Pattern.compile( "^((   )+)"
                + TYPES[i].getItemPattern() + "(.*)$" );
        }
    }

    /**
     * @see BlockParser#accept(String)
     */
    public final boolean accept( final String line )
    {
        boolean ret = false;

        for ( int i = 0; !ret && i < patterns.length; i++ )
        {
            ret |= patterns[i].matcher( line ).lookingAt();
        }

        return ret;
    }

    /**
     * @see BlockParser#visit(String,org.apache.maven.doxia.util.ByLineSource)
     */
    public final Block visit( final String line, final ByLineSource source )
        throws ParseException
    {
        final TreeListBuilder treeListBuilder =
            new TreeListBuilder( formatedTextParser );
        // new TreeListBuilder(formatedTextParser);
        String l = line;
        do
        {
            if ( !accept( l ) )
            {
                break;
            }

            for ( int i = 0; i < patterns.length; i++ )
            {
                final Matcher m = patterns[i].matcher( l );
                if ( m.lookingAt() )
                {
                    final int numberOfSpaces = 3;
                    final int textGroup = 3;
                    assert m.group( 1 ).length() % numberOfSpaces == 0;
                    final int level = m.group( 1 ).length() / numberOfSpaces;
                    treeListBuilder.feedEntry( TYPES[i], level,
                                               m.group( textGroup ).trim() );
                    break;
                }
            }
        }
        while ( ( l = source.getNextLine() ) != null );

        if ( l != null )
        {
            source.ungetLine();
        }

        return treeListBuilder.getBlock();
    }


    /**
     * Sets the formatTextParser.
     *
     * @param textParser <code>FormatedTextParser</code> with the formatTextParser.
     */
    public final void setTextParser( final FormatedTextParser textParser )
    {
        if ( textParser == null )
        {
            throw new IllegalArgumentException(
                "formatTextParser can't be null" );
        }
        this.formatedTextParser = textParser;
    }
    
    public interface Type
    {
        /**
         * @return the pattern of the item part of the list regex
         */
        String getItemPattern();
        
        /**
         * @param items children of the new listblock
         * @return a new ListBlock
         */
        ListBlock createList( final ListItemBlock []items );
        
    }
    
    /**
     * unordered list
     */
    private static final Type LIST = new Type() {
        /**
         * @see Type#getItemPattern()
         */
        public String getItemPattern()
        {
            return "[*]";
        }

        /**
         * @see Type#createList(ListItemBlock)
         */
        public ListBlock createList( final ListItemBlock []items )
        {
            return new UnorderedListBlock( items );
        }
    };
    
    /**
     * a.
     */
    private static final Type ORDERED_LOWER_ALPHA = new Type() {
        /**
         * @see Type#getItemPattern()
         */
        public String getItemPattern()
        {
            return "[a-hj-z][.]";
        }

        /**
         * @see Type#createList(ListItemBlock)
         */
        public ListBlock createList( final ListItemBlock []items )
        {
            return new NumeratedListBlock( Sink.NUMBERING_LOWER_ALPHA,
                                           items );
        }
    };
    
    /**
     * A.
     */
    private static final Type ORDERED_UPPER_ALPHA = new Type() {
        /**
         * @see Type#getItemPattern()
         */
        public String getItemPattern()
        {
            return "[A-HJ-Z][.]";
        }

        /**
         * @see Type#createList(ListItemBlock)
         */
        public ListBlock createList( final ListItemBlock []items )
        {
            return new NumeratedListBlock( Sink.NUMBERING_UPPER_ALPHA,
                                           items );
        }
    };
    
    /**
     * 1.
     */
    private static final Type ORDERERED_DECIMAL = new Type(){
        /**
         * @see Type#getItemPattern()
         */
        public String getItemPattern()
        {
            return "[0123456789][.]";
        }

        /**
         * @see Type#createList(ListItemBlock)
         */
        public ListBlock createList( final ListItemBlock []items )
        {
            return new NumeratedListBlock( Sink.NUMBERING_DECIMAL,
                                           items );
        }
    };
    
    /**
     * i.
     */
    private static final Type ORDERERED_LOWER_ROMAN = new Type(){
        /**
         * @see Type#getItemPattern()
         */
        public String getItemPattern()
        {
            return "[i][.]";
        }

        /**
         * @see Type#createList(ListItemBlock)
         */
        public ListBlock createList( final ListItemBlock []items )
        {
            return new NumeratedListBlock( Sink.NUMBERING_LOWER_ROMAN,
                                           items );
        }
    };
    
    /**
     * I.
     */
    private static final Type ORDERERED_UPPER_ROMAN = new Type(){
        /**
         * @see Type#getItemPattern()
         */
        public String getItemPattern()
        {
            return "[I][.]";
        }

        /**
         * @see Type#createList(ListItemBlock)
         */
        public ListBlock createList( final ListItemBlock []items )
        {
            return new NumeratedListBlock( Sink.NUMBERING_UPPER_ROMAN,
                                           items );
        }
    };
    
    private static final Type[] TYPES = {
        LIST, ORDERED_LOWER_ALPHA, ORDERED_UPPER_ALPHA, ORDERERED_DECIMAL, ORDERERED_LOWER_ROMAN, ORDERERED_UPPER_ROMAN
    };

}

/**
 * It helps to build
 *
 * @author Juan F. Codagnone
 * @since Nov 1, 2005
 */
class TreeListBuilder
{
    /**
     * parser that create text blocks
     */
    private final FormatedTextParser textParser;
    /**
     * tree root
     */
    private final TreeComponent root;
    /**
     * the current element of the tree
     */
    private TreeComponent current;


    /**
     * Creates the TreeListBuilder.
     *
     * @param formatTextParser parser that create text blocks
     * @throws IllegalArgumentException if <code>formatTextParser</code> is null
     */
    public TreeListBuilder( final FormatedTextParser formatTextParser )
        throws IllegalArgumentException
    {
        if ( formatTextParser == null )
        {
            throw new IllegalArgumentException( "argument is null" );
        }
        this.textParser = formatTextParser;
        root = new TreeComponent( null, "root", null );
        current = root;
    }

    /**
     * recibe un nivel y un texto y armar magicamente (manteniendo estado)
     * el �rbol
     *
     * @param type  type of list
     * @param level indentation level of the item
     * @param text  text of the item
     */
    public void feedEntry( final GenericListBlockParser.Type type,
                           final int level, final String text )
    {
        final int currentDepth = current.getDepth();
        final int incomingLevel = level - 1;

        if ( incomingLevel == currentDepth )
        {
            // nothing to move
        }
        else if ( incomingLevel > currentDepth )
        {
            // el actual ahora es el �ltimo que insert�
            final TreeComponent []components = current.getChildren();
            if ( components.length == 0 )
            {
                /* for example:
                 *        * item1
                 *     * item2
                 */
                for ( int i = 0, n = incomingLevel - currentDepth; i < n; i++ )
                {
                    current = current.addChildren( "", type );
                }
            }
            else
            {
                current = components[components.length - 1];
            }

        }
        else
        {
            for ( int i = 0, n = currentDepth - incomingLevel; i < n; i++ )
            {
                current = current.getFather();
                if ( current == null )
                {
                    throw new IllegalStateException();
                }
            }
        }
        current.addChildren( text, type );
    }

    /**
     * @return a Block for the list that we received
     */
    public ListBlock getBlock()
    {
        return getList( root );
    }

    /**
     * Wrapper
     *
     * @param tc tree
     * @return list Block for this tree
     */
    private ListBlock getList( final TreeComponent tc )
    {
        ListItemBlock[] li = (ListItemBlock[]) getListItems( tc ).toArray( new ListItemBlock[]{} );
        return tc.getChildren()[0].getType().createList( li );
    }

    /**
     * @param tc tree
     * @return list Block for this tree
     */
    private List getListItems( final TreeComponent tc )
    {
        final List blocks = new ArrayList();

        for ( int i = 0; i < tc.getChildren().length; i++ )
        {
            final TreeComponent child = tc.getChildren()[i];
            
            Block[] text = new Block[]{};
            if ( child.getFather() != null )
            {
                text = textParser.parse( child.getText() );
            }

            if ( child.getChildren().length != 0 )
            {
                blocks.add( new ListItemBlock( text, getList( child ) ) );
            }
            else
            {
                blocks.add( new ListItemBlock( text ) );
            }
        }

        return blocks;
    }
    
    /**
     * A bidirectional tree node
     *
     * @author Juan F. Codagnone
     * @since Nov 1, 2005
     */
    class TreeComponent
    {
        /**
         * childrens
         */
        private List children = new ArrayList();
        /**
         * node text
         */
        private String text;
        /**
         * the father
         */
        private TreeComponent father;
        /**
         * type of the list
         */
        private GenericListBlockParser.Type type;

        /**
         * Creates the TreeComponent.
         *
         * @param father Component father
         * @param text   component text
         * @param type   component type
         */
        public TreeComponent( final TreeComponent father, final String text,
                              final GenericListBlockParser.Type type )
        {
            this.text = text;
            this.father = father;
            this.type = type;
        }

        /**
         * @return my childrens
         */
        public TreeComponent[] getChildren()
        {
            return (TreeComponent[]) children.toArray( new TreeComponent[]{} );
        }

        /**
         * adds a children node
         *
         * @param t     text of the children
         * @param ttype component type
         * @return the new node created
         */
        public TreeComponent addChildren( final String t,
                                          final GenericListBlockParser.Type ttype )
        {
            if ( t == null || ttype == null )
            {
                throw new IllegalArgumentException( "argument is null" );
            }
            final TreeComponent ret = new TreeComponent( this, t, ttype );
            children.add( ret );

            return ret;
        }

        /**
         * @return the father
         */
        public TreeComponent getFather()
        {
            return father;
        }

        /**
         * @return the node depth in the tree
         */
        public int getDepth()
        {
            int ret = 0;

            TreeComponent c = this;

            while ( ( c = c.getFather() ) != null )
            {
                ret++;
            }

            return ret;
        }

        /**
         * @see Object#toString()
         */
        
        public String toString()
        {
            return toString( "" );
        }

        /**
         * @see Object#toString()
         */
        public String toString( final String indent )
        {
            final StringBuffer sb = new StringBuffer();

            if ( father != null )
            {
                sb.append( indent );
                sb.append( "- " );
                sb.append( text );
                sb.append( GenericListBlockParser.EOL );
            }
            for ( Iterator it = children.iterator(); it.hasNext(); )
            {
                TreeComponent lc = (TreeComponent) it.next();
                sb.append( lc.toString( indent + "   " ) );
            }
            return sb.toString();
        }

        /**
         * Returns the text.
         *
         * @return <code>String</code> with the text.
         */
        public String getText()
        {
            return text;
        }

        /**
         * Returns the type.
         *
         * @return <code>Type</code> with the text.
         */
        public GenericListBlockParser.Type getType()
        {
            return type;
        }
    }

}

