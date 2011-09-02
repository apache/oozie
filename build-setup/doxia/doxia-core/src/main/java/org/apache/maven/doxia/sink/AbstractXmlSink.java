package org.apache.maven.doxia.sink;

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

import java.util.Enumeration;

import javax.swing.text.AttributeSet;
import javax.swing.text.MutableAttributeSet;
import javax.swing.text.html.HTML.Tag;

import org.apache.maven.doxia.markup.XmlMarkup;

/**
 * An abstract <code>Sink</code> for xml markup syntax.
 *
 * @author <a href="mailto:vincent.siveton@gmail.com">Vincent Siveton</a>
 * @version $Id: AbstractXmlSink.java 566741 2007-08-16 15:01:27Z ltheussl $
 * @since 1.0
 */
public abstract class AbstractXmlSink
    extends SinkAdapter
    implements XmlMarkup
{
    /**
     * Starts a Tag. For instance:
     * <pre>
     * &lt;tag&gt;
     * </pre>
     *
     * @param t a non null tag
     * @see #writeStartTag(javax.swing.text.html.HTML.Tag, javax.swing.text.MutableAttributeSet)
     */
    protected void writeStartTag( Tag t )
    {
        writeStartTag ( t, null );
    }

    /**
     * Starts a Tag with attributes. For instance:
     * <pre>
     * &lt;tag attName="attValue"&gt;
     * </pre>
     *
     * @param t a non null tag
     * @param att a set of attributes
     * @see #writeStartTag(javax.swing.text.html.HTML.Tag, javax.swing.text.MutableAttributeSet, boolean)
     */
    protected void writeStartTag( Tag t, MutableAttributeSet att )
    {
        writeStartTag ( t, att, false );
    }

    /**
     * Starts a Tag with attributes. For instance:
     * <pre>
     * &lt;tag attName="attValue"&gt;
     * </pre>
     *
     * @param t a non null tag
     * @param att a set of attributes
     * @param isSimpleTag boolean to write as a simple tag
     */
    protected void writeStartTag( Tag t, MutableAttributeSet att, boolean isSimpleTag )
    {
        if ( t == null )
        {
            throw new IllegalArgumentException( "A tag is required" );
        }

        StringBuffer sb = new StringBuffer();
        sb.append( String.valueOf( LESS_THAN ) );
        sb.append( t.toString() );

        if ( att != null )
        {
            Enumeration names = att.getAttributeNames();

            while ( names.hasMoreElements() )
            {
                Object key = names.nextElement();
                Object value = att.getAttribute( key );

                if ( value instanceof AttributeSet )
                {
                    // ignored
                }
                else
                {
                    sb.append( String.valueOf( SPACE ) ).append( key.toString() ).append( String.valueOf( EQUAL ) )
                        .append( String.valueOf( QUOTE ) ).append( value.toString() ).append( String.valueOf( QUOTE ) );
                }
            }
        }

        if ( isSimpleTag )
        {
            sb.append( String.valueOf( SPACE ) ).append( String.valueOf( SLASH ) );
        }

        sb.append( String.valueOf( GREATER_THAN ) );

        if ( isSimpleTag )
        {
            sb.append( EOL );
        }

        write( sb.toString() );
    }

    /**
     * Ends a Tag. For instance:
     * <pre>
     * &lt;/tag&gt;
     * </pre>
     *
     * @param t a tag
     */
    protected void writeEndTag( Tag t )
    {
        StringBuffer sb = new StringBuffer();
        sb.append( String.valueOf( LESS_THAN ) );
        sb.append( String.valueOf( SLASH ) );
        sb.append( t.toString() );
        sb.append( String.valueOf( GREATER_THAN ) );

        sb.append( EOL );

        write( sb.toString() );
    }

    /**
     * Starts a simple Tag. For instance:
     * <pre>
     * &lt;tag /&gt;
     * </pre>
     *
     * @param t a non null tag
     * @see #writeSimpleTag(javax.swing.text.html.HTML.Tag, javax.swing.text.MutableAttributeSet)
     */
    protected void writeSimpleTag( Tag t )
    {
        writeSimpleTag( t, null );
    }

    /**
     * Starts a simple Tag with attributes. For instance:
     * <pre>
     * &lt;tag attName="attValue" /&gt;
     * </pre>
     *
     * @param t a non null tag
     * @param att a set of attributes
     * @see #writeStartTag(javax.swing.text.html.HTML.Tag, javax.swing.text.MutableAttributeSet, boolean)
     */
    protected void writeSimpleTag( Tag t, MutableAttributeSet att )
    {
        writeStartTag ( t, att, true );
    }

    /**
     * TODO DOXIA-59 Need to uniform writing
     *
     * @param text the given text to write
     */
    protected abstract void write( String text );
}
