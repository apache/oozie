package org.apache.maven.doxia.parser;

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

import java.io.IOException;
import java.io.Reader;

import org.apache.maven.doxia.macro.MacroExecutionException;
import org.apache.maven.doxia.markup.XmlMarkup;
import org.apache.maven.doxia.sink.Sink;
import org.codehaus.plexus.util.xml.pull.MXParser;
import org.codehaus.plexus.util.xml.pull.XmlPullParser;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;

/**
 * An abstract class that defines some convenience methods for <code>XML</code> parsers.
 *
 * @author <a href="mailto:vincent.siveton@gmail.com">Vincent Siveton</a>
 * @version $Id: AbstractXmlParser.java 567665 2007-08-20 12:23:16Z ltheussl $
 * @since 1.0
 */
public abstract class AbstractXmlParser
    extends AbstractParser
    implements XmlMarkup
{
    /** {@inheritDoc} */
    public void parse( Reader source, Sink sink )
        throws ParseException
    {
        try
        {
            XmlPullParser parser = new MXParser();

            parser.setInput( source );

            parseXml( parser, sink );
        }
        catch ( XmlPullParserException ex )
        {
            throw new ParseException( "Error parsing the model: " + ex.getMessage(), ex );
        }
        catch ( MacroExecutionException ex )
        {
            throw new ParseException( "Macro execution failed: " + ex.getMessage(), ex );
        }
    }

    /** {@inheritDoc} */
    public final int getType()
    {
        return XML_TYPE;
    }

    /**
     * Parse the model from the XmlPullParser into the given sink.
     *
     * @param parser A parser.
     * @param sink the sink to receive the events.
     * @throws XmlPullParserException if there's a problem parsing the model
     * @throws MacroExecutionException if there's a problem executing a macro
     */
    private void parseXml( XmlPullParser parser, Sink sink )
        throws XmlPullParserException, MacroExecutionException
    {
        int eventType = parser.getEventType();

        while ( eventType != XmlPullParser.END_DOCUMENT )
        {
            if ( eventType == XmlPullParser.START_TAG )
            {
                handleStartTag( parser, sink );
            }
            else if ( eventType == XmlPullParser.END_TAG )
            {
                handleEndTag( parser, sink );
            }
            else if ( eventType == XmlPullParser.TEXT )
            {
                handleText( parser, sink );
            }
            else if ( eventType == XmlPullParser.CDSECT )
            {
                // TODO: handle CDATA sections
                // handleCdsect( parser, sink );
            }
            else if ( eventType == XmlPullParser.COMMENT )
            {
                // TODO: handle comments, see DOXIA-137
                // handleComment( parser, sink );
            }
            else if ( eventType == XmlPullParser.ENTITY_REF )
            {
                // TODO: handle entities
                // handleEntity( parser, sink );
            }

            try
            {
                // TODO: use nextToken() to report CDSECT, COMMENT and ENTITY_REF
                eventType = parser.next();
            }
            catch ( IOException io )
            {
                throw new XmlPullParserException( "IOException: " + io.getMessage(), parser, io );
            }
        }
    }

    /**
     * Goes through the possible start tags.
     *
     * @param parser A parser.
     * @param sink the sink to receive the events.
     * @throws XmlPullParserException if there's a problem parsing the model
     * @throws MacroExecutionException if there's a problem executing a macro
     */
    protected abstract void handleStartTag( XmlPullParser parser, Sink sink )
        throws XmlPullParserException, MacroExecutionException;

    /**
     * Goes through the possible end tags.
     *
     * @param parser A parser.
     * @param sink the sink to receive the events.
     * @throws XmlPullParserException if there's a problem parsing the model
     * @throws MacroExecutionException if there's a problem executing a macro
     */
    protected abstract void handleEndTag( XmlPullParser parser, Sink sink )
        throws XmlPullParserException, MacroExecutionException;

    /**
     * Handles text events.
     *
     * @param parser A parser.
     * @param sink the sink to receive the events.
     * @throws XmlPullParserException if there's a problem parsing the model
     */
    protected abstract void handleText( XmlPullParser parser, Sink sink )
        throws XmlPullParserException;
}
