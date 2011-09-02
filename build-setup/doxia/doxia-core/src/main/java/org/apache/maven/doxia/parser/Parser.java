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

import org.apache.maven.doxia.sink.Sink;

import java.io.Reader;

/**
 * A Parser is responsible for parsing any document in a supported front-end
 * format, and emitting the standard Doxia events, which can then be consumed
 * by any Doxia Sink.
 *
 * @author <a href="mailto:jason@maven.org">Jason van Zyl</a>
 * @version $Id: Parser.java 564180 2007-08-09 12:15:44Z vsiveton $
 * @since 1.0
 */
public interface Parser
{
    /** The Plexus lookup role. */
    String ROLE = Parser.class.getName();

    /** Unknown parser type */
    int UNKNOWN_TYPE = 0;

    /** Text parser type */
    int TXT_TYPE = 1;

    /** XML parser type */
    int XML_TYPE = 2;

    /** Used for table cells: justify center. */
    int JUSTIFY_CENTER = 0;

    /** Used for table cells: justify left. */
    int JUSTIFY_LEFT = 1;

    /** Used for table cells: justify right. */
    int JUSTIFY_RIGHT = 2;

    /**
     * Parses the given source model and emits Doxia events into the given sink.
     *
     * @param source A reader that provides the source document.
     * @param sink A sink that consumes the Doxia events.
     * @throws ParseException if the model could not be parsed.
     */
    void parse( Reader source, Sink sink )
        throws ParseException;

    /**
     * The parser type value could be {@link #UNKNOWN_TYPE}, {@link #TXT_TYPE} or
     * {@link #XML_TYPE}.
     *
     * @return the type of Parser
     */
    int getType();
}
