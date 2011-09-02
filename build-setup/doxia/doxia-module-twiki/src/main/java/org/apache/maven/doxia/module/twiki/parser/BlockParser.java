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

/**
 * Parse a twiki syntax block
 *
 * @author Juan F. Codagnone
 * @since Nov 1, 2005
 */
public interface BlockParser
{

    /**
     * @param line text line
     * @return <code>true</code> if this class can handle this line
     */
    boolean accept( String line );

    /**
     * @param line   a line of text
     * @param source the source of lines
     * @return a block
     * @throws ParseException on error
     */
    Block visit( String line, ByLineSource source ) throws ParseException;

}
