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

import junit.framework.TestCase;

import org.apache.maven.doxia.module.twiki.TWikiParser;


/**
 * Common code to the Block unit tests
 *
 * @author Juan F. Codagnone
 * @since Nov 1, 2005
 */
public abstract class AbstractBlockTestCase extends TestCase
{
    /**
     * sectionParser to use in all the tests
     */
    protected final SectionBlockParser sectionParser = new SectionBlockParser();
    /**
     * ParagraphBlockParser  to use in all the tests
     */
    protected final ParagraphBlockParser paraParser =
        new ParagraphBlockParser();
    /**
     * ListBlockParser used in all the tests
     */
    protected final GenericListBlockParser listParser =
        new GenericListBlockParser();
    /**
     * FormatedTextParser used in all the tests
     */
    protected final FormatedTextParser formatTextParser =
        new FormatedTextParser();
    /**
     * TextParser used in all the tests
     */
    protected final TextParser textParser = new TextParser();
    /**
     * TextParser used in all the tests
     */
    protected final HRuleBlockParser hruleParser = new HRuleBlockParser();
    /**
     * TableBlockParser used in all the tests
     */
    protected final TableBlockParser tableParser = new TableBlockParser();
    /**
     * TWiki used in all the tests
     */
    protected final TWikiParser twikiParser = new TWikiParser();

    /**
     * Creates the AbstractBlockTestCase.
     */
    public AbstractBlockTestCase()
    {
        sectionParser.setParaParser( paraParser );
        sectionParser.setHrulerParser( hruleParser );
        paraParser.setSectionParser( sectionParser );
        paraParser.setListParser( listParser );
        paraParser.setTextParser( formatTextParser );
        paraParser.setHrulerParser( hruleParser );
        paraParser.setTableBlockParser( tableParser );
        listParser.setTextParser( formatTextParser );
        formatTextParser.setTextParser( textParser );
        tableParser.setTextParser( formatTextParser );
    }
}
