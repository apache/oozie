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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.maven.doxia.util.ByLineSource;
import org.apache.maven.doxia.parser.ParseException;


/**
 * Block that represents an horizontal rule
 *
 * @author Juan F. Codagnone
 * @since Nov 5, 2005
 */
public class HRuleBlockParser implements BlockParser
{
    /**
     * pattern used to detect horizontal rulers
     */
    private static final Pattern HRULE_PATTERN =
        Pattern.compile( "^(---)(-*)(.*)$" );

    /**
     * @see BlockParser#accept(String)
     */
    public final boolean accept( final String line )
    {
        final Matcher m = HRULE_PATTERN.matcher( line );
        boolean ret = false;

        if ( m.lookingAt() )
        {
            final int textGroup = 3;
            String s = m.group( textGroup );
            if ( s != null && !s.startsWith( "+" ) )
            {
                ret = true;
            }
        }

        return ret;
    }

    /**
     * @see BlockParser#visit(String, ByLineSource)
     */
    public final Block visit( final String line, final ByLineSource source )
        throws ParseException
    {
        Block ret = new HorizontalRuleBlock();
        final Matcher matcher = HRULE_PATTERN.matcher( line );
        if ( matcher.lookingAt() )
        {
            final int textGroup = 3;
            source.unget( matcher.group( textGroup ) );
        }
        else
        {
            throw new ParseException( "i was expecting a hruler!" );
        }

        return ret;
    }
}
