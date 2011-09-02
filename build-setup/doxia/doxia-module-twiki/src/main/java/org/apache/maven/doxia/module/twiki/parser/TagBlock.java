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

import org.apache.maven.doxia.sink.Sink;
import org.apache.maven.doxia.macro.MacroRequest;
import org.apache.maven.doxia.macro.MacroExecutionException;
import org.apache.maven.doxia.macro.manager.MacroNotFoundException;
import org.apache.maven.doxia.module.twiki.TWikiParser;
import org.codehaus.plexus.util.StringUtils;

import java.util.Map;
import java.util.HashMap;
import java.io.File;


/**
 * Block that represents a link.
 *
 * @author Abhijit Bagri
 * @since May 13, 2008
 */
public class TagBlock implements Block
{
    /**
     * link reference
     */
    public int tag;

    public String html;

    private boolean isEndTag;

    private static final String BLOCKQUOTE_NAME = "BLOCKQUOTE";
    private static final String NOAUTOLINK_NAME = "NOAUTOLINK";
    private static final String VERBATIM_NAME = "VERBATIM";
    private static final String CODE_NAME = "CODE";

    private static final int BLOCKQUOTE = 1;
    private static final int HTML = 2;
    private static final int VERBATIM = 3;
    private static final int CODE = 4;
    private static final int PARTIAL_HTML = 5;
    private static final int NO_AUTOLINK = 6;

    /**
     * Creates the MacroBlock.
     * @param macroDef The definition of the macro. Everything that exists between '%'s
     */
    public TagBlock( final String tag, boolean isPartial)
        throws IllegalArgumentException
    {
        if ( tag == null )
        {
            throw new IllegalArgumentException( "tag cannot be null" );
        }
        String tagName;
        if(tag.startsWith("/")) {
            isEndTag = true;
            tagName = tag.substring(1);
        } else {
            tagName = tag;
        }

        if(tagName.equalsIgnoreCase(BLOCKQUOTE_NAME)) {
            this.tag = BLOCKQUOTE;
            if(isEndTag) {
                State.clearVerbatimMode();
            } else {
                State.setVerbatimMode();
            }
        } else if(tagName.equalsIgnoreCase(VERBATIM_NAME)) {
            this.tag = VERBATIM;
            if(isEndTag) {
                State.clearVerbatimMode();
            } else {
                State.setVerbatimMode();
            }
        } else if(tagName.equalsIgnoreCase(NOAUTOLINK_NAME)) {
            this.tag = NO_AUTOLINK;
            if(isEndTag) {
                State.enableAutoLinking();
            } else {
                State.disableAutoLinking();
            }
        } else if(tagName.equalsIgnoreCase(CODE_NAME)) {
            this.tag = CODE;
        } else {
            if(isPartial) {
                this.tag = PARTIAL_HTML;
            } else {
                this.tag = HTML;
            }
            html = tag;
        }

    }

    /**
     * @see Block#traverse(org.apache.maven.doxia.sink.Sink)
     */
    public final void traverse( final Sink sink )
    {
        if(isEndTag) {
            switch(tag) {
                case BLOCKQUOTE:
                    sink.verbatim_(); break;
                case VERBATIM:
                    sink.rawText("</pre>"); break;
                case CODE:
                    sink.monospaced_(); break;
                case HTML:
                    sink.rawText("<" + html + ">"); break;
                case PARTIAL_HTML:
                    sink.rawText(html); break;

            }
        } else {
            switch(tag) {
                case BLOCKQUOTE:
                    sink.verbatim(false); break;
                case VERBATIM:
                    sink.rawText("<pre>"); break;
                case CODE:
                    sink.monospaced(); break;
                case HTML:
                    sink.rawText("<" + html + ">"); break;
                case PARTIAL_HTML:
                    sink.rawText(html); break;

            }
        }
    }

    /**
     * @see Object#equals(Object)
     */

    public final boolean equals( final Object obj )
    {
        boolean ret = false;

        if ( obj == this )
        {
            ret = true;
        }
        else if ( obj instanceof TagBlock )
        {
            final TagBlock l = (TagBlock) obj;
            ret = tag ==l.tag;
        }

        return ret;
    }

    /**
     * @see Object#hashCode()
     */

    public final int hashCode()
    {
        final int magic1 = 17;
        final int magic2 = 37;

        return magic1 + magic2 * tag;
    }

}