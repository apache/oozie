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
public class MacroBlock implements Block
{
    /**
     * link reference
     */
    public final String macro;

    public final String lineAfterMacro;

    private String sourceContent;

    private TWikiParser parser;

    /**
     * Creates the MacroBlock.
     * @param macroDef The definition of the macro. Everything that exists between '%'s
     */
    public MacroBlock( final String macroDef, String sourceContent, TWikiParser parser, String restLine )
        throws IllegalArgumentException
    {
        if ( macroDef == null )
        {
            throw new IllegalArgumentException( "macro cannot be null" );
        }
        macro = macroDef;
        if(macro.startsWith("CODE")) {
            State.setVerbatimMode();
        }
        if(macro.startsWith("ENDCODE")) {
            State.clearVerbatimMode();            
        }

        this.parser = parser;
        this.sourceContent = sourceContent;
        lineAfterMacro = restLine;
    }

    /**
     * @see org.apache.maven.doxia.module.twiki.parser.Block#traverse(org.apache.maven.doxia.sink.Sink)
     */
    public final void traverse( final Sink sink )
    {
//        sink.text("Macro works :) macro=" + macro);
        if ( parser.isSecondParsing() )
        {
           return;
        }

        String s = macro;

        s = escapeForMacro( s );

        String[] params = StringUtils.split( s, "{" );

        String macroId = params[0].toLowerCase();

        Map parameters = new HashMap();

        if(params.length > 1) {
            parameters = getParams(params[1]);
        }

        parameters.put( "sourceContent", sourceContent );

        TWikiParser twikiParser = new TWikiParser();
        twikiParser.setSecondParsing( true );
        parameters.put( "parser", twikiParser );
        parameters.put("afterMacroLine", lineAfterMacro);

        MacroRequest request = new MacroRequest(parameters, getBasedir() );
        try
        {
            parser.executeMacro( macroId, request, sink );
        }
        catch ( MacroExecutionException e )
        {
            throw new IllegalArgumentException("Unable to execute macro in the Twiki document", e );
        }
        catch ( MacroNotFoundException e )
        {
            throw new IllegalArgumentException( "Unable to find macro used in the Twiki document", e );
        }
    }


    private Map getParams(String s) {

        Map map = new HashMap();
        String paramList  = s.trim();
        if(paramList.length() > 0) {
            if(!paramList.endsWith("}")) {
                throw new IllegalArgumentException("No ending } for macro: " + s);
            }
            /* The format will be "xyz" a = "b" c = "d" */
            // For the first one, there may be no param name, we put it as default
            int beginIndex = paramList.indexOf("\"") + 1;
            int endIndex = -1;
            if(beginIndex == 1) {
                /* the default value */
                endIndex = paramList.indexOf("\"",beginIndex);
                String value = paramList.substring(beginIndex, endIndex);
                map.put("default", value);
                beginIndex =  paramList.indexOf("\"", endIndex + 1) + 1;
            } 
            while(beginIndex > 0 && endIndex < paramList.length() - 1) {
               int beginEquals = paramList.indexOf("=", endIndex);
               String param =  paramList.substring(endIndex + 1, beginEquals).trim();
               endIndex = paramList.indexOf("\"",beginIndex);
               String value = paramList.substring(beginIndex, endIndex);
               map.put(param, value);
               beginIndex =  paramList.indexOf("\"", endIndex + 1) + 1;
            }

        }
        return map;
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
        else if ( obj instanceof MacroBlock )
        {
            final MacroBlock l = (MacroBlock) obj;
            ret = macro.equals( l.macro );
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

        return magic1 + magic2 * macro.hashCode();
    }

    /**
     * escapeForMacro
     *
     * @param s String
     * @return String
     */
    private String escapeForMacro( String s )
    {
        if ( s == null || s.length() < 1 )
        {
            return s;
        }

        String result = s;

        // use some outrageously out-of-place chars for text
        // (these are device control one/two in unicode)
        result = StringUtils.replace( result, "\\=", "\u0011" );
        result = StringUtils.replace( result, "\\|", "\u0012" );

        return result;
    }

    /**
     * unescapeForMacro
     *
     * @param s String
     * @return String
     */
    private String unescapeForMacro( String s )
    {
        if ( s == null || s.length() < 1 )
        {
            return s;
        }

        String result = s;

        result = StringUtils.replace( result, "\u0011", "=" );
        result = StringUtils.replace( result, "\u0012", "|" );

        return result;
    }

    /**
     * Returns the current base directory.
     *
     * @return The base directory.
     */
    protected File getBasedir()
    {
        // TODO: This is baaad, it should come in with the request.

        String basedir = System.getProperty( "basedir" );

        if ( basedir != null )
        {
            return new File( basedir );
        }

        return new File( new File( "" ).getAbsolutePath() );
    }
}