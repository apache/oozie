package org.apache.maven.doxia.macro;

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

import java.util.Iterator;

/**
 * A simple macro that prints out the key and value of some supplied parameters.
 *
 * @plexus.component role-hint="echo"
 */
public class EchoMacro
    extends AbstractMacro
{
    /** System-dependent EOL. */
    private static final String EOL = System.getProperty( "line.separator" );

    /** {@inheritDoc} */
    public void execute( Sink sink, MacroRequest request )
    {
        sink.verbatim( true );

        sink.text( "echo" + EOL );

        for ( Iterator i = request.getParameters().keySet().iterator(); i.hasNext(); )
        {
            String key = (String) i.next();

            sink.text( key + " ---> " + request.getParameter( key ) + EOL );
        }

        sink.verbatim_();
    }
}
