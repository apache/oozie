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

/**
 * Base interface of a macro.
 *
 * @author <a href="mailto:jason@maven.org">Jason van Zyl</a>
 * @version $Id: Macro.java 567311 2007-08-18 18:30:54Z vsiveton $
 * @since 1.0
 */
public interface Macro
{
    /** The Plexus lookup role. */
    String ROLE = Macro.class.getName();

    /**
     * Execute the current macro using the given MacroRequest,
     * and emit events into the given sink.
     *
     * @param sink The sink to receive the events.
     * @param request The corresponding MacroRequest.
     * @throws MacroExecutionException if an error occurred during execution.
     */
    void execute( Sink sink, MacroRequest request )
        throws MacroExecutionException;
}
