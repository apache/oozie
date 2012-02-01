/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.command;

/**
 * Transition command for update status.
 */
public abstract class StatusUpdateXCommand extends XCommand<Void>{

    /**
     * The constructor for abstract class {@link StatusUpdateXCommand}
     *
     * @param name the command name
     * @param type the command type
     * @param priority the command priority
     */
    public StatusUpdateXCommand(String name, String type, int priority) {
        super(name, type, priority);
    }

    /**
     * The constructor for abstract class {@link StatusUpdateXCommand}
     *
     * @param name the command name
     * @param type the command type
     * @param priority the command priority
     * @param dryrun true if dryrun is enable
     */
    public StatusUpdateXCommand(String name, String type, int priority, boolean dryrun) {
        super(name, type, priority, dryrun);
    }
}
