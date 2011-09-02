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

import org.apache.maven.doxia.markup.TextMarkup;

/**
 * An abstract class that defines some convenience methods for <code>Text</code> parsers.
 *
 * @author <a href="mailto:vincent.siveton@gmail.com">Vincent Siveton</a>
 * @version $Id: AbstractTextParser.java 564180 2007-08-09 12:15:44Z vsiveton $
 * @since 1.0
 */
public abstract class AbstractTextParser
    extends AbstractParser
    implements TextMarkup
{
    /** {@inheritDoc} */
    public final int getType()
    {
        return TXT_TYPE;
    }
}
