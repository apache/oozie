package org.apache.maven.doxia.sink;

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

import org.codehaus.plexus.PlexusTestCase;

/**
 * Test for org.apache.maven.doxia.sink.StructureSink
 */
public class StructureSinkTest
    extends PlexusTestCase
{

    /**
     * Test for org.apache.maven.doxia.sink.StructureSink#isExternalLink
     */
    public void testIsExternalLink()
    {
        String link = "http://maven.apache.org/";
        assertTrue( "Should be an external link: " + link,
            StructureSink.isExternalLink( link ) );

        link = "https://maven.apache.org/";
        assertTrue( "Should be an external link: " + link,
            StructureSink.isExternalLink( link ) );

        link = "ftp:/maven.apache.org/";
        assertTrue( "Should be an external link: " + link,
            StructureSink.isExternalLink( link ) );

        link = "mailto:maven@apache.org";
        assertTrue( "Should be an external link: " + link,
            StructureSink.isExternalLink( link ) );

        link = "file:/index.html";
        assertTrue( "Should be an external link: " + link,
            StructureSink.isExternalLink( link ) );

        link = "./index.html";
        assertTrue( "Should be an external link: " + link,
            StructureSink.isExternalLink( link ) );

        link = "../index.html";
        assertTrue( "Should be an external link: " + link,
            StructureSink.isExternalLink( link ) );


        link = "file:\\index.html";
        assertFalse( "Should NOT be an external link: " + link,
            StructureSink.isExternalLink( link ) );

        link = ".\\index.html";
        assertFalse( "Should NOT be an external link: " + link,
            StructureSink.isExternalLink( link ) );

        link = "..\\index.html";
        assertFalse( "Should NOT be an external link: " + link,
            StructureSink.isExternalLink( link ) );
    }

}
