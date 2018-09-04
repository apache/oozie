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

package org.apache.oozie.dependency.s3;

import com.amazonaws.services.s3.AmazonS3URI;
import junit.framework.TestCase;
import org.apache.oozie.dependency.URIHandlerException;

import java.net.URI;

public class TestS3Utils
    extends TestCase
{
    public void testGetNormalizedURI_1()
        throws Exception
    {
        URI uri = S3Utils.getNormalizedURI(new URI("s3n://my-bucket//my-folder//2015/01/01//"));

        assertEquals(uri.toString(), "s3://my-bucket/my-folder/2015/01/01/");
        assertEquals(uri.getScheme(), "s3");
        assertEquals(uri.getHost(), "my-bucket");
        assertEquals(uri.getPath(), "/my-folder/2015/01/01/");

        System.out.println(uri.toString());
    }

    public void testGetNormalizedURI_2()
        throws Exception
    {
        try
        {
            URI uri = S3Utils.getNormalizedURI(new URI("s2n://my-bucket//my-folder//2015/01/01//"));

            assertTrue("This test should throw, but it didn't", false);
        }
        catch (URIHandlerException ex)
        {
            assertTrue(true);
        }
    }

    public void testGetAmazonS3URI_1()
        throws Exception
    {
        AmazonS3URI uri = S3Utils.getAmazonS3URI(new URI("s3n://my-bucket//my-folder//2015/01/01//"));

        assertEquals(uri.toString(), "s3://my-bucket/my-folder/2015/01/01/");
        assertEquals(uri.getBucket(), "my-bucket");

        // PLEASE: not the difference between getKey on AmazonS3URI and getPath() on a standard URI
        assertEquals(uri.getKey(), "my-folder/2015/01/01/");

        System.out.println(uri.toString());
    }
}