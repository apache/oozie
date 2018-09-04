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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.dependency.URIHandlerException;
import org.apache.oozie.util.XLog;

import java.net.URI;
import java.net.URISyntaxException;

public class S3Utils
{
    private static final XLog LOG = XLog.getLog(S3Utils.class);

    /*
    public static final String AWS_ACCESS_KEY_ID = "AWSAccessKeyId";
    public static final String AWS_SECRET_KEY = "AWSSecretKey";
    */

    static public URI getNormalizedURI(URI uri)
        throws URIHandlerException
    {
        String scheme = uri.getScheme().substring(0, 2).toLowerCase();
        if (scheme.compareTo("s3") != 0)
        {
            throw new URIHandlerException(ErrorCode.E0906, uri.toString());
        }

        try
        {
            return new URI(scheme + "://" + uri.getHost() + uri.getPath().replace("//", "/"));
        }
        catch (URISyntaxException ex)
        {
            throw new URIHandlerException(ErrorCode.E0906, uri.toString());
        }
    }

    static public AmazonS3URI getAmazonS3URI(URI uri)
        throws URIHandlerException
    {
        return new AmazonS3URI(getNormalizedURI(uri));
    }

    static private boolean doesMatch(String commonPrefix, String targetPrefix)
    {
        return commonPrefix.compareTo(targetPrefix) == 0;
    }

    static public boolean exists(AmazonS3URI amazonS3URI, AmazonS3 amazonS3)
    {
        LOG.trace("boolean S3URIHandler::exists([{0}], [{1}])", amazonS3URI.toString(), amazonS3.getClass().toString());

        final String targetPrefix = amazonS3URI.getKey();

        final ObjectListing objectListing = amazonS3
            .listObjects(
                new ListObjectsRequest()
                    .withBucketName(amazonS3URI.getBucket())
                    .withPrefix(targetPrefix.endsWith("/") ? targetPrefix.substring(0, targetPrefix.length() - 1) : targetPrefix)
                    .withDelimiter("/")
            );

        for (String commonPrefix : objectListing.getCommonPrefixes())
        {
            if (doesMatch(commonPrefix, targetPrefix))
            {
                LOG.debug("Found match for [{0}] in prefix [{1}]", amazonS3URI.toString(), commonPrefix);
                return true;
            }
        }

        for (S3ObjectSummary summary : objectListing.getObjectSummaries())
        {
            if (doesMatch(summary.getKey(), targetPrefix))
            {
                LOG.debug("Found match for [{0}] in S3ObjectSummary [{1}]", amazonS3URI.toString(), summary.getKey());
                return true;
            }
        }

        return false;
    }
}
