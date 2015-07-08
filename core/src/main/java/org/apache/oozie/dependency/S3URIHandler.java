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

package org.apache.oozie.dependency;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.action.hadoop.LauncherURIHandler;
import org.apache.oozie.action.hadoop.S3LauncherURIHandler;
import org.apache.oozie.dependency.s3.S3Utils;
import org.apache.oozie.util.XLog;

import java.net.URI;
import java.util.List;
import java.util.Set;

public class S3URIHandler implements URIHandler {

    private static final XLog LOG = XLog.getLog(S3URIHandler.class);
    private static final Set<String> SUPPORTED_SCHEMES = Sets.newHashSet("s3", "s3n");

    // private AWSCredentials awsCredentials;
    private List<Class<?>> classesToShip;

    @Override
    public void init(Configuration conf)
    {
        /*
        LOG.info("Reading AWS credentials from Configuration file");

        final String awsAccessKeyId = conf.get(S3Utils.AWS_ACCESS_KEY_ID);
        final String awsSecretKey = conf.get(S3Utils.AWS_SECRET_KEY);

        Preconditions.checkArgument(!awsAccessKeyId.isEmpty());
        Preconditions.checkArgument(!awsSecretKey.isEmpty());

        awsCredentials = new BasicAWSCredentials(awsAccessKeyId, awsSecretKey);
        */
        classesToShip = new S3LauncherURIHandler().getClassesForLauncher();
    }

    @Override
    public Set<String> getSupportedSchemes()
    {
        return SUPPORTED_SCHEMES;
    }

    @Override
    public Class<? extends LauncherURIHandler> getLauncherURIHandlerClass()
    {
        return S3LauncherURIHandler.class;
    }

    @Override
    public List<Class<?>> getClassesForLauncher()
    {
        return classesToShip;
    }

    @Override
    public DependencyType getDependencyType(URI uri)
        throws URIHandlerException
    {
        return DependencyType.PULL;
    }

    @Override
    public void registerForNotification(URI uri, Configuration conf, String user, String actionID)
        throws URIHandlerException
    {
        throw new UnsupportedOperationException("Notifications are not supported for " + uri.getScheme());
    }

    @Override
    public boolean unregisterFromNotification(URI uri, String actionID)
    {
        throw new UnsupportedOperationException("Notifications are not supported for " + uri.getScheme());
    }

    @Override
    public Context getContext(URI uri, Configuration conf, String user, boolean readOnly)
        throws URIHandlerException
    {
        return new S3Context(conf, user);
    }

    @Override
    public boolean exists(URI uri, Context context)
        throws URIHandlerException
    {
        return S3Utils.exists(
            S3Utils.getAmazonS3URI(uri),
            ((S3Context) context).getAmazonS3()
        );
    }

    @Override
    public boolean exists(URI uri, Configuration conf, String user)
        throws URIHandlerException
    {
        return S3Utils.exists(
            S3Utils.getAmazonS3URI(uri),
            ((S3Context) getContext(S3Utils.getNormalizedURI(uri), conf, user, true)).getAmazonS3()
        );
    }

    @Override
    public void delete(URI uri, Context context)
        throws URIHandlerException
    {
        try
        {
            final AmazonS3URI amazonS3URI = S3Utils.getAmazonS3URI(uri);
            final AmazonS3 amazonS3 = ((S3Context) context).getAmazonS3();

            if (uri.getPath().endsWith("/"))    // this is a directory
            {
                LOG.debug("Removing S3 directory [{0}]", amazonS3URI.toString());

                ObjectListing fileList = amazonS3.listObjects(amazonS3URI.getBucket(), amazonS3URI.getKey());
                // remove all files in folder
                for (S3ObjectSummary file : fileList.getObjectSummaries())
                {
                    amazonS3.deleteObject(amazonS3URI.getBucket(), file.getKey());
                }
                amazonS3.deleteObject(amazonS3URI.getBucket(), amazonS3URI.getKey());   // remove folder
            }
            else
            {
                // remove single object
                LOG.debug("Removing S3 object [{0}]", amazonS3URI.toString());

                amazonS3.deleteObject(amazonS3URI.getBucket(), amazonS3URI.getKey());
            }
        }
        catch (AmazonS3Exception ex)
        {
            LOG.error(ex);

            throw new URIHandlerException(ErrorCode.E0907, uri);
        }
    }

    @Override
    public void delete(URI uri, Configuration conf, String user)
        throws URIHandlerException
    {
        final URI normalizedURI = S3Utils.getNormalizedURI(uri);
        delete(normalizedURI, getContext(normalizedURI, conf, user, false));
    }

    @Override
    public String getURIWithDoneFlag(String uri, String doneFlag)
        throws URIHandlerException
    {
        if (doneFlag.length() > 0) {
            uri += "/" + doneFlag;
        }
        return uri;
    }

    @Override
    public void validate(String uri)
        throws URIHandlerException
    {
        // do nothing
    }

    @Override
    public void destroy()
    {
        // do nothing
    }

    static class S3Context extends Context {

        private final AmazonS3Client amazonS3;

        /**
         * Create a S3Context that can be used to access an S3 URI
         *
         * @param conf Configuration to access the URI
         * @param user name of the user the URI should be accessed as
         */
        public S3Context(Configuration conf, String user) {
            super(conf, user);

            amazonS3 = new AmazonS3Client();
            Preconditions.checkNotNull(amazonS3);
        }

        public AmazonS3Client getAmazonS3()
        {
            return amazonS3;
        }
    }
}
