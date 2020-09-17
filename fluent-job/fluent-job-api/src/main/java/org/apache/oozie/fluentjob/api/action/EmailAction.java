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

package org.apache.oozie.fluentjob.api.action;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A class representing the Oozie email action.
 * Instances of this class should be built using the builder {@link EmailActionBuilder}.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}.
 *
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link EmailActionBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class EmailAction extends Node {
    private final String to;
    private final String cc;
    private final String bcc;
    private final String subject;
    private final String body;
    private final String contentType;
    private final String attachment;

    EmailAction(final Node.ConstructionData constructionData,
                final String to,
                final String cc,
                final String bcc,
                final String subject,
                final String body,
                final String contentType,
                final String attachment) {
        super(constructionData);
        this.to = to;
        this.cc = cc;
        this.bcc = bcc;
        this.subject = subject;
        this.body = body;
        this.contentType = contentType;
        this.attachment = attachment;
    }

    /**
     * Returns the address of the recipient of the email.
     * @return The address of the recipient of the email.
     */
    public String getRecipient() {
        return to;
    }

    /**
     * Returns the address of the recipient of a copy of the email.
     * @return The address of the recipient of a copy of the email.
     */
    public String getCc() {
        return cc;
    }

    /**
     * Returns the address of the secret recipient of a copy of the email.
     * @return The address of the secret recipient of a copy of the email.
     */
    public String getBcc() {
        return bcc;
    }

    /**
     * Returns the subject of the email.
     * @return The subject of the email.
     */
    public String getSubject() {
        return subject;
    }

    /**
     * Returns the body of the email.
     * @return The body of the email.
     */
    public String getBody() {
        return body;
    }

    /**
     * Returns the content type of the email.
     * @return The content type of the email.
     */
    public String getContentType() {
        return contentType;
    }

    /**
     * Returns the attachment of the email.
     * @return The attachment of the email.
     */
    public String getAttachment() {
        return attachment;
    }
}
