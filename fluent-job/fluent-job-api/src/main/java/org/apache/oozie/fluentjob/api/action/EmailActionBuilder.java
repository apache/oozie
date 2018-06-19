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
import org.apache.oozie.fluentjob.api.ModifyOnce;

/**
 * A builder class for {@link EmailAction}.
 * <p>
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}. The properties that are lists are an exception to this rule, of course multiple
 * elements can be added / removed.
 * <p>
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link EmailActionBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class EmailActionBuilder extends NodeBuilderBaseImpl<EmailActionBuilder> implements Builder<EmailAction> {
    private final ModifyOnce<String> to;
    private final ModifyOnce<String> cc;
    private final ModifyOnce<String> bcc;
    private final ModifyOnce<String> subject;
    private final ModifyOnce<String> body;
    private final ModifyOnce<String> contentType;
    private final ModifyOnce<String> attachment;

    /**
     * Creates and returns an empty builder.
     * @return An empty builder.
     */
    public static EmailActionBuilder create() {
        final ModifyOnce<String> to = new ModifyOnce<>();
        final ModifyOnce<String> cc = new ModifyOnce<>();
        final ModifyOnce<String> bcc = new ModifyOnce<>();
        final ModifyOnce<String> subject = new ModifyOnce<>();
        final ModifyOnce<String> body = new ModifyOnce<>();
        final ModifyOnce<String> contentType = new ModifyOnce<>();
        final ModifyOnce<String> attachment = new ModifyOnce<>();

        return new EmailActionBuilder(
                null,
                to,
                cc,
                bcc,
                subject,
                body,
                contentType,
                attachment);
    }

    /**
     * Create and return a new {@link EmailActionBuilder} that is based on an already built
     * {@link EmailAction} object. The properties of the builder will initially be the same as those of the
     * provided {@link EmailAction} object, but it is possible to modify them once.
     * @param action The {@link EmailAction} object on which this {@link EmailActionBuilder} will be based.
     * @return A new {@link EmailActionBuilder} that is based on a previously built {@link EmailAction} object.
     */
    public static EmailActionBuilder createFromExistingAction(final EmailAction action) {
        final ModifyOnce<String> to = new ModifyOnce<>(action.getRecipient());
        final ModifyOnce<String> cc = new ModifyOnce<>(action.getCc());
        final ModifyOnce<String> bcc = new ModifyOnce<>(action.getBcc());
        final ModifyOnce<String> subject = new ModifyOnce<>(action.getSubject());
        final ModifyOnce<String> body = new ModifyOnce<>(action.getBody());
        final ModifyOnce<String> contentType = new ModifyOnce<>(action.getContentType());
        final ModifyOnce<String> attachment = new ModifyOnce<>(action.getAttachment());

        return new EmailActionBuilder(
                action,
                to,
                cc,
                bcc,
                subject,
                body,
                contentType,
                attachment);
    }

    public static EmailActionBuilder createFromExistingAction(final Node action) {
        final ModifyOnce<String> to = new ModifyOnce<>();
        final ModifyOnce<String> cc = new ModifyOnce<>();
        final ModifyOnce<String> bcc = new ModifyOnce<>();
        final ModifyOnce<String> subject = new ModifyOnce<>();
        final ModifyOnce<String> body = new ModifyOnce<>();
        final ModifyOnce<String> contentType = new ModifyOnce<>();
        final ModifyOnce<String> attachment = new ModifyOnce<>();

        return new EmailActionBuilder(
                action,
                to,
                cc,
                bcc,
                subject,
                body,
                contentType,
                attachment);
    }

    EmailActionBuilder(final Node action,
                       final ModifyOnce<String> to,
                       final ModifyOnce<String> cc,
                       final ModifyOnce<String> bcc,
                       final ModifyOnce<String> subject,
                       final ModifyOnce<String> body,
                       final ModifyOnce<String> contentType,
                       final ModifyOnce<String> attachment) {
        super(action);
        this.to = to;
        this.cc = cc;
        this.bcc = bcc;
        this.subject = subject;
        this.body = body;
        this.contentType = contentType;
        this.attachment = attachment;
    }

    /**
     * Sets the address of the recipient of the email.
     * @param to the recipient in To: field
     * @return This builder.
     */
    public EmailActionBuilder withRecipient(final String to) {
        this.to.set(to);
        return this;
    }

    /**
     * Sets the address of the recipient of a copy of the email.
     * @param cc the recipient in CC: field
     * @return This builder.
     */
    public EmailActionBuilder withCc(final String cc) {
        this.cc.set(cc);
        return this;
    }

    /**
     * Sets the address of the secret recipient of a copy of the email.
     * @param bcc the recipient in BCC: field
     * @return This builder.
     */
    public EmailActionBuilder withBcc(final String bcc) {
        this.bcc.set(bcc);
        return this;
    }

    /**
     * Sets the subject of the email.
     * @param subject the email subject
     * @return This builder.
     */
    public EmailActionBuilder withSubject(final String subject) {
        this.subject.set(subject);
        return this;
    }

    /**
     * Sets the body of the email.
     * @param body the email body
     * @return This builder.
     */
    public EmailActionBuilder withBody(final String body) {
        this.body.set(body);
        return this;
    }

    /**
     * Sets the content type of the email.
     * @param contentType the email content type
     * @return This builder
     */
    public EmailActionBuilder withContentType(final String contentType) {
        this.contentType.set(contentType);
        return this;
    }

    /**
     * Sets the attachment of the email.
     * @param attachment the email attachment path
     * @return This builder.
     */
    public EmailActionBuilder withAttachment(final String attachment) {
        this.attachment.set(attachment);
        return this;
    }

    /**
     * Creates a new {@link EmailAction} object with the properties stores in this builder.
     * The new {@link EmailAction} object is independent of this builder and the builder can be used to build
     * new instances.
     * @return A new {@link EmailAction} object with the properties stored in this builder.
     */
    @Override
    public EmailAction build() {
        final Node.ConstructionData constructionData = getConstructionData();

        final EmailAction instance = new EmailAction(
                constructionData,
                to.get(),
                cc.get(),
                bcc.get(),
                subject.get(),
                body.get(),
                contentType.get(),
                attachment.get());

        addAsChildToAllParents(instance);

        return instance;
    }

    @Override
    protected EmailActionBuilder getRuntimeSelfReference() {
        return this;
    }
}
