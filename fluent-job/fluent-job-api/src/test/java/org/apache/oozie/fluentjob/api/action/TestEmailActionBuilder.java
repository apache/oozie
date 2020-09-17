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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestEmailActionBuilder extends TestNodeBuilderBaseImpl<EmailAction, EmailActionBuilder> {
    @Override
    protected EmailActionBuilder getBuilderInstance() {
        return EmailActionBuilder.create();
    }

    @Override
    protected EmailActionBuilder getBuilderInstance(EmailAction action) {
        return EmailActionBuilder.createFromExistingAction(action);
    }

    @Test
    public void testRecipientAdded() {
        final String recipient = "recipient@something.com";

        final EmailActionBuilder builder = getBuilderInstance();
        builder.withRecipient(recipient);

        final EmailAction emailAction = builder.build();
        assertEquals(recipient, emailAction.getRecipient());
    }

    @Test
    public void testRecipientAddedTwiceThrows() {
        final EmailActionBuilder builder = getBuilderInstance();
        builder.withRecipient("some.recipient@something.com");

        expectedException.expect(IllegalStateException.class);
        builder.withRecipient("any.recipient@something.com");
    }

    @Test
    public void testCcAdded() {
        final String cc = "recipient@something.com";

        final EmailActionBuilder builder = getBuilderInstance();
        builder.withCc(cc);

        final EmailAction emailAction = builder.build();
        assertEquals(cc, emailAction.getCc());
    }

    @Test
    public void testCcAddedTwiceThrows() {
        final EmailActionBuilder builder = getBuilderInstance();
        builder.withCc("some.recipient@something.com");

        expectedException.expect(IllegalStateException.class);
        builder.withCc("any.recipient@something.com");
    }

    @Test
    public void testBccAdded() {
        final String bcc = "recipient@something.com";

        final EmailActionBuilder builder = getBuilderInstance();
        builder.withBcc(bcc);

        final EmailAction emailAction = builder.build();
        assertEquals(bcc, emailAction.getBcc());
    }

    @Test
    public void testBccAddedTwiceThrows() {
        final EmailActionBuilder builder = getBuilderInstance();
        builder.withBcc("some.recipient@something.com");

        expectedException.expect(IllegalStateException.class);
        builder.withBcc("any.recipient@something.com");
    }

    @Test
    public void testSubjectAdded() {
        final String subject = "Subject";

        final EmailActionBuilder builder = getBuilderInstance();
        builder.withSubject(subject);

        final EmailAction emailAction = builder.build();
        assertEquals(subject, emailAction.getSubject());
    }

    @Test
    public void testSubjectAddedTwiceThrows() {
        final EmailActionBuilder builder = getBuilderInstance();
        builder.withSubject("Subject");

        expectedException.expect(IllegalStateException.class);
        builder.withSubject("Any subject");
    }

    @Test
    public void testBodyAdded() {
        final String body = "Email body.";

        final EmailActionBuilder builder = getBuilderInstance();
        builder.withBody(body);

        final EmailAction emailAction = builder.build();
        assertEquals(body, emailAction.getBody());
    }

    @Test
    public void testBodyAddedTwiceThrows() {
        final EmailActionBuilder builder = getBuilderInstance();
        builder.withBody("Email body.");

        expectedException.expect(IllegalStateException.class);
        builder.withBody("Any email body.");
    }

    @Test
    public void testContentTypeAdded() {
        final String contentType = "content_type";

        final EmailActionBuilder builder = getBuilderInstance();
        builder.withContentType(contentType);

        final EmailAction emailAction = builder.build();
        assertEquals(contentType, emailAction.getContentType());
    }

    @Test
    public void testContentTypeAddedTwiceThrows() {
        final EmailActionBuilder builder = getBuilderInstance();
        builder.withContentType("content_type");

        expectedException.expect(IllegalStateException.class);
        builder.withContentType("any_content_type");
    }

    @Test
    public void testAttachmentAdded() {
        final String attachment = "attachment";

        final EmailActionBuilder builder = getBuilderInstance();
        builder.withAttachment(attachment);

        final EmailAction emailAction = builder.build();
        assertEquals(attachment, emailAction.getAttachment());
    }

    @Test
    public void testAttachmentAddedTwiceThrows() {
        final EmailActionBuilder builder = getBuilderInstance();
        builder.withAttachment("attachment");

        expectedException.expect(IllegalStateException.class);
        builder.withAttachment("any_attachment");
    }

    @Test
    public void testFromOtherAction() {
        final ShellAction parent = ShellActionBuilder.create()
                .withName("parent")
                .withExecutable("echo")
                .build();

        final ShellAction otherAction = ShellActionBuilder.createFromExistingAction(parent)
                .withName("shell")
                .withParent(parent)
                .build();

        final EmailAction fromOtherAction = EmailActionBuilder.createFromExistingAction(otherAction)
                .withName("email")
                .withBody("body")
                .build();

        assertEquals(parent, fromOtherAction.getParentsWithoutConditions().get(0));
        assertEquals("email", fromOtherAction.getName());
        assertEquals("body", fromOtherAction.getBody());
    }
}
