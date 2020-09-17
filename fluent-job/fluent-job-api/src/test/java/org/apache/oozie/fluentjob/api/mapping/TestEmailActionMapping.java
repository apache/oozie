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

package org.apache.oozie.fluentjob.api.mapping;

import org.apache.oozie.fluentjob.api.action.EmailAction;
import org.apache.oozie.fluentjob.api.action.EmailActionBuilder;
import org.apache.oozie.fluentjob.api.generated.action.email.ACTION;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestEmailActionMapping {
    @Test
    public void testMappingEmailAction() {
        final String to = "recipient@something.com";
        final String cc = "cc@something.com";
        final String bcc = "bcc@something.com";
        final String subject = "Subject";
        final String body = "Email body.";
        final String contentType = "content_type";
        final String attachment = "attachment";

        final EmailAction action = EmailActionBuilder.create()
                .withName("email-action")
                .withRecipient(to)
                .withCc(cc)
                .withBcc(bcc)
                .withSubject(subject)
                .withBody(body)
                .withContentType(contentType)
                .withAttachment(attachment)
                .build();

        final ACTION emailAction = DozerBeanMapperSingleton.instance().map(action, ACTION.class);

        assertEquals(to, emailAction.getTo());
        assertEquals(cc, emailAction.getCc());
        assertEquals(bcc, emailAction.getBcc());
        assertEquals(subject, emailAction.getSubject());
        assertEquals(body, emailAction.getBody());
        assertEquals(contentType, emailAction.getContentType());
        assertEquals(attachment, emailAction.getAttachment());
    }
}
