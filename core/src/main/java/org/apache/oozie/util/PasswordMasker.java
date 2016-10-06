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

package org.apache.oozie.util;

import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A generic password masker that masks {@code Map<String, String>} values given that its keys are considered password keys.
 * <p/>
 * Tested with {@see System#getProperties()} and {@see System#getenv()}.
 */
class PasswordMasker {

    /**
     * The mask that is applied to recognized passwords.
     **/
    private static final String PASSWORD_MASK = "*****";

    /**
     * A key is considered a password key, if it contains {{pass}}, case ignored.
     **/
    private static final String PASSWORD_KEY = "pass";

    /**
     * Tells us whether an OS environment variable that contains a password fragment.
     * <p/>
     * E.g. {{-Djavax.net.ssl.trustStorePassword=password}} from {{$CATALINA_OPTS}}.
     **/
    private static final String REGEX_CONTAINING_PASSWORD_FRAGMENT_OS_ENV_STYLE =
            ".*[((\\s)+-[D|X][\\w[.\\w]*]*(?i)pass[\\w[.\\w]*]*=)([\\w]+)]+.*";

    /**
     * Extracts a password fragment from an OS environment variable. Can be used iteratively to get all fragments.
     * <p/>
     * E.g. {{-Doozie.https.keystore.pass=password}} and {{-Djavax.net.ssl.trustStorePassword=password}} from {{$CATALINA_OPTS}}.
     * {@see java.util.Matcher#find()}
     **/
    private static final String REGEX_EXTRACTING_PASSWORD_FRAGMENTS_OS_ENV_STYLE =
            "((\\s)+-[D|X][\\w[.\\w]*]*(?i)pass[\\w[.\\w]*]*=)([\\w]+)";

    private static final Pattern PATTERN_CONTAINING_PASSWORD_FRAGMENTS = Pattern
            .compile(REGEX_CONTAINING_PASSWORD_FRAGMENT_OS_ENV_STYLE);

    private static final Pattern PATTERN_EXTRACTING_PASSWORD_FRAGMENTS = Pattern
            .compile(REGEX_EXTRACTING_PASSWORD_FRAGMENTS_OS_ENV_STYLE);

    Map<String, String> mask(Map<String, String> unmasked) {
        return Maps.transformEntries(unmasked, new Maps.EntryTransformer<String, String, String>() {
            @Override
            public String transformEntry(@Nullable String key, @Nullable String value) {
                checkNotNull(key, "key has to be set");
                checkNotNull(value, "value has to be set");

                if (isPasswordKey(key)) {
                    return PASSWORD_MASK;
                }

                if (containsPasswordFragment(value)) {
                    return maskPasswordFragments(value);
                }

                return value;
            }
        });
    }

    private boolean isPasswordKey(String key) {
        return key.toLowerCase().contains(PASSWORD_KEY);

    }

    private boolean containsPasswordFragment(String maybePasswordFragments) {
        return PATTERN_CONTAINING_PASSWORD_FRAGMENTS
                .matcher(maybePasswordFragments)
                .matches();
    }

    private String maskPasswordFragments(String maybePasswordFragments) {
        StringBuilder maskedBuilder = new StringBuilder();
        Matcher passwordFragmentsMatcher = PATTERN_EXTRACTING_PASSWORD_FRAGMENTS
                .matcher(maybePasswordFragments);

        int start = 0, end;
        while (passwordFragmentsMatcher.find()) {
            end = passwordFragmentsMatcher.start();

            maskedBuilder.append(maybePasswordFragments.substring(start, end));
            maskedBuilder.append(passwordFragmentsMatcher.group(1));
            maskedBuilder.append(PASSWORD_MASK);

            start = passwordFragmentsMatcher.end();
        }

        maskedBuilder.append(maybePasswordFragments.substring(start));

        return maskedBuilder.toString();
    }
}
