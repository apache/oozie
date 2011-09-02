/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.util;

import java.util.List;

/**
 * Utility class to check common parameter preconditions.
 */
public class ParamChecker {

    /**
     * Check that a value is not null. If null throws an IllegalArgumentException.
     *
     * @param obj value.
     * @param name parameter name for the exception message.
     * @return the given value.
     */
    public static <T> T notNull(T obj, String name) {
        if (obj == null) {
            throw new IllegalArgumentException(name + " cannot be null");
        }
        return obj;
    }

    /**
     * Check that a list is not null and that none of its elements is null. If null or if the list has emtpy elements
     * throws an IllegalArgumentException.
     *
     * @param list the list of strings.
     * @param name parameter name for the exception message.
     * @return the given list.
     */
    public static <T> List<T> notNullElements(List<T> list, String name) {
        notNull(list, name);
        for (int i = 0; i < list.size(); i++ ) {
            notNull(list.get(i), XLog.format("list [{0}] element [{1}]", name, i));
        }
        return list;
    }

    /**
     * Check that a string is not null and not empty. If null or emtpy throws an IllegalArgumentException.
     *
     * @param str value.
     * @param name parameter name for the exception message.
     * @return the given value.
     */
    public static String notEmpty(String str, String name) {
        if (str == null) {
            throw new IllegalArgumentException(name + " cannot be null");
        }
        if (str.length() == 0) {
            throw new IllegalArgumentException(name + " cannot be empty");
        }
        return str;
    }

    /**
     * Check that a list is not null and that none of its elements is null. If null or if the list has emtpy elements
     * throws an IllegalArgumentException.
     *
     * @param list the list of strings.
     * @param name parameter name for the exception message.
     * @return the given list.
     */
    public static List<String> notEmptyElements(List<String> list, String name) {
        notNull(list, name);
        for (int i = 0; i < list.size(); i++ ) {
            notEmpty(list.get(i), XLog.format("list [{0}] element [{1}]", name, i));
        }
        return list;
    }

    private static final int MAX_NODE_NAME_LEN = 50;

    /**
     * Check that the given string is a valid action name [a-zA-Z_][0-9a-zA-Z_\-]* and not longer than 50 chars.
     *
     * @param actionName string to validate is a token.
     * @return the given string.
     */
    public static String validateActionName(String actionName) {
        ParamChecker.notEmpty(actionName, "action name");
        if (actionName.length() > MAX_NODE_NAME_LEN) {
            throw new IllegalArgumentException(XLog.format("name [{0}] must be {1} chars or less",
                                                           actionName, MAX_NODE_NAME_LEN));
        }

        char c = actionName.charAt(0);
        if (!(c>='A' && c<='Z') && !(c>='a' && c<='z') && !(c=='_')) {
            throw new IllegalArgumentException(XLog.format("name [{0}], must start with [A-Za-z_]", actionName));
        }
        for (int i = 1; i < actionName.length(); i++) {
            c = actionName.charAt(i);
            if (!(c>='0' && c<='9') && !(c>='A' && c<='Z') && !(c>='a' && c<='z') && !(c=='_' || c=='-')) {
                throw new IllegalArgumentException(XLog.format("name [{0}] must be [A-Za-z_][0-9A-Za-z_]*",
                                                               actionName));
            }
        }
        return actionName;
    }

    /**
     * Return if the specified token is a valid Java identifier.
     *
     * @param token string to validate if it is a valid Java identifier.
     * @return if the specified token is a valid Java identifier.
     */
    public static boolean isValidIdentifier(String token) {
        ParamChecker.notEmpty(token, "identifier");
        for (int i = 0; i < token.length(); i++) {
            char c = token.charAt(i);
            if (!(c>='0' && c<='9') && !(c>='A' && c<='Z') && !(c>='a' && c<='z') && !(c=='_')) {
                return false;
            }
            if (i == 0 && (c>='0' && c<='9')) {
                return false;
            }
        }
        return true;
    }

}