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

package org.apache.oozie.coord.input.dependency;

import java.io.UnsupportedEncodingException;

import org.apache.commons.lang.StringUtils;
import org.apache.oozie.StringBlob;
import org.apache.oozie.util.WritableUtils;
import org.apache.oozie.util.XLog;

public class CoordInputDependencyFactory {

    // We need to choose magic number which is not allowed for file/dir.
    // Magic number is ::$
    private static final byte[] MAGIC_NUMBER = new byte[] { 58, 58, 36 };
    public static final String CHAR_ENCODING = "ISO-8859-1";

    public static XLog LOG = XLog.getLog(CoordInputDependencyFactory.class);

    /**
     * Create the pull dependencies.
     *
     * @param isInputLogicSpecified to check if input logic is enable
     * @return the pull dependencies
     */
    public static CoordInputDependency createPullInputDependencies(boolean isInputLogicSpecified) {
        if (!isInputLogicSpecified) {
            return new CoordOldInputDependency();
        }
        else {
            return new CoordPullInputDependency();
        }
    }

    /**
     * Create the push dependencies.
     *
     * @param isInputLogicSpecified to check if input logic is enable
     * @return the push dependencies
     */
    public static CoordInputDependency createPushInputDependencies(boolean isInputLogicSpecified) {
        if (!isInputLogicSpecified) {
            return new CoordOldInputDependency();
        }
        else {
            return new CoordPushInputDependency();
        }
    }

    /**
     * Gets the pull input dependencies.
     *
     * @param missingDependencies the missing dependencies
     * @return the pull input dependencies
     */
    public static CoordInputDependency getPullInputDependencies(StringBlob missingDependencies) {
        if (missingDependencies == null) {
            return new CoordOldInputDependency();
        }
        return getPullInputDependencies(missingDependencies.getString());
    }

    public static CoordInputDependency getPullInputDependencies(String dependencies) {

        if (StringUtils.isEmpty(dependencies) || !hasInputLogic(dependencies)) {
            return new CoordOldInputDependency(dependencies);
        }
        else
            try {
                return WritableUtils.fromByteArray(getDependenciesWithoutMagicNumber(dependencies).getBytes(CHAR_ENCODING),
                        CoordPullInputDependency.class);
            }
            catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
    }

    /**
     * Gets the push input dependencies.
     *
     * @param pushMissingDependencies the push missing dependencies
     * @return the push input dependencies
     */
    public static CoordInputDependency getPushInputDependencies(StringBlob pushMissingDependencies) {

        if (pushMissingDependencies == null) {
            return new CoordOldInputDependency();
        }
        return getPushInputDependencies(pushMissingDependencies.getString());

    }

    public static CoordInputDependency getPushInputDependencies(String dependencies) {

        if (StringUtils.isEmpty(dependencies) || !hasInputLogic(dependencies)) {
            return new CoordOldInputDependency(dependencies);
        }

        else {
            try {
                return WritableUtils.fromByteArray(getDependenciesWithoutMagicNumber(dependencies).getBytes(CHAR_ENCODING),
                        CoordPushInputDependency.class);
            }
            catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }

        }
    }

    /**
     * Checks if input logic is enable.
     *
     * @param dependencies the dependencies
     * @return true, if is input logic enable
     */
    private static boolean hasInputLogic(String dependencies) {
        return dependencies.startsWith(getMagicNumber());
    }

    /**
     * Gets the magic number.
     *
     * @return the magic number
     */
    public static String getMagicNumber() {
        try {
            return new String(MAGIC_NUMBER, CHAR_ENCODING);
        }
        catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Gets the dependencies without magic number.
     *
     * @param dependencies the dependencies
     * @return the dependencies without magic number
     */
    public static String getDependenciesWithoutMagicNumber(String dependencies) {
        return dependencies.substring(getMagicNumber().length());
    }

}
