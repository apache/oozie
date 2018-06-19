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

import org.apache.oozie.fluentjob.api.generated.action.ssh.FLAG;
import org.apache.oozie.fluentjob.api.generated.action.ssh.ObjectFactory;
import org.dozer.DozerConverter;

/**
 * A {@link DozerConverter} converting from {@link Boolean} to JAXB {@link FLAG}.
 */
public class BooleanToSshFLAGConverter extends DozerConverter<Boolean, FLAG> {
    public BooleanToSshFLAGConverter() {
        super(Boolean.class, FLAG.class);
    }

    @Override
    public FLAG convertTo(final Boolean source, final FLAG destination) {
        if (source == null) {
            return null;
        }

        if (source) {
            return new ObjectFactory().createFLAG();
        }

        return null;
    }

    @Override
    public Boolean convertFrom(final FLAG source, final Boolean destination) {
        throw new UnsupportedOperationException("This mapping is not bidirectional.");
    }
}
