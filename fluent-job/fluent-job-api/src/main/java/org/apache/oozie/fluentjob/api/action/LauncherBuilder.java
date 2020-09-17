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
 * A builder class for {@link Launcher}.
 *
 * The properties of the builder can only be set once, an attempt to set them a second time will trigger
 * an {@link IllegalStateException}.
 *
 * Builder instances can be used to build several elements, although properties already set cannot be changed after
 * a call to {@link LauncherBuilder#build} either.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class LauncherBuilder implements Builder<Launcher> {
    private final ModifyOnce<Long> memoryMb;
    private final ModifyOnce<Long> vCores;
    private final ModifyOnce<String> queue;
    private final ModifyOnce<String> sharelib;
    private final ModifyOnce<String> viewAcl;
    private final ModifyOnce<String> modifyAcl;

    public LauncherBuilder() {
        this.memoryMb = new ModifyOnce<>();
        this.vCores = new ModifyOnce<>();
        this.queue = new ModifyOnce<>();
        this.sharelib = new ModifyOnce<>();
        this.viewAcl = new ModifyOnce<>();
        this.modifyAcl = new ModifyOnce<>();
    }

    @Override
    public Launcher build() {
        return new Launcher(memoryMb.get(),
                vCores.get(),
                queue.get(),
                sharelib.get(),
                viewAcl.get(),
                modifyAcl.get());
    }

    public LauncherBuilder withMemoryMb(final long memoryMb) {
        this.memoryMb.set(memoryMb);
        return this;
    }

    public LauncherBuilder withVCores(final long vCores) {
        this.vCores.set(vCores);
        return this;
    }

    public LauncherBuilder withQueue(final String queue) {
        this.queue.set(queue);
        return this;
    }

    public LauncherBuilder withSharelib(final String sharelib) {
        this.sharelib.set(sharelib);
        return this;
    }

    public LauncherBuilder withViewAcl(final String viewAcl) {
        this.viewAcl.set(viewAcl);
        return this;
    }

    public LauncherBuilder withModifyAcl(final String modifyAcl) {
        this.modifyAcl.set(modifyAcl);
        return this;
    }
}
