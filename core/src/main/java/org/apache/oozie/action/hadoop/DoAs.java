/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.action.hadoop;

import java.util.concurrent.Callable;

//TODO this class goes away when doing 20.100+ only

//TODO this class is for testing, but is here to allow selective compilation
public class DoAs implements Callable<Void> {
    private String user;
    private Callable<Void> callable;

    public void setUser(String user) {
        this.user = user;
    }

    protected String getUser() {
        return user;
    }

    protected Callable<Void> getCallable() {
        return callable;
    }

    public void setCallable(Callable<Void> callable) {
        this.callable = callable;
    }

    public Void call() throws Exception {
        callable.call();
        return null;
    }

    public static void call(String user, Callable<Void> callable) throws Exception {
        Class klass;
        try {
            klass = Class.forName("org.apache.oozie.action.hadoop.KerberosDoAs");
        }
        catch (ClassNotFoundException ex) {
            klass = DoAs.class;
        }
        DoAs doAs = (DoAs) klass.newInstance();
        doAs.setCallable(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                PigMain.main(null);
                return null;
            }
        });
        doAs.setUser(user);
        doAs.setCallable(callable);
        doAs.call();
    }

}
