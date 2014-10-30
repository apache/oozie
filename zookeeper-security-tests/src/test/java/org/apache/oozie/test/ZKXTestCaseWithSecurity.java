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

package org.apache.oozie.test;

import java.io.File;
import javax.security.auth.login.Configuration;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.JaasConfiguration;
import org.apache.zookeeper.server.ZooKeeperSaslServer;

/**
 * Provides a version of {@link ZKXTestCase} with security.  A MiniKdc will be started (so no special outside setup is needed) and
 * the embedded ZooKeeper provided by this class will support connecting to it with SASL/Kerberos authentication.  However,
 * currently, the client returned by {@link #getClient()) and the client used by DummyZKOozie do not authenticate, so they won't
 * have full access to any znodes with "sasl" ACLs (this is not always true, see {@link #setupZKServer()).
 * <p>
 * Anything using {@link ZKUtils} can connect using authentication by simply setting "oozie.zookeeper.secure" to "true" before
 * creating the first thing that uses ZKUtils.  Make sure to set it back to false when done.
 */
public abstract class ZKXTestCaseWithSecurity extends ZKXTestCase {
    private MiniKdc kdc = null;
    private File keytabFile;
    private String originalKeytabLoc;
    private String originalPrincipal;

    /**
     * The primary part of the principal name for the Kerberos user
     */
    protected static final String PRIMARY_PRINCIPAL = "oozie";

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        // Set the keytab location and principal to the miniKDC values
        originalKeytabLoc = Services.get().getConf().get(HadoopAccessorService.KERBEROS_KEYTAB);
        originalPrincipal = Services.get().getConf().get(HadoopAccessorService.KERBEROS_PRINCIPAL);
        Services.get().getConf().set(HadoopAccessorService.KERBEROS_KEYTAB, keytabFile.getAbsolutePath());
        Services.get().getConf().set(HadoopAccessorService.KERBEROS_PRINCIPAL, getPrincipal());
    }

    @Override
    protected void tearDown() throws Exception {
        // Restore these values
        Services.get().getConf().set(HadoopAccessorService.KERBEROS_KEYTAB, originalKeytabLoc);
        Services.get().getConf().set(HadoopAccessorService.KERBEROS_PRINCIPAL, originalPrincipal);
        // Just in case the test forgets to set this back
        Services.get().getConf().set("oozie.zookeeper.secure", "false");
        super.tearDown();
        if (kdc != null) {
            kdc.stop();
        }
    }

    /**
     * Creates and sets up the embedded ZooKeeper server.  Test subclasses should have no reason to override this method.
     * <p>
     * Here we override it to start the MiniKdc, set the jaas configuration, configure ZooKeeper for SASL/Kerberos authentication
     * and ACLs, and to start the ZooKeeper server.
     * <p>
     * Unfortunately, ZooKeeper security requires setting the security for the entire JVM.  And for the tests, we're running the
     * ZK server and one or more clients from the same JVM, so things get messy.  There are two ways to tell ZooKeeper to
     * authenticate: (1) set the system property, "java.security.auth.login.config", to a jaas.conf file and (2) create a
     * javax.security.auth.login.Configuration object with the same info as the jaas.conf and set it.  In either case, once set and
     * something has authenticated, it seems that it can't be unset or changed, and there's no way to log out.  By setting the
     * system property, "javax.security.auth.useSubjectCredsOnly", to "false" we can sort-of change the jaas Configuration, but its
     * kind of funny about it.  Another effect of this is that we have to add jaas entries for the "Server" and "Client" here
     * instead of just the "Server" here and the "Client" in the normal place ({@link ZKUtils}) or it will be unable to find the
     * "Client" info.  Also, because there is no way to logout, once any client has authenticated once, all subsequent clients will
     * automatically connect using the same authentication; trying to stop this is futile and either results in an error or has no
     * effect.  This means that there's no way to do any tests with an unauthenticated client.  Also, if any tests using secure
     * ZooKeeper get run before tests not using secure ZooKeeper, they will likely fail because it will try to use authentication:
     * so they should be run separately.  For this reason, the secure tests should be run in a separate module where they will get
     * their own JVM.
     *
     * @return the embedded ZooKeeper server
     * @throws Exception
     */
    @Override
    protected TestingServer setupZKServer() throws Exception {
        // Not entirely sure exactly what "javax.security.auth.useSubjectCredsOnly=false" does, but it has something to do with
        // re-authenticating in cases where it otherwise wouldn't.  One of the sections on this page briefly mentions it:
        // http://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/Troubleshooting.html
        setSystemProperty("javax.security.auth.useSubjectCredsOnly", "false");

        // Setup KDC and principal
        kdc = new MiniKdc(MiniKdc.createConf(), new File(getTestCaseDir()));
        kdc.start();
        keytabFile = new File(getTestCaseDir(), "test.keytab");
        String serverPrincipal = "zookeeper/127.0.0.1";
        kdc.createPrincipal(keytabFile, getPrincipal(), serverPrincipal);

        setSystemProperty("zookeeper.authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
        setSystemProperty("zookeeper.kerberos.removeHostFromPrincipal", "true");
        setSystemProperty("zookeeper.kerberos.removeRealmFromPrincipal", "true");

        JaasConfiguration.addEntry("Server", serverPrincipal, keytabFile.getAbsolutePath());
        // Here's where we add the "Client" to the jaas configuration, even though we'd like not to
        JaasConfiguration.addEntry("Client", getPrincipal(), keytabFile.getAbsolutePath());
        Configuration.setConfiguration(JaasConfiguration.getInstance());

        setSystemProperty(ZooKeeperSaslServer.LOGIN_CONTEXT_NAME_KEY, "Server");

        return new TestingServer();
    }

    /**
     * Returns the principal of the Kerberos user.  This would be {@link #PRIMARY_PRINCIPAL}/_host_
     *
     * @return the principal of the Kerberos user
     */
    protected String getPrincipal() {
        return PRIMARY_PRINCIPAL + "/" + kdc.getHost();
    }
}

