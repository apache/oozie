/*
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

package org.apache.oozie.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.compression.CodecFactory;
import org.apache.oozie.executor.jpa.JPAExecutor;
import org.apache.oozie.util.IOUtils;
import org.apache.openjpa.conf.OpenJPAConfiguration;
import org.apache.openjpa.persistence.OpenJPAEntityManagerFactorySPI;
import org.apache.openjpa.persistence.OpenJPAEntityManagerSPI;
import org.apache.openjpa.persistence.OpenJPAEntityTransaction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import javax.persistence.EntityManager;
import javax.persistence.Persistence;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import static org.apache.oozie.service.JPAService.CONF_PASSWORD;
import static org.apache.oozie.service.JPAService.CONF_URL;
import static org.apache.oozie.service.JPAService.INITIAL_WAIT_TIME;
import static org.apache.oozie.service.JPAService.MAX_RETRY_COUNT;
import static org.apache.oozie.service.JPAService.MAX_WAIT_TIME;
import static org.apache.oozie.service.JPAService.MAX_WAIT_TIME_DEPRECATED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;

@RunWith(Parameterized.class)
public class TestJPAService {

    private MockedStatic<Services> SERVICES;
    private MockedStatic<IOUtils> IOUTILS;
    private MockedStatic<Persistence> PERSISTENCE;
    private MockedStatic<CodecFactory> CODEFACTORY;

    private TestJPAServiceInput testJPAServiceInput;

    public TestJPAService(TestJPAServiceInput testJPAServiceInput) {
        this.testJPAServiceInput = testJPAServiceInput;
    }

    /* Defining the actual test cases below.
    *
    * Explanation of the content of the fields:
    * - Configuration: the configuration properties to be used when initializing the JPA Service (in our case they are
    * populated with the JPA service's retry mechanism related properties)
    * - JPAExecutor: the JPA executor which is run by the JPA Service. In our cases there are two types of them:
    * a passing (MyPassingJPAExecutor) and a failing (MyFailingJPAExecutor) one. The latter is responsible for
    * activating the JPA Service's retry mechanism.
    * - executedTimes: the property indicating the number of times the provided JPA Executor needs to be executed
    * according to the values of the JPA Service's retry mechanism related configuration.
    * - minElapsedTime: the value of the minimum amount of time in ms, that is elapsed between the first and last (retry)
    * execution. (explanation can be found below about how this is calculated)
    * - maxElapsedTime: the value of the maximum amount of time in ms, that is elapsed between the first and last (retry)
    *  execution. (explanation can be found below about how this is calculated)
    *
    * Explanation on how the expected elapsed time is being calculated:
    * Due to the JPA Service's retry mechanism, after an unsuccessful try, it waits for `initialWaitTime` amount of time
    * before the next try. If the second try is not successful either, than it waits for the double of `initialWaitTime`
    * before the third try. This wait time is being doubled until the `maxWaitTime` is reached. Then the value of
    * `maxWaitTime` will be used between the tries until the maximum number of tries `maxRetryCount` is reached.
    * Therefore theoretically the minimum elapsed time can be calculated easily. The maximum elapsed time however is
    * a proportional value that theoretically should not be crossed taking into consideration the value of the JPA
    * Service's retry related configuration.
    *
    * */
    public static final TestJPAServiceInput testJPAServiceInputPassing = new TestJPAServiceInput(
            createConfiguration(100, 30000, null, 10),
            new MyPassingJPAExecutor(),
            1);

    public static final TestJPAServiceInput testJPAServiceInputFailingWithDefaultRetryConfig = new TestJPAServiceInput(
            createConfiguration(100, 30000, null, 10),
            new MyFailingJPAExecutor(),
            10,
            51100,
            52000);

    public static final TestJPAServiceInput testJPAServiceInputFailinhWithOnlyCorrectRetryWaitTimeConfig =
            new TestJPAServiceInput(
                    createConfiguration(100, 200, null, 4),
                    new MyFailingJPAExecutor(),
                    4,
                    500,
                    600);

    public static final TestJPAServiceInput testJPAServiceInputFailinhWithBothRetryWaitTimeConfig = new TestJPAServiceInput(
            createConfiguration(100, 10000, 200, 4),
            new MyFailingJPAExecutor(),
            4,
            500,
            600);

    @Parameterized.Parameters
    public static Collection<Object[]> testCases() {
       List<TestJPAServiceInput> testJPAServiceInputs = Arrays.asList(
               testJPAServiceInputPassing,
               testJPAServiceInputFailingWithDefaultRetryConfig,
               testJPAServiceInputFailinhWithOnlyCorrectRetryWaitTimeConfig,
               testJPAServiceInputFailinhWithBothRetryWaitTimeConfig
       );

        Collection<Object[]> result = new ArrayList<>();
        for (TestJPAServiceInput testJPAServiceInput : testJPAServiceInputs) {
            result.add(new Object[] { testJPAServiceInput });
        }

        return result;
    }

    public static class MyJPAExecutor implements JPAExecutor<String> {
        List<Date> dates = new ArrayList<>();

        @Override
        public String getName() {
            return "foobar";
        }

        @Override
        public String execute(EntityManager em) {
            return null;
        }
    }

    public static class MyPassingJPAExecutor extends MyJPAExecutor {
        @Override
        public String execute(EntityManager em) {
            assertNotNull(em);
            return "ret";
        }
    }

    public static class MyFailingJPAExecutor extends MyJPAExecutor {
        @Override
        public String execute(EntityManager em) {
            // saving the dates of the executions
            Date date = new Date();
            dates.add(date);

            // intentionally throwing a not blacklisted exception to activate the retry mechanism
            throw new UnsupportedOperationException("Intentionally thrown exception to activate the JPA Service's " +
                    "retry mechanism...");
        }
    }

    private static class TestJPAServiceInput {

        private Configuration conf;
        private JPAExecutor<String> jpaExecutor;
        private int executedTimes;
        private Long minElapsedTime = null;
        private Long maxElapsedTime = null;

        public TestJPAServiceInput(Configuration conf, JPAExecutor<String> jpaExecutor, int executedTimes) {
            this.conf = conf;
            this.jpaExecutor = jpaExecutor;
            this.executedTimes = executedTimes;
        }

        public TestJPAServiceInput(Configuration conf, JPAExecutor<String> jpaExecutor, int executedTimes, long minElapsedTime,
                                   long maxElapsedTime) {
            this.conf = conf;
            this.jpaExecutor = jpaExecutor;
            this.executedTimes = executedTimes;
            this.minElapsedTime = minElapsedTime;
            this.maxElapsedTime = maxElapsedTime;
        }
    }

    /*
    * Tests the mechanism of the JPA service.
    */
    @Test
    public void testJPAService() {
        JPAService mockedJPAService = null;
        try {
            // adding the additional required configuration properties
            setAdditionalRequiredConfProps(testJPAServiceInput.conf);
            mockedJPAService = createMockedJPAService(testJPAServiceInput.conf);
            MyJPAExecutor myJPAExecutor = (MyJPAExecutor) Mockito.spy(testJPAServiceInput.jpaExecutor);

            String ret = null;
            try {
                 ret = mockedJPAService.execute(myJPAExecutor);
            } catch (Exception e) {
                // no-op
            }

            Mockito.verify(myJPAExecutor, Mockito.times(testJPAServiceInput.executedTimes)).execute(Mockito.any());

            if (testJPAServiceInput.jpaExecutor instanceof MyPassingJPAExecutor) {
                assertEquals("The return value should have been `ret`", "ret", ret);
                // if the JPA executor is passing then there is no point to check the retry mechanism, we're done here
                return;
            }

            // saving the elapsed time between the first and last try
            long elapsedTimeBetweenFirstAndLastExecution =
                    myJPAExecutor.dates.get(testJPAServiceInput.executedTimes - 1).getTime()
                            - myJPAExecutor.dates.get(0).getTime();

            assertTrue("The elapsed time between the first and last execution should took minimum ["
                            + testJPAServiceInput.minElapsedTime + "ms] but it took instead ["
                            + elapsedTimeBetweenFirstAndLastExecution + "ms].",
                    elapsedTimeBetweenFirstAndLastExecution > testJPAServiceInput.minElapsedTime);

            assertTrue("The elapsed time between the first and last execution should took maximum ["
                            + testJPAServiceInput.maxElapsedTime + "ms] but it took instead ["
                            + elapsedTimeBetweenFirstAndLastExecution + "ms].",
                    elapsedTimeBetweenFirstAndLastExecution < testJPAServiceInput.maxElapsedTime);

        } catch (Exception e) {
           throw new RuntimeException(e);
        } finally {
            if (mockedJPAService != null) {
                mockedJPAService.destroy();
            }
            closeStaticMocks();
        }
    }

    private static Configuration createConfiguration(Integer initialWaitTime, Integer maxWaitTime, Integer maxWaitTimeDeprecated,
                                                     Integer maxRetryCount) {
        Configuration conf = new Configuration();
        setIntToConfIfNotNull(conf, INITIAL_WAIT_TIME, initialWaitTime);
        setIntToConfIfNotNull(conf, MAX_WAIT_TIME, maxWaitTime);
        setIntToConfIfNotNull(conf, MAX_WAIT_TIME_DEPRECATED, maxWaitTimeDeprecated);
        setIntToConfIfNotNull(conf, MAX_RETRY_COUNT, maxRetryCount);
        return conf;
    }

    private static void setIntToConfIfNotNull(Configuration conf, String key, Integer value) {
        if (value != null) {
            conf.setInt(key, value);
        }
    }

    private void setAdditionalRequiredConfProps(Configuration conf) {
        conf.set(CONF_PASSWORD, "foobar");
        conf.set(CONF_URL, "jdbc:foo:bar");
    }

    private JPAService createMockedJPAService(Configuration conf) throws ServiceException {
        Services mockServices = Mockito.mock(Services.class);
        doReturn(conf).when(mockServices).getConf();

        IOUTILS = Mockito.mockStatic(IOUtils.class);
        InputStream mockInputStream = Mockito.mock(InputStream.class);
        IOUTILS.when(() -> IOUtils.getResourceAsStream(Mockito.anyString(), Mockito.anyInt())).thenReturn(mockInputStream);

        SERVICES = Mockito.mockStatic(Services.class);
        SERVICES.when(Services::get).thenReturn(mockServices);

        PERSISTENCE = Mockito.mockStatic(Persistence.class);
        OpenJPAEntityTransaction mockEntityTransaction = Mockito.mock(OpenJPAEntityTransaction.class);
        OpenJPAEntityManagerSPI mockEntityManager = Mockito.mock(OpenJPAEntityManagerSPI.class);
        OpenJPAEntityManagerFactorySPI mockEntityManagerFactory = Mockito.mock(OpenJPAEntityManagerFactorySPI.class);
        OpenJPAConfiguration mockOpenJPAConfiguration = Mockito.mock(OpenJPAConfiguration.class);
        Mockito.when(mockEntityTransaction.isActive()).thenReturn(false);
        Mockito.when(mockEntityManager.getTransaction()).thenReturn(mockEntityTransaction);
        Mockito.when(mockEntityManagerFactory.createEntityManager()).thenReturn(mockEntityManager);
        Mockito.when(mockOpenJPAConfiguration.getConnectionProperties()).thenReturn("foobar");
        Mockito.when(mockEntityManagerFactory.getConfiguration()).thenReturn(mockOpenJPAConfiguration);
        PERSISTENCE.when(() -> Persistence.createEntityManagerFactory(Mockito.anyString(),
                Mockito.any())).thenReturn(mockEntityManagerFactory);

        CODEFACTORY = Mockito.mockStatic(CodecFactory.class);

        JPAService jpaService = new JPAService();
        jpaService.init(mockServices);
        return jpaService;
    }

    private void closeStaticMocks() {
        if (SERVICES != null) {
            try {
                SERVICES.close();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                SERVICES = null;
            }
        }

        if (IOUTILS != null) {
            try {
                IOUTILS.close();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                IOUTILS = null;
            }
        }

        if (PERSISTENCE != null) {
            try {
                PERSISTENCE.close();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                PERSISTENCE = null;
            }
        }

        if (CODEFACTORY != null) {
            try {
                CODEFACTORY.close();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                CODEFACTORY = null;
            }
        }
    }
}
