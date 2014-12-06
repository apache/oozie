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

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Timer;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.oozie.test.XTestCase;

// Most tests adpated from TestInstrumentation
public class TestMetricsInstrumentation extends XTestCase {
    private static final long INTERVAL = 300;

    // Filter that removes the "jvm.memory" gauges
    private final MetricFilter noJvm = new MetricFilter() {
        @Override
        public boolean matches(String name, Metric metric) {
            return !name.startsWith("jvm.memory");
        }
    };

    public void testInstrumentationCounter() throws Exception {
        MetricsInstrumentation inst = new MetricsInstrumentation();
        assertEquals(0, inst.getMetricRegistry().getCounters().size());
        inst.incr("a", "1", 1);
        assertEquals(1, inst.getMetricRegistry().getCounters().size());
        inst.incr("a", "2", 2);
        assertEquals(2, inst.getMetricRegistry().getCounters().size());
        inst.incr("b", "1", 3);
        assertEquals(3, inst.getMetricRegistry().getCounters().size());
        assertEquals(1L, inst.getMetricRegistry().getCounters().get("a.1").getCount());
        assertEquals(2L, inst.getMetricRegistry().getCounters().get("a.2").getCount());
        assertEquals(3L, inst.getMetricRegistry().getCounters().get("b.1").getCount());
    }

    private long getTimerValue(Timer timer) {
        long[] values = timer.getSnapshot().getValues();
        // These get stored in nanoseconds but Cron is in milliseconds
        return TimeUnit.NANOSECONDS.toMillis(values[0]);
    }

    public void testInstrumentationTimer() throws Exception {
        MetricsInstrumentation inst = new MetricsInstrumentation();
        assertEquals(0, inst.getMetricRegistry().getTimers().size());
        Instrumentation.Cron cron1 = new Instrumentation.Cron();
        inst.addCron("a", "1", cron1);
        assertEquals(1, inst.getMetricRegistry().getTimers().size());
        Instrumentation.Cron cron2 = new Instrumentation.Cron();
        cron2.start();
        Thread.sleep(INTERVAL);
        cron2.stop();
        inst.addCron("a", "2", cron2);
        assertEquals(2, inst.getMetricRegistry().getTimers().size());
        Instrumentation.Cron cron3 = new Instrumentation.Cron();
        cron3.start();
        Thread.sleep(INTERVAL * 2);
        cron3.stop();
        inst.addCron("b", "1", cron3);
        assertEquals(3, inst.getMetricRegistry().getTimers().size());

        assertEquals(cron1.getOwn(), getTimerValue(inst.getMetricRegistry().getTimers().get("a.1.timer")));
        assertEquals(cron2.getOwn(), getTimerValue(inst.getMetricRegistry().getTimers().get("a.2.timer")));
        assertEquals(cron3.getOwn(), getTimerValue(inst.getMetricRegistry().getTimers().get("b.1.timer")));
    }

    public void testVariables() throws Exception {
        MetricsInstrumentation inst = new MetricsInstrumentation();

        inst.addVariable("a", "1", new Instrumentation.Variable<Long>() {
            private long counter = 0;

            public Long getValue() {
                return counter++;
            }
        });
        assertEquals(1, inst.getMetricRegistry().getGauges(noJvm).size());

        inst.addVariable("a", "2", new Instrumentation.Variable<Long>() {
            private long counter = 1;

            public Long getValue() {
                return counter++;
            }
        });
        assertEquals(2, inst.getMetricRegistry().getGauges(noJvm).size());
        inst.addVariable("b", "1", new Instrumentation.Variable<Long>() {
            private long counter = 2;

            public Long getValue() {
                return counter++;
            }
        });
        assertEquals(3, inst.getMetricRegistry().getGauges(noJvm).size());

        assertEquals(0L, inst.getMetricRegistry().getGauges(noJvm).get("a.1").getValue());
        assertEquals(1L, inst.getMetricRegistry().getGauges(noJvm).get("a.2").getValue());
        assertEquals(2L, inst.getMetricRegistry().getGauges(noJvm).get("b.1").getValue());
        assertEquals(1L, inst.getMetricRegistry().getGauges(noJvm).get("a.1").getValue());
        assertEquals(2L, inst.getMetricRegistry().getGauges(noJvm).get("a.2").getValue());
        assertEquals(3L, inst.getMetricRegistry().getGauges(noJvm).get("b.1").getValue());
    }

    public void testSamplers() throws Exception {
        MetricsInstrumentation inst = new MetricsInstrumentation();
        ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
        try {
            inst.setScheduler(scheduledExecutorService);

            inst.addSampler("a", "1", 10, 1, new Instrumentation.Variable<Long>() {
                public Long getValue() {
                    return 1L;
                }
            });
            assertEquals(1, inst.getMetricRegistry().getHistograms().size());

            inst.addSampler("a", "2", 10, 1, new Instrumentation.Variable<Long>() {
                public Long getValue() {
                    return 2L;
                }
            });
            assertEquals(2, inst.getMetricRegistry().getHistograms().size());

            inst.addSampler("b", "1", 10, 1, new Instrumentation.Variable<Long>() {
                private long counter = 0;

                public Long getValue() {
                    return counter++ % 10;
                }
            });
            assertEquals(3, inst.getMetricRegistry().getHistograms().size());

            waitFor(20 * 1000, new Predicate() {
                public boolean evaluate() throws Exception {
                    return false;
                }
            });

            assertEquals(1D, inst.getMetricRegistry().getHistograms().get("a.1.histogram").getSnapshot().getMean(), 0.01D);
            assertEquals(2D, inst.getMetricRegistry().getHistograms().get("a.2.histogram").getSnapshot().getMean(), 0.02D);
            assertEquals(4D, inst.getMetricRegistry().getHistograms().get("b.1.histogram").getSnapshot().getMean(), 0.5D);
        } finally {
            scheduledExecutorService.shutdownNow();
        }
    }

    public void testUnsupportedOpertation() {
        MetricsInstrumentation instr = new MetricsInstrumentation();
        try {
            instr.getAll();
            fail();
        } catch (UnsupportedOperationException uoe) {
        }
        try {
            instr.getCounters();
            fail();
        } catch (UnsupportedOperationException uoe) {
        }
        try {
            instr.getSamplers();
            fail();
        } catch (UnsupportedOperationException uoe) {
        }
        try {
            instr.getTimers();
            fail();
        } catch (UnsupportedOperationException uoe) {
        }
        try {
            instr.getVariables();
            fail();
        } catch (UnsupportedOperationException uoe) {
        }
    }
}
