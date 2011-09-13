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

import org.apache.oozie.test.XTestCase;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ScheduledExecutorService;

public class TestInstrumentation extends XTestCase {
    private static final long INTERVAL = 300;
    private static final long TOLERANCE = 30;

    public void testCron() throws Exception {
        Instrumentation.Cron cron = new Instrumentation.Cron();
        long start = System.currentTimeMillis();
        assertEquals("", 0, cron.getStart(), TOLERANCE);
        assertEquals("", 0, cron.getEnd(), TOLERANCE);
        assertEquals(cron.getStart(), cron.getEnd());
        assertEquals(0, cron.getOwn());
        assertEquals(0, cron.getTotal());

        cron.start();
        long s = System.currentTimeMillis();
        Thread.sleep(INTERVAL);
        cron.stop();
        long realOwnDelay = System.currentTimeMillis() - s;
        long now = System.currentTimeMillis();
        assertEquals("", start, cron.getStart(), TOLERANCE);
        assertEquals("", now, cron.getEnd(), TOLERANCE);
        assertEquals("", INTERVAL, cron.getTotal(), TOLERANCE);
        assertEquals("", INTERVAL, cron.getOwn(), TOLERANCE);
        assertEquals("", cron.getTotal(), cron.getOwn(), TOLERANCE);

        long realTotalDelay = System.currentTimeMillis() - s;
        s = System.currentTimeMillis();
        Thread.sleep(INTERVAL);

        cron.start();

        realTotalDelay += System.currentTimeMillis() - s;

        s = System.currentTimeMillis();
        Thread.sleep(INTERVAL);
        cron.stop();
        now = System.currentTimeMillis();

        realTotalDelay += System.currentTimeMillis() - s;
        realOwnDelay += System.currentTimeMillis() - s;

        assertEquals("", start, cron.getStart(), TOLERANCE);
        assertEquals("", now, cron.getEnd(), TOLERANCE);
        assertEquals("", realTotalDelay, cron.getTotal(), TOLERANCE);
        assertEquals("", realOwnDelay, cron.getOwn(), TOLERANCE);
    }

    public void testTimer() throws Exception {
        Instrumentation.Timer timer = new Instrumentation.Timer();

        assertEquals(0, timer.getTicks());
        assertEquals(0, timer.getTotal());
        assertEquals(0, timer.getOwn());
        assertEquals(0, timer.getOwnAvg());
        assertEquals(0, timer.getTotalAvg());
        assertEquals(0, timer.getOwnSquareSum());
        assertEquals(0, timer.getTotalSquareSum());
        assertEquals(0, timer.getOwnMin());
        assertEquals(0, timer.getOwnMax());
        assertEquals(0, timer.getTotalMin());
        assertEquals(0, timer.getTotalMax());

        assertEquals(0, timer.getValue().getTicks());
        assertEquals(0, timer.getValue().getTotal());
        assertEquals(0, timer.getValue().getOwn());
        assertEquals(0, timer.getValue().getOwnAvg());
        assertEquals(0, timer.getValue().getTotalAvg());
        assertEquals(0, timer.getValue().getOwnSquareSum());
        assertEquals(0, timer.getValue().getTotalSquareSum());
        assertEquals(0, timer.getValue().getOwnMin());
        assertEquals(0, timer.getValue().getOwnMax());
        assertEquals(0, timer.getValue().getTotalMin());
        assertEquals(0, timer.getValue().getTotalMax());

        Instrumentation.Cron cron1 = new Instrumentation.Cron();
        cron1.start();
        Thread.sleep(INTERVAL);
        cron1.stop();
        timer.addCron(cron1);

        assertEquals(1, timer.getTicks());
        assertEquals(cron1.getTotal(), timer.getTotal());
        assertEquals(cron1.getOwn(), timer.getOwn());
        assertEquals(cron1.getOwn(), timer.getOwnAvg());
        assertEquals(cron1.getTotal(), timer.getTotalAvg());
        assertEquals(cron1.getOwn() * cron1.getOwn(), timer.getOwnSquareSum());
        assertEquals(cron1.getTotal() * cron1.getTotal(), timer.getTotalSquareSum());
        assertEquals(cron1.getOwn(), timer.getOwnMin());
        assertEquals(cron1.getOwn(), timer.getOwnMax());
        assertEquals(cron1.getTotal(), timer.getTotalMin());
        assertEquals(cron1.getTotal(), timer.getTotalMax());

        assertEquals(1, timer.getValue().getTicks());
        assertEquals(cron1.getTotal(), timer.getValue().getTotal());
        assertEquals(cron1.getOwn(), timer.getValue().getOwn());
        assertEquals(cron1.getOwn(), timer.getValue().getOwnAvg());
        assertEquals(cron1.getTotal(), timer.getValue().getTotalAvg());
        assertEquals(cron1.getOwn() * cron1.getOwn(), timer.getValue().getOwnSquareSum());
        assertEquals(cron1.getTotal() * cron1.getTotal(), timer.getValue().getTotalSquareSum());
        assertEquals(cron1.getOwn(), timer.getValue().getOwnMin());
        assertEquals(cron1.getOwn(), timer.getValue().getOwnMax());
        assertEquals(cron1.getTotal(), timer.getValue().getTotalMin());
        assertEquals(cron1.getTotal(), timer.getValue().getTotalMax());

        Instrumentation.Cron cron2 = new Instrumentation.Cron();
        cron2.start();
        Thread.sleep(INTERVAL * 2);
        cron2.stop();
        timer.addCron(cron2);

        assertEquals(2, timer.getTicks());
        assertEquals(cron1.getTotal() + cron2.getTotal(), timer.getTotal());
        assertEquals(cron1.getOwn() + cron2.getOwn(), timer.getOwn());
        assertEquals((cron1.getOwn() + cron2.getOwn()) / 2, timer.getOwnAvg());
        assertEquals((cron1.getTotal() + cron2.getTotal()) / 2, timer.getTotalAvg());
        assertEquals(cron1.getOwn() * cron1.getOwn() + cron2.getOwn() * cron2.getOwn(),
                     timer.getOwnSquareSum());
        assertEquals(cron1.getTotal() * cron1.getTotal() + cron2.getTotal() * cron2.getTotal(),
                     timer.getTotalSquareSum());
        assertEquals(cron1.getOwn(), timer.getOwnMin());
        assertEquals(cron2.getOwn(), timer.getOwnMax());
        assertEquals(cron1.getTotal(), timer.getTotalMin());
        assertEquals(cron2.getTotal(), timer.getTotalMax());
    }

    public void testInstrumentationCounter() throws Exception {
        Instrumentation inst = new Instrumentation();
        assertEquals(0, inst.getCounters().size());
        inst.incr("a", "1", 1);
        assertEquals(1, inst.getCounters().size());
        assertEquals(1, inst.getCounters().get("a").size());
        inst.incr("a", "2", 2);
        assertEquals(1, inst.getCounters().size());
        assertEquals(2, inst.getCounters().get("a").size());
        inst.incr("b", "1", 3);
        assertEquals(2, inst.getCounters().size());
        assertEquals(2, inst.getCounters().get("a").size());
        assertEquals(1, inst.getCounters().get("b").size());
        assertEquals(new Long(1), inst.getCounters().get("a").get("1").getValue());
        assertEquals(new Long(2), inst.getCounters().get("a").get("2").getValue());
        assertEquals(new Long(3), inst.getCounters().get("b").get("1").getValue());
    }

    public void testInstrumentationTimer() throws Exception {
        Instrumentation inst = new Instrumentation();
        assertEquals(0, inst.getTimers().size());
        Instrumentation.Cron cron1 = new Instrumentation.Cron();
        inst.addCron("a", "1", cron1);
        assertEquals(1, inst.getTimers().size());
        assertEquals(1, inst.getTimers().get("a").size());
        Instrumentation.Cron cron2 = new Instrumentation.Cron();
        cron2.start();
        Thread.sleep(INTERVAL);
        cron2.stop();
        inst.addCron("a", "2", cron2);
        assertEquals(1, inst.getTimers().size());
        assertEquals(2, inst.getTimers().get("a").size());
        Instrumentation.Cron cron3 = new Instrumentation.Cron();
        cron3.start();
        Thread.sleep(INTERVAL * 2);
        cron3.stop();
        inst.addCron("b", "1", cron3);
        assertEquals(2, inst.getTimers().size());
        assertEquals(2, inst.getTimers().get("a").size());
        assertEquals(1, inst.getTimers().get("b").size());

        assertEquals(cron1.getOwn(), inst.getTimers().get("a").get("1").getValue().getOwn());
        assertEquals(cron2.getOwn(), inst.getTimers().get("a").get("2").getValue().getOwn());
        assertEquals(cron3.getOwn(), inst.getTimers().get("b").get("1").getValue().getOwn());
    }

    public void testVariables() throws Exception {
        Instrumentation inst = new Instrumentation();

        inst.addVariable("a", "1", new Instrumentation.Variable<Long>() {
            private long counter = 0;

            public Long getValue() {
                return counter++;
            }
        });
        assertEquals(1, inst.getVariables().size());
        assertEquals(1, inst.getVariables().get("a").size());

        inst.addVariable("a", "2", new Instrumentation.Variable<Long>() {
            private long counter = 1;

            public Long getValue() {
                return counter++;
            }
        });
        assertEquals(1, inst.getVariables().size());
        assertEquals(2, inst.getVariables().get("a").size());
        inst.addVariable("b", "1", new Instrumentation.Variable<Long>() {
            private long counter = 2;

            public Long getValue() {
                return counter++;
            }
        });
        assertEquals(2, inst.getVariables().size());
        assertEquals(2, inst.getVariables().get("a").size());
        assertEquals(1, inst.getVariables().get("b").size());

        assertEquals(new Long(0), ((Instrumentation.Variable) inst.getVariables().get("a").get("1")).getValue());
        assertEquals(new Long(1), ((Instrumentation.Variable) inst.getVariables().get("a").get("2")).getValue());
        assertEquals(new Long(2), ((Instrumentation.Variable) inst.getVariables().get("b").get("1")).getValue());
        assertEquals(new Long(1), ((Instrumentation.Variable) inst.getVariables().get("a").get("1")).getValue());
        assertEquals(new Long(2), ((Instrumentation.Variable) inst.getVariables().get("a").get("2")).getValue());
        assertEquals(new Long(3), ((Instrumentation.Variable) inst.getVariables().get("b").get("1")).getValue());
    }

    public void testSamplers() throws Exception {
        Instrumentation inst = new Instrumentation();
        ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
        inst.setScheduler(scheduledExecutorService);

        inst.addSampler("a", "1", 10, 1, new Instrumentation.Variable<Long>() {
            public Long getValue() {
                return 1L;
            }
        });
        assertEquals(1, inst.getSamplers().size());
        assertEquals(1, inst.getSamplers().get("a").size());

        inst.addSampler("a", "2", 10, 1, new Instrumentation.Variable<Long>() {
            public Long getValue() {
                return 2L;
            }
        });
        assertEquals(1, inst.getSamplers().size());
        assertEquals(2, inst.getSamplers().get("a").size());

        inst.addSampler("b", "1", 10, 1, new Instrumentation.Variable<Long>() {
            private long counter = 0;

            public Long getValue() {
                return counter++ % 10;
            }
        });
        assertEquals(2, inst.getSamplers().size());
        assertEquals(2, inst.getSamplers().get("a").size());
        assertEquals(1, inst.getSamplers().get("b").size());

        waitFor(20 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                return false;
            }
        });

        assertEquals("", 1D, inst.getSamplers().get("a").get("1").getValue(), 0.01D);
        assertEquals("", 2D, inst.getSamplers().get("a").get("2").getValue(), 0.02D);
        assertEquals("", 5D, inst.getSamplers().get("b").get("1").getValue(), 0.5D);

        scheduledExecutorService.shutdownNow();
    }

    public void testAll() throws Exception {
        Instrumentation inst = new Instrumentation();
        inst.addVariable("a", "1", new Instrumentation.Variable<Long>() {
            private long counter = 0;

            public Long getValue() {
                return counter++;
            }
        });
        inst.incr("a", "1", 1);
        Instrumentation.Cron cron1 = new Instrumentation.Cron();
        inst.addCron("a", "1", cron1);

        assertEquals(4, inst.getAll().size());
        assertEquals(1, inst.getAll().get("variables").size());
        assertEquals(1, inst.getAll().get("counters").size());
        assertEquals(1, inst.getAll().get("timers").size());
        assertEquals(0, inst.getAll().get("samplers").size());
        assertEquals(new Long(0), ((Instrumentation.Element) inst.getAll().get("variables").get("a").get("1")).getValue());
        assertEquals(new Long(1), ((Instrumentation.Element) inst.getAll().get("counters").get("a").get("1")).getValue());
        assertEquals(cron1.getOwn(), ((Instrumentation.Timer) ((Instrumentation.Element) inst.getAll().
                get("timers").get("a").get("1")).getValue()).getOwn());
    }

}
