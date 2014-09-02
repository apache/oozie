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

 package org.apache.oozie.command.coord;

import java.util.Calendar;

import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.util.ELEvaluator;

public class CoordELExtensions {
    private static final String PREFIX = "coordext:";

    public static String ph1_today_echo(int hr, int min) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable(".wrap", "true");
        return PREFIX + "today(" + hr + ", " + min + ")"; // Unresolved
    }

    public static String ph2_today_inst(int hr, int min) throws Exception {
        Calendar nominalInstanceCal = CoordELFunctions.getEffectiveNominalTime();
        if (nominalInstanceCal == null) {
            return "";
        }

        Calendar dsInstanceCal = Calendar.getInstance(CoordELFunctions.getDatasetTZ());
        dsInstanceCal.setTime(nominalInstanceCal.getTime());
        dsInstanceCal.set(Calendar.HOUR_OF_DAY, hr);
        dsInstanceCal.set(Calendar.MINUTE, min);
        dsInstanceCal.set(Calendar.SECOND, 0);
        dsInstanceCal.set(Calendar.MILLISECOND, 0);

        int[] instCnt = new int[1];
        Calendar compInstCal = CoordELFunctions
                .getCurrentInstance(dsInstanceCal.getTime(), instCnt);
        if (compInstCal == null) {
            return "";
        }
        int dsInstanceCnt = instCnt[0];

        compInstCal = CoordELFunctions.getCurrentInstance(nominalInstanceCal.getTime(), instCnt);
        if (compInstCal == null) {
            return "";
        }
        int nominalInstanceCnt = instCnt[0];

        return "coord:current(" + (dsInstanceCnt - nominalInstanceCnt) + ")";
    }

    public static String ph2_today(int hr, int min) throws Exception {
        String inst = ph2_today_inst(hr, min);
        return evaluateCurrent(inst);
    }

    private static String evaluateCurrent(String curExpr) throws Exception {
        if (curExpr.equals("")) {
            return curExpr;
        }

        int inst = CoordCommandUtils.parseOneArg(curExpr);
        return CoordELFunctions.ph2_coord_current(inst);
    }
}
