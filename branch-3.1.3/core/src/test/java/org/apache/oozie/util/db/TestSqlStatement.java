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
package org.apache.oozie.util.db;

import static org.apache.oozie.util.db.SqlStatement.*;
import static org.apache.oozie.util.db.TestSchema.TestColumns.*;
import static org.apache.oozie.util.db.TestSchema.TestTable.*;
import org.apache.oozie.service.StoreService;
import org.apache.oozie.service.Services;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Calendar;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.db.Schema.Table;

public class TestSqlStatement extends XTestCase {

    private Connection conn;
    private final String[] names = {"a", "b", "c", "d", "e"};
    private Timestamp currTime;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        Services services = new Services();
        services.init();
//        conn = dataSourceServ.getRawConnection();
        conn = TestSchema.getDirectConnection();
        TestSchema.prepareDB(conn);
    }

    @Override
    protected void tearDown() throws Exception {
        TestSchema.dropSchema(conn);
        conn.close();
        super.tearDown();
    }

    public void testSQLStatements() throws SQLException {
        _testInsertAndGetCountAndprepare();
        _testParser();
        _testSelect();
        _testUpdate();
        _testDelete();
    }

    private void _testDelete() throws SQLException {
        ResultSet rs = getCount(TEST_TABLE).where(isEqual(TEST_LONG, 0)).prepareAndSetValues(conn).executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        deleteFrom(TEST_TABLE).where(isEqual(TEST_LONG, 0)).prepareAndSetValues(conn).executeUpdate();
        rs = getCount(TEST_TABLE).where(isEqual(TEST_LONG, 0)).prepareAndSetValues(conn).executeQuery();
        rs.next();
        assertEquals(0, rs.getInt(1));
    }

    private void _testUpdate() throws SQLException {
        update(TEST_TABLE).set(TEST_STRING, "test").where(isEqual(TEST_LONG, 0)).prepareAndSetValues(conn)
                .executeUpdate();
        ResultSetReader rsReader = parse(selectColumns(TEST_STRING, TEST_LONG).where(isEqual(TEST_LONG, 0))
                .prepareAndSetValues(conn).executeQuery());
        rsReader.next();
        assertEquals("test", rsReader.getString(TEST_STRING));
        rsReader.close();

        update(TEST_TABLE).set(TEST_STRING, "a").where(isEqual(TEST_LONG, 0)).prepareAndSetValues(conn).executeUpdate();
        rsReader = parse(selectColumns(TEST_STRING, TEST_LONG).where(isEqual(TEST_LONG, 0)).prepareAndSetValues(conn)
                .executeQuery());
        rsReader.next();
        assertEquals("a", rsReader.getString(TEST_STRING));
        rsReader.close();
    }

    private void _testSelect() throws SQLException {
        ResultSetReader rsReader = parse(selectAllFrom(TEST_TABLE).orderBy(TEST_LONG, true).prepareAndSetValues(conn)
                .executeQuery());
        assertEquals(5, checkIdAndName(rsReader));

        rsReader = parse(selectAllFrom(TEST_TABLE).orderBy(TEST_LONG, true).limit(0, 3).prepareAndSetValues(conn)
                .executeQuery());
        assertEquals(3, checkIdAndName(rsReader));

        rsReader = parse(selectColumns(TEST_STRING, TEST_LONG).orderBy(TEST_LONG, true).prepareAndSetValues(conn)
                .executeQuery());
        assertEquals(5, checkIdAndName(rsReader));

        rsReader = parse(selectColumns(TEST_STRING, TEST_LONG).where(isLike(TEST_STRING, names[0])).orderBy(TEST_LONG,
                                                                                                            true).prepareAndSetValues(conn).executeQuery());
        assertEquals(1, checkIdAndName(rsReader));

        rsReader = parse(selectColumns(TEST_STRING, TEST_LONG).where(isNotLike(TEST_STRING, names[4])).orderBy(
                TEST_LONG, true).prepareAndSetValues(conn).executeQuery());
        assertEquals(4, checkIdAndName(rsReader));

        rsReader = parse(selectColumns(TEST_STRING, TEST_LONG).where(isEqual(TEST_LONG, 0)).orderBy(TEST_LONG, true)
                .prepareAndSetValues(conn).executeQuery());
        assertEquals(1, checkIdAndName(rsReader));

        rsReader = parse(selectColumns(TEST_STRING, TEST_LONG).where(isNotEqual(TEST_LONG, 4)).orderBy(TEST_LONG, true)
                .prepareAndSetValues(conn).executeQuery());
        assertEquals(4, checkIdAndName(rsReader));

        rsReader = parse(selectColumns(TEST_STRING, TEST_LONG).where(lessThan(TEST_LONG, 3)).orderBy(TEST_LONG, true)
                .prepareAndSetValues(conn).executeQuery());
        assertEquals(3, checkIdAndName(rsReader));

        rsReader = parse(selectColumns(TEST_STRING, TEST_LONG).where(lessThanOrEqual(TEST_LONG, 3)).orderBy(TEST_LONG,
                                                                                                            true).prepareAndSetValues(conn).executeQuery());
        assertEquals(4, checkIdAndName(rsReader));

        ResultSet rs = getCount(TEST_TABLE).where(greaterThan(TEST_LONG, 3)).prepareAndSetValues(conn).executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));

        rs = getCount(TEST_TABLE).where(greaterThanOrEqual(TEST_LONG, 3)).prepareAndSetValues(conn).executeQuery();
        rs.next();
        assertEquals(2, rs.getInt(1));

        rsReader = parse(selectColumns(TEST_STRING, TEST_LONG).where(in(TEST_LONG, 0, 1, 2)).orderBy(TEST_LONG, true)
                .prepareAndSetValues(conn).executeQuery());
        assertEquals(3, checkIdAndName(rsReader));

        rsReader = parse(selectColumns(TEST_STRING, TEST_LONG).where(notIn(TEST_LONG, 3, 4)).orderBy(TEST_LONG, true)
                .prepareAndSetValues(conn).executeQuery());
        assertEquals(3, checkIdAndName(rsReader));

        rs = getCount(TEST_TABLE).where(between(TEST_LONG, 1, 3)).prepareAndSetValues(conn).executeQuery();
        rs.next();
        assertEquals(3, rs.getInt(1));

        rs = getCount(TEST_TABLE).where(notBetween(TEST_LONG, 1, 3)).prepareAndSetValues(conn).executeQuery();
        rs.next();
        assertEquals(2, rs.getInt(1));

        rsReader = parse(selectColumns(TEST_STRING, TEST_LONG).where(
                and(isEqual(TEST_LONG, 0), isEqual(TEST_STRING, names[1]))).orderBy(TEST_LONG, true)
                .prepareAndSetValues(conn).executeQuery());
        assertEquals(0, checkIdAndName(rsReader));

        rsReader = parse(selectColumns(TEST_STRING, TEST_LONG).where(
                and(isEqual(TEST_LONG, 0), isEqual(TEST_STRING, names[0]), isEqual(TEST_BOOLEAN, false))).orderBy(
                TEST_LONG, true).prepareAndSetValues(conn).executeQuery());
        assertEquals(1, checkIdAndName(rsReader));

        rsReader = parse(selectColumns(TEST_STRING, TEST_LONG).where(
                or(isEqual(TEST_LONG, 0), isEqual(TEST_STRING, names[1]))).orderBy(TEST_LONG, true)
                .prepareAndSetValues(conn).executeQuery());
        assertEquals(2, checkIdAndName(rsReader));
    }

    private void _testInsertAndGetCountAndprepare() throws SQLException {
        int i;
        List<Map<Object, Object>> maps = new ArrayList<Map<Object, Object>>();
        SqlStatement insert = insertInto(TEST_TABLE).value(TEST_LONG, "1").value(TEST_STRING, "2").value(TEST_BOOLEAN,
                                                                                                         true);
        SqlStatement update = update(TEST_TABLE).set(TEST_BOOLEAN, false).where(
                and(isEqual(TEST_LONG, "1"), isEqual(TEST_STRING, "2")));
        PreparedStatement pUpdate = update.prepare(conn);
        PreparedStatement pInsert = insert.prepare(conn);
        for (i = 0; i < 4; i++) {
            Map<Object, Object> values = new HashMap<Object, Object>();
            values.put("1", i);
            values.put("2", names[i]);
            insert.getNewStatementWithValues(values).prepare(pInsert).execute();
            maps.add(values);
        }

        ResultSet rs = getCount(TEST_TABLE).prepareAndSetValues(conn).executeQuery();
        rs.next();
        int cnt = myGetCount(TEST_TABLE);
        assertEquals(4, cnt);
        assertEquals(rs.getInt(1), cnt);

        ResultSetReader rsReader = parse(selectAllFrom(TEST_TABLE).where(isEqual(TEST_BOOLEAN, true)).orderBy(
                TEST_LONG, true).prepareAndSetValues(conn).executeQuery());
        assertEquals(4, checkIdAndName(rsReader));

        update.prepareForBatch(conn, maps, pUpdate).executeBatch();
        rsReader = parse(selectAllFrom(TEST_TABLE).where(isEqual(TEST_BOOLEAN, false)).orderBy(TEST_LONG, true)
                .prepareAndSetValues(conn).executeQuery());
        assertEquals(4, checkIdAndName(rsReader));

        currTime = new java.sql.Timestamp(Calendar.getInstance().getTimeInMillis());
        SqlStatement stmt = insertInto(TEST_TABLE).value(TEST_LONG, "1").value(TEST_STRING, "2").value(TEST_BOOLEAN,
                                                                                                       "3").value(TEST_TIMESTAMP, "4").value(TEST_BLOB, "5");
        Map<Object, Object> values = new HashMap<Object, Object>();
        values.put("1", i);
        values.put("2", names[i]);
        values.put("3", true);
        values.put("4", currTime);
        values.put("5", names[i].getBytes());
        PreparedStatement pstmt = stmt.prepare(conn);
        stmt.getNewStatementWithValues(values).prepare(pstmt).executeUpdate();
        assertEquals(5, myGetCount(TEST_TABLE));
    }

    private void _testParser() throws SQLException {
        ResultSetReader rsReader = parse(selectAllFrom(TEST_TABLE).where(isEqual(TEST_LONG, 4)).prepareAndSetValues(
                conn).executeQuery());
        rsReader.next();
        assertEquals(4, rsReader.getLong(TEST_LONG).longValue());
        assertEquals(names[4], rsReader.getString(TEST_STRING));
        assertEquals(String.format("yyyyy-mm-dd hh:mm", currTime), String.format("yyyyy-mm-dd hh:mm", rsReader
                .getTimestamp(TEST_TIMESTAMP)));
        assertEquals(true, rsReader.getBoolean(TEST_BOOLEAN).booleanValue());
        assertEquals(names[4], new String(rsReader.getByteArray(TEST_BLOB)));
        rsReader.close();
    }

    private int myGetCount(Table table) throws SQLException {
        ResultSet rs = conn.prepareStatement("SELECT count(*) FROM " + table).executeQuery();
        rs.next();
        return rs.getInt(1);
    }

    private int checkIdAndName(ResultSetReader rsReader) throws SQLException {
        int cnt = 0;
        while (rsReader.next()) {
            assertEquals(cnt, rsReader.getLong(TEST_LONG).longValue());
            assertEquals(names[cnt], rsReader.getString(TEST_STRING));
            cnt++;
        }
        rsReader.close();
        return cnt;
    }
}
