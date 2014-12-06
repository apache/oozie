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

package org.apache.oozie.workflow.lite;

import java.sql.Connection;
import java.sql.SQLException;
import javax.xml.validation.Schema;

import org.apache.oozie.store.OozieSchema.OozieColumn;
import org.apache.oozie.store.OozieSchema.OozieTable;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.WritableUtils;
import org.apache.oozie.util.db.SqlStatement.ResultSetReader;
import org.apache.oozie.util.db.SqlStatement;
import org.apache.oozie.ErrorCode;

//TODO javadoc
public class DBLiteWorkflowLib extends LiteWorkflowLib {
    private final Connection connection;

    public DBLiteWorkflowLib(Schema schema,
                             Class<? extends ControlNodeHandler> controlNodeHandler,
                             Class<? extends DecisionNodeHandler> decisionHandlerClass,
                             Class<? extends ActionNodeHandler> actionHandlerClass, Connection connection) {
        super(schema, controlNodeHandler, decisionHandlerClass, actionHandlerClass);
        this.connection = connection;
    }

    /**
     * Save the Workflow Instance for the given Workflow Application.
     *
     * @param instance
     * @return
     * @throws WorkflowException
     */
    @Override
    public void insert(WorkflowInstance instance) throws WorkflowException {
        ParamChecker.notNull(instance, "instance");
        try {
            SqlStatement.insertInto(OozieTable.WF_PROCESS_INSTANCE).value(OozieColumn.PI_wfId, instance.getId()).value(
                    OozieColumn.PI_state, WritableUtils.toByteArray((LiteWorkflowInstance) instance))
                    .prepareAndSetValues(connection).executeUpdate();
        }
        catch (SQLException e) {
            throw new WorkflowException(ErrorCode.E0713, e.getMessage(), e);
        }
    }

    /**
     * Loads the Workflow instance with the given ID.
     *
     * @param id
     * @return
     * @throws WorkflowException
     */
    @Override
    public WorkflowInstance get(String id) throws WorkflowException {
        ParamChecker.notNull(id, "id");
        try {
            ResultSetReader rs = SqlStatement.parse(SqlStatement.selectColumns(OozieColumn.PI_state).where(
                    SqlStatement.isEqual(OozieColumn.PI_wfId, ParamChecker.notNull(id, "id"))).
                    prepareAndSetValues(connection).executeQuery());
            rs.next();
            LiteWorkflowInstance pInstance = WritableUtils.fromByteArray(rs.getByteArray(OozieColumn.PI_state),
                                                                         LiteWorkflowInstance.class);
            return pInstance;
        }
        catch (SQLException e) {
            throw new WorkflowException(ErrorCode.E0713, e.getMessage(), e);
        }
    }

    /**
     * Updates the Workflow Instance to DB.
     *
     * @param instance
     * @throws WorkflowException
     */
    @Override
    public void update(WorkflowInstance instance) throws WorkflowException {
        ParamChecker.notNull(instance, "instance");
        try {
            SqlStatement.update(OozieTable.WF_PROCESS_INSTANCE).set(OozieColumn.PI_state,
                                                                    WritableUtils.toByteArray((LiteWorkflowInstance) instance)).where(
                    SqlStatement.isEqual(OozieColumn.PI_wfId, instance.getId())).
                    prepareAndSetValues(connection).executeUpdate();
        }
        catch (SQLException e) {
            throw new WorkflowException(ErrorCode.E0713, e.getMessage(), e);
        }
    }

    /**
     * Delets the Workflow Instance with the given id.
     *
     * @param id
     * @throws WorkflowException
     */
    @Override
    public void delete(String id) throws WorkflowException {
        ParamChecker.notNull(id, "id");
        try {
            SqlStatement.deleteFrom(OozieTable.WF_PROCESS_INSTANCE).where(
                    SqlStatement.isEqual(OozieColumn.PI_wfId, id)).prepareAndSetValues(connection).executeUpdate();
        }
        catch (SQLException e) {
            throw new WorkflowException(ErrorCode.E0713, e.getMessage(), e);
        }
    }

    @Override
    public void commit() throws WorkflowException {
        // NOP
    }

    @Override
    public void close() throws WorkflowException {
        // NOP
    }
}
