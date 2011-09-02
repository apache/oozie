/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.action.hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;

import java.text.MessageFormat;

public class TestFsActionExecutor extends ActionExecutorTestCase {

    protected void setSystemProps() {
        super.setSystemProps();
        setSystemProperty("oozie.service.ActionService.executor.classes", FsActionExecutor.class.getName());
    }

    public void testSetupMethods() throws Exception {
        FsActionExecutor ae = new FsActionExecutor();
        assertEquals("fs", ae.getType());
    }

    private Context createContext(String actionXml) throws Exception {
        FsActionExecutor ae = new FsActionExecutor();

        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, System.getProperty("user.name"));
        protoConf.set(WorkflowAppService.HADOOP_UGI, System.getProperty("user.name") + ",other");

        WorkflowJobBean wf = createBaseWorkflow(protoConf, "fs-action");
        WorkflowActionBean action = (WorkflowActionBean) wf.getActions().get(0);
        action.setType(ae.getType());
        action.setConf(actionXml);

        return new Context(wf, action);
    }

    public void testValidatePath() throws Exception {
        FsActionExecutor ae = new FsActionExecutor();
        ae.validatePath(new Path("hdfs://x/bla"), true);
        ae.validatePath(new Path("bla"), false);

        try {
            ae.validatePath(new Path("hdfs://x/bla"), false);
        }
        catch (ActionExecutorException ex) {
            assertEquals("FS003", ex.getErrorCode());
        }

        try {
            ae.validatePath(new Path("bla"), true);
        }
        catch (ActionExecutorException ex) {
            assertEquals("FS001", ex.getErrorCode());
        }

        try {
            ae.validatePath(new Path("file://bla"), true);
        }
        catch (ActionExecutorException ex) {
            assertEquals("FS002", ex.getErrorCode());
        }
    }

    public void testMkdir() throws Exception {
        FsActionExecutor ae = new FsActionExecutor();

        FileSystem fs = getFileSystem();

        Path path = new Path(getFsTestCaseDir(), "dir1");

        Context context = createContext("<fs/>");

        ae.mkdir(context, path);

        assertTrue(fs.exists(path));

        ae.mkdir(context, path);
    }

    public void testDelete() throws Exception {
        FsActionExecutor ae = new FsActionExecutor();
        FileSystem fs = getFileSystem();

        Path path = new Path(getFsTestCaseDir(), "dir1");

        Context context = createContext("<fs/>");

        fs.mkdirs(path);

        ae.delete(context, path);

        assertTrue(!fs.exists(path));

        ae.delete(context, path);
    }

    public void testMove() throws Exception {
        FsActionExecutor ae = new FsActionExecutor();
        FileSystem fs = getFileSystem();

        Path source = new Path(getFsTestCaseDir(), "source");
        Path target = new Path(getFsTestCaseDir(), "target");

        Context context = createContext("<fs/>");

        fs.mkdirs(source);

        ae.move(context, source, new Path(target.toUri().getPath()), false);

        assertTrue(!fs.exists(source));
        assertTrue(fs.exists(target));

        try {
            ae.move(context, source, new Path(target.toUri().getPath()), false);
            fail();
        }
        catch (ActionExecutorException ex) {
            assertEquals("FS006", ex.getErrorCode());
        }

        fs.mkdirs(source);
        try {
            ae.move(context, source, new Path(target.toUri().getPath()), false);
            fail();
        }
        catch (ActionExecutorException ex) {
            assertEquals("FS007", ex.getErrorCode());
        }

        fs.delete(source, true);
        ae.move(context, source, new Path(target.toUri().getPath()), true);

        fs.mkdirs(source);
        fs.delete(target, true);
        ae.move(context, source, new Path(target.toUri().getPath()), true);
        assertTrue(!fs.exists(source));
        assertTrue(fs.exists(target));
    }

    public void testChmod() throws Exception {
        FsActionExecutor ae = new FsActionExecutor();
        FileSystem fs = getFileSystem();

        Path path = new Path(getFsTestCaseDir(), "dir");
        Path child = new Path(path, "child");
        Path grandchild = new Path(child, "grandchild");
        fs.mkdirs(grandchild);
        fs.setPermission(path, new FsPermission((short) 0));
        fs.setPermission(child, new FsPermission((short) 0));
        fs.setPermission(grandchild, new FsPermission((short) 0));

        Context context = createContext("<fs/>");

        ae.chmod(context, path, "-r--r--r--", false);
        assertEquals("r--r--r--", fs.getFileStatus(path).getPermission().toString());
        assertEquals("---------", fs.getFileStatus(child).getPermission().toString());
        assertEquals("---------", fs.getFileStatus(grandchild).getPermission().toString());

        ae.chmod(context, path, "111", false);
        assertEquals(111, fs.getFileStatus(path).getPermission().toShort());
        assertEquals(0, fs.getFileStatus(child).getPermission().toShort());
        assertEquals(0, fs.getFileStatus(grandchild).getPermission().toShort());

        ae.chmod(context, path, "-r--r--r--", true);
        assertEquals("r--r--r--", fs.getFileStatus(path).getPermission().toString());
        assertEquals("r--r--r--", fs.getFileStatus(child).getPermission().toString());
        assertEquals("---------", fs.getFileStatus(grandchild).getPermission().toString());
    }

    public void testDoOperations() throws Exception {
        FsActionExecutor ae = new FsActionExecutor();
        FileSystem fs = getFileSystem();

        Path mkdir = new Path(getFsTestCaseDir(), "mkdir");
        Path delete = new Path(getFsTestCaseDir(), "delete");
        fs.mkdirs(delete);
        Path source = new Path(getFsTestCaseDir(), "source");
        fs.mkdirs(source);
        Path target = new Path(new Path(getFsTestCaseDir(), "target").toUri().getPath());
        Path chmod1 = new Path(getFsTestCaseDir(), "chmod1");
        fs.mkdirs(chmod1);
        fs.setPermission(chmod1, new FsPermission((short)0));
        Path child1  = new Path(getFsTestCaseDir(), "child1");
        fs.mkdirs(child1);
        fs.setPermission(child1, new FsPermission((short)0));
        Path chmod2  = new Path(getFsTestCaseDir(), "chmod2");
        fs.mkdirs(chmod2);
        fs.setPermission(chmod2, new FsPermission((short)0));
        Path child2  = new Path(getFsTestCaseDir(), "child2");
        fs.mkdirs(child2);
        fs.setPermission(child2, new FsPermission((short)0));

        String str = MessageFormat.format("<root><mkdir path=''{0}''/>" +
                                          "<delete path=''{1}''/>" +
                                          "<move source=''{2}'' target=''{3}''/>" +
                                          "<chmod path=''{4}'' permissions=''111''/>" +
                                          "<chmod path=''{5}'' permissions=''222'' dir-files=''false''/>" +
                                          "</root>", mkdir, delete, source, target, chmod1, chmod2);

        Element xml = XmlUtils.parseXml(str);

        ae.doOperations(createContext("<fs/>"), xml);

        assertTrue(fs.exists(mkdir));
        assertFalse(fs.exists(delete));
        assertFalse(fs.exists(source));
        assertTrue(fs.exists(target));

        assertEquals(111, fs.getFileStatus(chmod1).getPermission().toShort());
        assertNotSame(111, fs.getFileStatus(child1).getPermission().toShort());
        assertEquals(222, fs.getFileStatus(chmod2).getPermission().toShort());
        assertEquals(0, fs.getFileStatus(child2).getPermission().toShort());

    }

    public void testSubmit() throws Exception {
        FsActionExecutor ae = new FsActionExecutor();
        FileSystem fs = getFileSystem();

        Path mkdir = new Path(getFsTestCaseDir(), "mkdir");
        Path delete = new Path(getFsTestCaseDir(), "delete");
        fs.mkdirs(delete);
        Path source = new Path(getFsTestCaseDir(), "source");
        fs.mkdirs(source);
        Path target = new Path(new Path(getFsTestCaseDir(), "target").toUri().getPath());
        Path chmod1 = new Path(getFsTestCaseDir(), "chmod1");
        fs.mkdirs(chmod1);
        fs.setPermission(chmod1, new FsPermission((short)0));
        Path child1  = new Path(getFsTestCaseDir(), "child1");
        fs.mkdirs(child1);
        fs.setPermission(child1, new FsPermission((short)0));
        Path chmod2  = new Path(getFsTestCaseDir(), "chmod2");
        fs.mkdirs(chmod2);
        fs.setPermission(chmod2, new FsPermission((short)0));
        Path child2  = new Path(getFsTestCaseDir(), "child2");
        fs.mkdirs(child2);
        fs.setPermission(child2, new FsPermission((short)0));
        
        String actionXml = MessageFormat.format("<fs><mkdir path=''{0}''/>" +
                                          "<delete path=''{1}''/>" +
                                          "<move source=''{2}'' target=''{3}''/>" +
                                          "<chmod path=''{4}'' permissions=''111''/>" +
                                          "<chmod path=''{5}'' permissions=''222'' dir-files=''false''/>" +
                                          "</fs>", mkdir, delete, source, target, chmod1, chmod2);


        Context context = createContext(actionXml);
        WorkflowAction action = context.getAction();

        assertFalse(fs.exists(ae.getRecoveryPath(context)));

        ae.start(context, action);

        assertTrue(fs.exists(ae.getRecoveryPath(context)));

        ae.check(context, context.getAction());
        assertEquals("OK", context.getAction().getExternalStatus());
        assertNull(context.getAction().getData());
        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());

        assertFalse(fs.exists(ae.getRecoveryPath(context)));
        
        assertTrue(fs.exists(mkdir));
        assertFalse(fs.exists(delete));
        assertFalse(fs.exists(source));
        assertTrue(fs.exists(target));

        assertEquals(111, fs.getFileStatus(chmod1).getPermission().toShort());
        assertNotSame(111, fs.getFileStatus(child1).getPermission().toShort());
        assertEquals(222, fs.getFileStatus(chmod2).getPermission().toShort());
        assertEquals(0, fs.getFileStatus(child2).getPermission().toShort());

    }

    public void testRecovery() throws Exception {
        FsActionExecutor ae = new FsActionExecutor();
        FileSystem fs = getFileSystem();

        Path mkdir = new Path(getFsTestCaseDir(), "mkdir");
        Path delete = new Path(getFsTestCaseDir(), "delete");
        fs.mkdirs(delete);
        Path source = new Path(getFsTestCaseDir(), "source");
        fs.mkdirs(source);
        Path target = new Path(new Path(getFsTestCaseDir(), "target").toUri().getPath());
        Path chmod1 = new Path(getFsTestCaseDir(), "chmod1");
        fs.mkdirs(chmod1);
        fs.setPermission(chmod1, new FsPermission((short)0));
        Path child1  = new Path(getFsTestCaseDir(), "child1");
        fs.mkdirs(child1);
        fs.setPermission(child1, new FsPermission((short)0));
        Path chmod2  = new Path(getFsTestCaseDir(), "chmod2");
        fs.mkdirs(chmod2);
        fs.setPermission(chmod2, new FsPermission((short)0));
        Path child2  = new Path(getFsTestCaseDir(), "child2");
        fs.mkdirs(child2);
        fs.setPermission(child2, new FsPermission((short)0));

        String actionXml = MessageFormat.format("<fs>" +
                                          "<mkdir path=''{0}''/>" +
                                          "<delete path=''{1}''/>" +
                                          "<move source=''{2}'' target=''{3}''/>" +
                                          "<chmod path=''{4}'' permissions=''111''/>" +
                                          "<chmod path=''{5}'' permissions=''222'' dir-files=''false''/>" +
                                          "</fs>", mkdir, delete, source.toUri().getPath(), target, chmod1, chmod2);


        String id = "ID" + System.currentTimeMillis();
        Context context = createContext(actionXml);
        ((WorkflowJobBean)context.getWorkflow()).setId(id);
        ((WorkflowActionBean)context.getWorkflow().getActions().get(0)).setJobId(id);
        ((WorkflowActionBean)context.getWorkflow().getActions().get(0)).setId(id + "-FS");

        WorkflowAction action = context.getAction();

        assertFalse(fs.exists(ae.getRecoveryPath(context)));

        try {
            ae.start(context, action);
        }
        catch (ActionExecutorException ex) {
            if (!ex.getErrorCode().equals("FS001")) {
                throw ex;
            }
        }

        assertTrue(fs.exists(mkdir));
        assertFalse(fs.exists(delete));

        assertTrue(fs.exists(ae.getRecoveryPath(context)));

        actionXml = MessageFormat.format("<fs>" +
                                         "<mkdir path=''{0}''/>" +
                                         "<delete path=''{1}''/>" +
                                         "<move source=''{2}'' target=''{3}''/>" +
                                         "<chmod path=''{4}'' permissions=''111''/>" +
                                         "<chmod path=''{5}'' permissions=''222'' dir-files=''false''/>" +
                                         "</fs>", mkdir, delete, source, target, chmod1, chmod2);

        context = createContext(actionXml);
        ((WorkflowJobBean)context.getWorkflow()).setId(id);
        ((WorkflowActionBean)context.getWorkflow().getActions().get(0)).setJobId(id);
        ((WorkflowActionBean)context.getWorkflow().getActions().get(0)).setId(id + "-FS");

        action = context.getAction();

        ae.start(context, action);

        ae.check(context, context.getAction());
        assertEquals("OK", context.getAction().getExternalStatus());
        assertNull(context.getAction().getData());
        ae.end(context, context.getAction());
        assertEquals(WorkflowAction.Status.OK, context.getAction().getStatus());

        assertFalse(fs.exists(ae.getRecoveryPath(context)));

        assertTrue(fs.exists(mkdir));
        assertFalse(fs.exists(delete));
        assertFalse(fs.exists(source));
        assertTrue(fs.exists(target));

        assertEquals(111, fs.getFileStatus(chmod1).getPermission().toShort());
        assertNotSame(111, fs.getFileStatus(child1).getPermission().toShort());
        assertEquals(222, fs.getFileStatus(chmod2).getPermission().toShort());
        assertEquals(0, fs.getFileStatus(child2).getPermission().toShort());

    }

}