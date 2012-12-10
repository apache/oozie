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
package org.apache.oozie.action.hadoop;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.MessageFormat;

public class TestFsActionExecutor extends ActionExecutorTestCase {

    protected void setSystemProps() throws Exception {
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
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());

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
            fail();
        }
        catch (ActionExecutorException ex) {
            assertEquals("FS002", ex.getErrorCode());
        }

        try {
            ae.validatePath(new Path("bla"), true);
            fail();
        }
        catch (ActionExecutorException ex) {
            assertEquals("FS001", ex.getErrorCode());
        }

        // testing schemes supported
        Services.get().destroy();
        setSystemProperty(HadoopAccessorService.SUPPORTED_FILESYSTEMS, "hdfs,viewfs");
        new Services().init();
        try {
            ae.validatePath(new Path("viewfs://bla"), true);
        }
        catch (ActionExecutorException ex) {
            fail("viewfs is a supported scheme. This should not throw exception");
        }

        try {
            ae.validatePath(new Path("file://bla"), true);

            fail("file is not a supported scheme. This should throw exception");
        }
        catch (ActionExecutorException ex) {
            assertTrue(ex.getMessage().contains("E0904"));
        }
    }

    public void testFileSchemeWildcard() throws Exception {
        FsActionExecutor ae = new FsActionExecutor();
        Services.get().destroy();
        setSystemProperty(HadoopAccessorService.SUPPORTED_FILESYSTEMS, "*");
        new Services().init();

        try {
            ae.validatePath(new Path("anyfs://bla"), true);
        }
        catch (ActionExecutorException ex) {
            fail("Wildcard indicates ALL schemes will be allowed. This should pass");
        }
    }


    public void testResolveToFullPath() throws Exception {
        FsActionExecutor ae = new FsActionExecutor();

        assertEquals(new Path("hdfs://x/bla"), ae.resolveToFullPath(null, new Path("hdfs://x/bla"), true));
        assertEquals(new Path("bla"), ae.resolveToFullPath(null, new Path("bla"), false));

        assertEquals(new Path("hdfs://x/bla"), ae.resolveToFullPath(new Path("hdfs://x"), new Path("/bla"), true));

        assertEquals(new Path("hdfs://x/bla"), ae.resolveToFullPath(new Path("hdfs://x/ha"), new Path("/bla"), true));

        assertEquals(new Path("hdfs://x/bla"), ae.resolveToFullPath(new Path("hdfs://z"), new Path("hdfs://x/bla"), true));

        assertEquals(new Path("hdfs://x/bla"), ae.resolveToFullPath(new Path("hdfs://x"), new Path("hdfs://x/bla"), true));

        try {
            ae.resolveToFullPath(null, new Path("hdfs://x/bla"), false);
            fail();
        }
        catch (ActionExecutorException ex) {
            assertEquals("FS002", ex.getErrorCode());
        }

        try {
            ae.resolveToFullPath(null, new Path("bla"), true);
            fail();
        }
        catch (ActionExecutorException ex) {
            assertEquals("FS001", ex.getErrorCode());	
        }

        Services.get().destroy();
        setSystemProperty(HadoopAccessorService.SUPPORTED_FILESYSTEMS, null);
        new Services().init();
        try {
            ae.resolveToFullPath(null, new Path("file://bla"), true);
            fail();
        }
        catch (ActionExecutorException ex) {
            assertTrue(ex.getMessage().contains("E0904"));
        }

        try {
            ae.resolveToFullPath(new Path("hdfs://z"), new Path("hdfs://x/bla"), false);
            fail();
        }
        catch (ActionExecutorException ex) {
            assertEquals("FS002", ex.getErrorCode());
        }

        try {
            ae.resolveToFullPath(new Path("hdfs://z"), new Path("bla"), true);
            fail();
        }
        catch (ActionExecutorException ex) {
            assertEquals("FS011", ex.getErrorCode());	
        }

        try {
            ae.resolveToFullPath(new Path("hdfs://z"), new Path("file://bla"), true);
            fail();
        }
        catch (ActionExecutorException ex) {
            assertTrue(ex.getMessage().contains("E0904"));
        }
    }

    public void testvalidateSameNN() throws Exception {
        FsActionExecutor ae = new FsActionExecutor();
        ae.validateSameNN(new Path("hdfs://x/bla"), new Path("hdfs://x/foo"));

        try {
            ae.validateSameNN(new Path("hdfs://x/bla"), new Path("viefs://x/bla"));
            fail();
        }
        catch (ActionExecutorException ex) {
            assertEquals("FS007", ex.getErrorCode());   
        }

        try {
            ae.validateSameNN(new Path("hdfs://x/bla"), new Path("hdfs://y/bla"));
            fail();
        }
        catch (ActionExecutorException ex) {
            assertEquals("FS007", ex.getErrorCode());   
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
        fs.createNewFile(new Path(source+"/newfile1"));
        fs.mkdirs(target);

        String dest = target.toUri().getPath();
        Path destPath = new Path(dest);
        ae.move(context, new Path(source+"/newfile1"), destPath, false);

        assertTrue(!fs.exists(new Path(source+"/newfile1")));
        assertTrue(fs.exists(target));

        try {
            ae.move(context, new Path(source+"/newfile1"), destPath, false);
            fail();	
        }
        catch (ActionExecutorException ex) {
            assertEquals("FS006", ex.getErrorCode());
        }

        fs.mkdirs(source); 
        fs.createNewFile(new Path(source+"/newfile"));
        Path complexTarget = new Path(target+"/a/b");
        fs.mkdirs(complexTarget);

        ae.move(context, source, complexTarget, false);
        assertTrue(fs.exists(new Path(complexTarget+"/"+source.getName())));

        fs.mkdirs(source);
        try {
            ae.move(context, source, new Path(target.toUri().getScheme()+"://foo/"+destPath), false);
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
        fs.setPermission(path, FsPermission.valueOf("-rwx------"));
        fs.setPermission(child, FsPermission.valueOf("-rwxr-----"));
        fs.setPermission(grandchild, FsPermission.valueOf("-rwx---r--"));
        assertEquals("rwx------", fs.getFileStatus(path).getPermission().toString());
        assertEquals("rwxr-----", fs.getFileStatus(child).getPermission().toString());
        assertEquals("rwx---r--", fs.getFileStatus(grandchild).getPermission().toString());

        Context context = createContext("<fs/>");

        ae.chmod(context, path, "-rwx-----x", false, false);
        assertEquals("rwx-----x", fs.getFileStatus(path).getPermission().toString());
        assertEquals("rwxr-----", fs.getFileStatus(child).getPermission().toString());
        assertEquals("rwx---r--", fs.getFileStatus(grandchild).getPermission().toString());

        ae.chmod(context, path, "-rwxr----x", true, false);
        assertEquals("rwxr----x", fs.getFileStatus(path).getPermission().toString());
        assertEquals("rwxr----x", fs.getFileStatus(child).getPermission().toString());
        assertEquals("rwx---r--", fs.getFileStatus(grandchild).getPermission().toString());
    }

public void testChmodRecursive() throws Exception {
        FsActionExecutor ae = new FsActionExecutor();
        FileSystem fs = getFileSystem();

        Path path = new Path(getFsTestCaseDir(), "dir");
        Path child = new Path(path, "child");
        Path grandchild = new Path(child, "grandchild");
        fs.mkdirs(grandchild);
        fs.setPermission(path, FsPermission.valueOf("-rwx------"));
        fs.setPermission(child, FsPermission.valueOf("-rwxr-----"));
        fs.setPermission(grandchild, FsPermission.valueOf("-rwx---r--"));
        assertEquals("rwx------", fs.getFileStatus(path).getPermission().toString());
        assertEquals("rwxr-----", fs.getFileStatus(child).getPermission().toString());
        assertEquals("rwx---r--", fs.getFileStatus(grandchild).getPermission().toString());

        Context context = createContext("<fs/>");

        ae.chmod(context, path, "-rwx-----x", false, false);
        assertEquals("rwx-----x", fs.getFileStatus(path).getPermission().toString());
        assertEquals("rwxr-----", fs.getFileStatus(child).getPermission().toString());
        assertEquals("rwx---r--", fs.getFileStatus(grandchild).getPermission().toString());

        ae.chmod(context, path, "-rwx-----x", false, true);
        assertEquals("rwx-----x", fs.getFileStatus(path).getPermission().toString());
        assertEquals("rwxr-----", fs.getFileStatus(child).getPermission().toString());
        assertEquals("rwx---r--", fs.getFileStatus(grandchild).getPermission().toString());

        ae.chmod(context, path, "-rwxr----x", true, true);
        assertEquals("rwxr----x", fs.getFileStatus(path).getPermission().toString());
        assertEquals("rwxr----x", fs.getFileStatus(child).getPermission().toString());
        assertEquals("rwxr----x", fs.getFileStatus(grandchild).getPermission().toString());
    }

    public void testTouchz() throws Exception {
        FsActionExecutor ae = new FsActionExecutor();
        FileSystem fs = getFileSystem();

        Path dir = new Path(getFsTestCaseDir(), "dir1");
        Path f1 = new Path(dir + "/newfile1");
        Path f2 = new Path(dir + "/newfile2");
        Context context = createContext("<fs/>");

        fs.mkdirs(dir);
        fs.createNewFile(f1);

        assertTrue(fs.exists(f1));
        assertTrue(fs.getFileStatus(f1).getLen() == 0);

        ae.touchz(context, f1);
        ae.touchz(context, f2);

        assertTrue(fs.exists(f1));
        assertTrue(fs.exists(f2));

        FileStatus fs1 = fs.getFileStatus(f1);
        FileStatus fs2 = fs.getFileStatus(f2);

        assertFalse(fs1.isDir());
        assertFalse(fs2.isDir());
        assertTrue(fs1.getLen() == 0);
        assertTrue(fs2.getLen() == 0);

        try {
            ae.touchz(context, dir);
            fail();
        }
        catch (Exception ex) {
            assertTrue(ex.getMessage().contains("is a directory"));
        }

        //Test touchz on a non-zero length file
        Path f3 = new Path(dir + "/newfile3");
        Writer writer = new OutputStreamWriter(fs.create(f3));
        writer.write("This is not a zero length file");
        writer.close();

        try {
            ae.touchz(context, f3);
            fail();
        }
        catch (Exception ex) {
            assertTrue(ex.getMessage().contains("must be a zero-length file"));
        }
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
        Path child1 = new Path(chmod1, "child1");
        fs.mkdirs(child1);
        Path chmod2 = new Path(getFsTestCaseDir(), "chmod2");
        fs.mkdirs(chmod2);
        Path child2 = new Path(chmod2, "child2");
        fs.mkdirs(child2);
        Path newFile1 = new Path(mkdir + "newFile1");
        Path newFile2 = new Path(mkdir + "newFile2");
        fs.createNewFile(newFile1);
        Path chmod3 = new Path(getFsTestCaseDir(), "chmod3");
        fs.mkdirs(chmod3);
        Path child3 = new Path(chmod3, "child3");
        fs.mkdirs(child3);
        Path grandchild3 = new Path(child3, "grandchild1");
        fs.mkdirs(grandchild3);

        String str = MessageFormat.format("<root><mkdir path=''{0}''/>" +
                "<delete path=''{1}''/>" +
                "<move source=''{2}'' target=''{3}''/>" +
                "<chmod path=''{4}'' permissions=''-rwxrwxrwx''/>" +
                "<chmod path=''{5}'' permissions=''-rwxrwx---'' dir-files=''false''/>" +
                "<touchz path=''{6}''/>" +
                "<touchz path=''{7}''/>" +
                "<chmod path=''{8}'' permissions=''-rwxrwx---''> <recursive/> </chmod>" +
                "</root>", mkdir, delete, source, target, chmod1, chmod2, newFile1, newFile2, chmod3);

        Element xml = XmlUtils.parseXml(str);

        ae.doOperations(createContext("<fs/>"), xml);

        assertTrue(fs.exists(mkdir));
        assertFalse(fs.exists(delete));
        assertFalse(fs.exists(source));
        assertTrue(fs.exists(target));
        assertTrue(fs.exists(newFile1));
        assertTrue(fs.exists(newFile2));

        assertEquals("rwxrwxrwx", fs.getFileStatus(chmod1).getPermission().toString());
        assertNotSame("rwxrwxrwx", fs.getFileStatus(child1).getPermission().toString());
        assertEquals("rwxrwx---", fs.getFileStatus(chmod2).getPermission().toString());
        assertNotSame("rwxrwx---", fs.getFileStatus(child2).getPermission().toString());

        assertEquals("rwxrwx---", fs.getFileStatus(child3).getPermission().toString());
        assertEquals("rwxrwx---", fs.getFileStatus(grandchild3).getPermission().toString());

        assertEquals("rwxrwx---", fs.getFileStatus(child3).getPermission().toString());
        assertEquals("rwxrwx---", fs.getFileStatus(grandchild3).getPermission().toString());
    }
    
    public void testDoOperationsWithNameNodeElement() throws Exception {
        FsActionExecutor ae = new FsActionExecutor();
        FileSystem fs = getFileSystem();

        Path mkdir = new Path(getFsTestCaseDir(), "mkdir");
        Path mkdirX = new Path(mkdir.toUri().getPath());
        Path delete = new Path(getFsTestCaseDir(), "delete");
        Path deleteX = new Path(delete.toUri().getPath());
        fs.mkdirs(delete);
        Path source = new Path(getFsTestCaseDir(), "source");
        Path sourceX = new Path(source.toUri().getPath());
        fs.mkdirs(source);
        Path target = new Path(new Path(getFsTestCaseDir(), "target").toUri().getPath());
        Path chmod1 = new Path(getFsTestCaseDir(), "chmod1");
        Path chmod1X = new Path(chmod1.toUri().getPath());
        fs.mkdirs(chmod1);
        Path child1 = new Path(chmod1, "child1");
        fs.mkdirs(child1);
        Path chmod2 = new Path(getFsTestCaseDir(), "chmod2");
        Path chmod2X = new Path(chmod2.toUri().getPath());
        fs.mkdirs(chmod2);
        Path child2 = new Path(chmod2, "child2");
        fs.mkdirs(child2);
        Path newFile1 = new Path(mkdir + "newFile1");
        Path newFile1X = new Path(newFile1.toUri().getPath());
        Path newFile2 = new Path(mkdir + "newFile2");
        Path newFile2X = new Path(newFile2.toUri().getPath());
        fs.createNewFile(newFile1);
        Path chmod3 = new Path(getFsTestCaseDir(), "chmod3");
        Path chmod3X = new Path(chmod3.toUri().getPath());
        fs.mkdirs(chmod3);
        Path child3 = new Path(chmod3, "child3");
        fs.mkdirs(child3);
        Path grandchild3 = new Path(child3, "grandchild1");
        fs.mkdirs(grandchild3);

        String str = MessageFormat.format("<root><name-node>{0}</name-node>" +
                "<mkdir path=''{1}''/>" +
                "<delete path=''{2}''/>" +
                "<move source=''{3}'' target=''{4}''/>" +
                "<chmod path=''{5}'' permissions=''-rwxrwxrwx''/>" +
                "<chmod path=''{6}'' permissions=''-rwxrwx---'' dir-files=''false''/>" +
                "<touchz path=''{7}''/>" +
                "<touchz path=''{8}''/>" +
                "<chmod path=''{9}'' permissions=''-rwxrwx---''> <recursive/> </chmod>" +
                "</root>", getNameNodeUri(), mkdirX, deleteX, sourceX, target, chmod1X, chmod2X, newFile1X, newFile2X, chmod3X);

        Element xml = XmlUtils.parseXml(str);

        ae.doOperations(createContext("<fs/>"), xml);

        assertTrue(fs.exists(mkdir));
        assertFalse(fs.exists(delete));
        assertFalse(fs.exists(source));
        assertTrue(fs.exists(target));
        assertTrue(fs.exists(newFile1));
        assertTrue(fs.exists(newFile2));

        assertEquals("rwxrwxrwx", fs.getFileStatus(chmod1).getPermission().toString());
        assertNotSame("rwxrwxrwx", fs.getFileStatus(child1).getPermission().toString());
        assertEquals("rwxrwx---", fs.getFileStatus(chmod2).getPermission().toString());
        assertNotSame("rwxrwx---", fs.getFileStatus(child2).getPermission().toString());

        assertEquals("rwxrwx---", fs.getFileStatus(child3).getPermission().toString());
        assertEquals("rwxrwx---", fs.getFileStatus(grandchild3).getPermission().toString());

        assertEquals("rwxrwx---", fs.getFileStatus(child3).getPermission().toString());
        assertEquals("rwxrwx---", fs.getFileStatus(grandchild3).getPermission().toString());
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
        Path child1 = new Path(chmod1, "child1");
        fs.mkdirs(child1);
        Path chmod2 = new Path(getFsTestCaseDir(), "chmod2");
        fs.mkdirs(chmod2);
        Path child2 = new Path(chmod2, "child2");
        fs.mkdirs(child2);
        Path newFile1 = new Path(mkdir + "newFile1");
        Path newFile2 = new Path(mkdir + "newFile2");
        fs.createNewFile(newFile1);

        String actionXml = MessageFormat.format("<fs><mkdir path=''{0}''/>" +
                "<delete path=''{1}''/>" +
                "<move source=''{2}'' target=''{3}''/>" +
                "<chmod path=''{4}'' permissions=''-rwxrwxrwx''/>" +
                "<chmod path=''{5}'' permissions=''-rwxrwx---'' dir-files=''false''/>" +
                "<touchz path=''{6}''/>" +
                "<touchz path=''{7}''/>" +
                "</fs>", mkdir, delete, source, target, chmod1, chmod2, newFile1, newFile2);

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
        assertTrue(fs.exists(newFile1));
        assertTrue(fs.exists(newFile2));

        assertEquals("rwxrwxrwx", fs.getFileStatus(chmod1).getPermission().toString());
        assertNotSame("rwxrwxrwx", fs.getFileStatus(child1).getPermission().toString());
        assertEquals("rwxrwx---", fs.getFileStatus(chmod2).getPermission().toString());
        assertNotSame("rwxrwx---", fs.getFileStatus(child2).getPermission().toString());

    }
    
    public void testSubmitWithNameNode() throws Exception {
        FsActionExecutor ae = new FsActionExecutor();
        FileSystem fs = getFileSystem();

        Path mkdir = new Path(getFsTestCaseDir(), "mkdir");
        Path mkdirX = new Path(mkdir.toUri().getPath());
        Path delete = new Path(getFsTestCaseDir(), "delete");
        Path deleteX = new Path(delete.toUri().getPath());
        fs.mkdirs(delete);
        Path source = new Path(getFsTestCaseDir(), "source");
        Path sourceX = new Path(source.toUri().getPath());
        fs.mkdirs(source);
        Path target = new Path(new Path(getFsTestCaseDir(), "target").toUri().getPath());
        Path chmod1 = new Path(getFsTestCaseDir(), "chmod1");
        Path chmod1X = new Path(chmod1.toUri().getPath());
        fs.mkdirs(chmod1);
        Path child1 = new Path(chmod1, "child1");
        fs.mkdirs(child1);
        Path chmod2 = new Path(getFsTestCaseDir(), "chmod2");
        Path chmod2X = new Path(chmod2.toUri().getPath());
        fs.mkdirs(chmod2);
        Path child2 = new Path(chmod2, "child2");
        fs.mkdirs(child2);
        Path newFile1 = new Path(mkdir + "newFile1");
        Path newFile1X = new Path(newFile1.toUri().getPath());
        Path newFile2 = new Path(mkdir + "newFile2");
        Path newFile2X = new Path(newFile2.toUri().getPath());
        fs.createNewFile(newFile1);

        String actionXml = MessageFormat.format("<fs><name-node>{0}</name-node>" + 
                "<mkdir path=''{1}''/>" +
                "<delete path=''{2}''/>" +
                "<move source=''{3}'' target=''{4}''/>" +
                "<chmod path=''{5}'' permissions=''-rwxrwxrwx''/>" +
                "<chmod path=''{6}'' permissions=''-rwxrwx---'' dir-files=''false''/>" +
                "<touchz path=''{7}''/>" +
                "<touchz path=''{8}''/>" +
                "</fs>", getNameNodeUri(), mkdirX, deleteX, sourceX, target, chmod1X, chmod2X, newFile1X, newFile2X);

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
        assertTrue(fs.exists(newFile1));
        assertTrue(fs.exists(newFile2));

        assertEquals("rwxrwxrwx", fs.getFileStatus(chmod1).getPermission().toString());
        assertNotSame("rwxrwxrwx", fs.getFileStatus(child1).getPermission().toString());
        assertEquals("rwxrwx---", fs.getFileStatus(chmod2).getPermission().toString());
        assertNotSame("rwxrwx---", fs.getFileStatus(child2).getPermission().toString());

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
        Path child1 = new Path(chmod1, "child1");
        fs.mkdirs(child1);
        Path chmod2 = new Path(getFsTestCaseDir(), "chmod2");
        fs.mkdirs(chmod2);
        Path child2 = new Path(chmod2, "child2");
        fs.mkdirs(child2);

        String actionXml = MessageFormat.format("<fs>" +
                "<mkdir path=''{0}''/>" +
                "<delete path=''{1}''/>" +
                "<move source=''{2}'' target=''{3}''/>" +
                "<chmod path=''{4}'' permissions=''111''/>" +
                "<chmod path=''{5}'' permissions=''222'' dir-files=''false''/>" +
                "</fs>", mkdir, delete, source.toUri().getPath(), target, chmod1, chmod2);


        String id = "ID" + System.currentTimeMillis();
        Context context = createContext(actionXml);
        ((WorkflowJobBean) context.getWorkflow()).setId(id);
        ((WorkflowActionBean) context.getWorkflow().getActions().get(0)).setJobId(id);
        ((WorkflowActionBean) context.getWorkflow().getActions().get(0)).setId(id + "-FS");

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
                "<chmod path=''{4}'' permissions=''-rwxrwxrwx''/>" +
                "<chmod path=''{5}'' permissions=''-rwxrwx---'' dir-files=''false''/>" +
                "</fs>", mkdir, delete, source, target, chmod1, chmod2);

        context = createContext(actionXml);
        ((WorkflowJobBean) context.getWorkflow()).setId(id);
        ((WorkflowActionBean) context.getWorkflow().getActions().get(0)).setJobId(id);
        ((WorkflowActionBean) context.getWorkflow().getActions().get(0)).setId(id + "-FS");

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

        assertEquals("rwxrwxrwx", fs.getFileStatus(chmod1).getPermission().toString());
        assertNotSame("rwxrwxrwx", fs.getFileStatus(child1).getPermission().toString());
        assertEquals("rwxrwx---", fs.getFileStatus(chmod2).getPermission().toString());
        assertNotSame("rwxrwx---", fs.getFileStatus(child2).getPermission().toString());
    }

    public void testPermissionMask() throws Exception {
        FsActionExecutor ae = new FsActionExecutor();
        assertEquals("rwxrwxrwx", ae.createShortPermission("777", null).toString());
        assertEquals("rwxrwxrwx", ae.createShortPermission("-rwxrwxrwx", null).toString());
        assertEquals("r--------", ae.createShortPermission("400", null).toString());
        assertEquals("r--------", ae.createShortPermission("-r--------", null).toString());
    }

}
