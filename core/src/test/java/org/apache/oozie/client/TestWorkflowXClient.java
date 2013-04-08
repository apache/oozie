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
package org.apache.oozie.client;

import org.apache.hadoop.fs.Path;
import org.apache.oozie.servlet.DagServletTestCase;
import org.apache.oozie.servlet.MockDagEngineService;
import org.apache.oozie.servlet.V1AdminServlet;
import org.apache.oozie.servlet.V1JobsServlet;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Properties;
import java.util.concurrent.Callable;

public class TestWorkflowXClient extends DagServletTestCase {

	static {
		new HeaderTestingVersionServlet();
		new V1JobsServlet();
		new V1AdminServlet();
	}

	private static final boolean IS_SECURITY_ENABLED = false;
	static final String VERSION = "/v" + OozieClient.WS_PROTOCOL_VERSION;
	static final String[] END_POINTS = { "/versions", VERSION + "/jobs",
	    VERSION + "/admin/*" };
	static final Class[] SERVLET_CLASSES = { HeaderTestingVersionServlet.class,
	    V1JobsServlet.class, V1AdminServlet.class };

	protected void setUp() throws Exception {
		super.setUp();
		MockDagEngineService.reset();
	}

	public void testSubmitPig() throws Exception {
		runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED,
		    new Callable<Void>() {
			    public Void call() throws Exception {
				    String oozieUrl = getContextURL();
				    int wfCount = MockDagEngineService.INIT_WF_COUNT;
				    XOozieClient wc = new XOozieClient(oozieUrl);
				    Properties conf = wc.createConfiguration();
				    Path libPath = new Path(getFsTestCaseDir(), "lib");
				    getFileSystem().mkdirs(libPath);
				    conf.setProperty(OozieClient.LIBPATH, libPath.toString());
				    conf.setProperty(XOozieClient.JT, "localhost:9001");
				    conf.setProperty(XOozieClient.NN, "hdfs://localhost:9000");

				    String pigScriptFile = getTestCaseDir() + "/test";
				    BufferedWriter writer = new BufferedWriter(new FileWriter(
				        pigScriptFile));
				    writer.write("a = load 'input.txt';\n dump a;");
				    writer.close();
				    assertEquals(MockDagEngineService.JOB_ID + wfCount
				        + MockDagEngineService.JOB_ID_END,
				        wc.submitScriptLanguage(conf, pigScriptFile, null, "pig"));

				    assertTrue(MockDagEngineService.started.get(wfCount));
				    return null;
			    }
		    });
	}

	public void testSubmitHive() throws Exception {
		runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED,
		    new Callable<Void>() {
			    public Void call() throws Exception {
				    String oozieUrl = getContextURL();
				    int wfCount = MockDagEngineService.INIT_WF_COUNT;
				    XOozieClient wc = new XOozieClient(oozieUrl);
				    Properties conf = wc.createConfiguration();
				    Path libPath = new Path(getFsTestCaseDir(), "lib");
				    getFileSystem().mkdirs(libPath);
				    System.out.println(libPath.toString());
				    conf.setProperty(OozieClient.LIBPATH, libPath.toString());
				    wc.setLib(conf, libPath.toString());
				    assertEquals(libPath.toString(), conf.get(OozieClient.LIBPATH));
				    conf.setProperty(XOozieClient.JT, "localhost:9001");
				    conf.setProperty(XOozieClient.NN, "hdfs://localhost:9000");

				    String hiveScriptFile = getTestCaseDir() + "/test";
				    System.out.println(hiveScriptFile);
				    BufferedWriter writer = new BufferedWriter(new FileWriter(
				        hiveScriptFile));
				    writer.write("CREATE EXTERNAL TABLE test (a INT);");
				    writer.close();
				    assertEquals(MockDagEngineService.JOB_ID + wfCount
				        + MockDagEngineService.JOB_ID_END,
				        wc.submitScriptLanguage(conf, hiveScriptFile, null, "hive"));

				    assertTrue(MockDagEngineService.started.get(wfCount));
				    return null;
			    }
		    });
	}

	public void testSubmitMR() throws Exception {
		runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED,
		    new Callable<Void>() {
			    public Void call() throws Exception {
				    String oozieUrl = getContextURL();
				    int wfCount = MockDagEngineService.INIT_WF_COUNT;
				    XOozieClient wc = new XOozieClient(oozieUrl);
				    Properties configuration = wc.createConfiguration();
				    Path libPath = new Path(getFsTestCaseDir(), "lib");
				    
				   				    getFileSystem().mkdirs(libPath);
				    
				    // try to submit without JT and NN
				    try {
					    wc.submitMapReduce(configuration);
					    fail("submit client without JT should throuhg exception");
				    } catch (RuntimeException exception) {
					    assertEquals(
                      "java.lang.RuntimeException: jobtracker is not specified in conf",
                      exception.toString());
				    }
				    configuration.setProperty(XOozieClient.JT, "localhost:9001");
				    try {
					    wc.submitMapReduce(configuration);
					    fail("submit client without NN should throuhg exception");
				    } catch (RuntimeException exception) {
					    assertEquals(
                      "java.lang.RuntimeException: namenode is not specified in conf",
                      exception.toString());
				    }
				    configuration.setProperty(XOozieClient.NN, "hdfs://localhost:9000");
				    try {
					    wc.submitMapReduce(configuration);
					    fail("submit client without LIBPATH should throuhg exception");
				    } catch (RuntimeException exception) {
					    assertEquals(
                      "java.lang.RuntimeException: libpath is not specified in conf",
                      exception.toString());
				    }

				    File tmp=new File("target");
				    int startPosition=libPath.toString().indexOf(tmp.getAbsolutePath());
				    String localPath=libPath.toString().substring(startPosition);
				    
				    configuration.setProperty(OozieClient.LIBPATH, localPath.substring(1));
				    	
				    try{
				    	wc.submitMapReduce(configuration);
				    	fail("lib path can not be relative");
				    }catch(RuntimeException e){
				    	assertEquals("java.lang.RuntimeException: libpath should be absolute", e.toString());
				    }
				    	
				    configuration.setProperty(OozieClient.LIBPATH, libPath.toString());

				    assertEquals(MockDagEngineService.JOB_ID + wfCount
				        + MockDagEngineService.JOB_ID_END, wc.submitMapReduce(configuration));

				    assertTrue(MockDagEngineService.started.get(wfCount));
				    return null;
			    }
		    });
	}

  /**
   *  Test some simple clint's methods
   */
	public void testSomeMethods() throws Exception {
		
		runTest(END_POINTS, SERVLET_CLASSES, IS_SECURITY_ENABLED,
		    new Callable<Void>() {
			    public Void call() throws Exception {
			  		String oozieUrl = getContextURL();
			  		XOozieClient wc = new XOozieClient(oozieUrl);
			  		Properties configuration = wc.createConfiguration();
			  		try {
			  			wc.addFile(configuration, null);
			  		} catch (IllegalArgumentException e) {
			  			assertEquals("file cannot be null or empty", e.getMessage());
			  		}
			  		wc.addFile(configuration, "file1");
			  		wc.addFile(configuration, "file2");
			  		assertEquals("file1,file2", configuration.get(XOozieClient.FILES));
			  		// test archive
			  		try {
			  			wc.addArchive(configuration, null);
			  		} catch (IllegalArgumentException e) {
			  			assertEquals("file cannot be null or empty", e.getMessage());
			  		}
			  		wc.addArchive(configuration, "archive1");
			  		wc.addArchive(configuration, "archive2");
			  		assertEquals("archive1,archive2", configuration.get(XOozieClient.ARCHIVES));

				    return null;
			    }
		    });
		}
	}

