/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */

package org.apache.oozie.service;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.oozie.util.XLog;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class MaprJobClient extends JobClient {

    // Thread pool for jobClient API callls
    private static ExecutorService threadExecutor =
        Executors.newFixedThreadPool(20);

    private final int timeOut = 60 * 1 * 1000;  // 1 min

    public MaprJobClient() {
        super();
    }

    public MaprJobClient(JobConf conf) throws IOException {
        super(conf);
    }

    public void init(JobConf conf) throws IOException {
        super.init(conf);
    }

    public static void shutdown() {
        threadExecutor.shutdown();
    }

    public RunningJob getJob(final JobID jobid) throws IOException {

        Callable <RunningJob> callTask = new Callable<RunningJob>() {
            public RunningJob call() throws Exception {
                return MaprJobClient.super.getJob(jobid);
            }
        };

        List<Callable<RunningJob>> callableList =
            new ArrayList<Callable<RunningJob>>();
        callableList.add(callTask);

        RunningJob runJob = null;
        try {
            XLog.getLog(getClass()).debug("Making jobClient call");
            runJob = threadExecutor.invokeAny(callableList, timeOut,
                TimeUnit.MILLISECONDS);
            XLog.getLog(getClass()).debug("jobClient call is successful");
        } catch (InterruptedException e) {
            XLog.getLog(getClass()).debug("JobClient call got InterruptedExeption");
        } catch (ExecutionException e) {
            XLog.getLog(getClass()).debug("JobClient call got ExecutionException");
        } catch (TimeoutException e) {
            XLog.getLog(getClass()).debug("JobClient call got TimeoutException");
        } catch (Exception e) {
            if (e instanceof IOException)
                throw (IOException) e;
        }

        return runJob;
    }

    public void close() throws IOException {
        Callable <Void> callTask = new Callable<Void>() {
            public Void call() throws Exception {
                MaprJobClient.super.close();
                return null;
            }
        };

        List<Callable<Void>> callableList =
            new ArrayList<Callable<Void>>();
        callableList.add(callTask);

        try {
            XLog.getLog(getClass()).debug("Making jobClient call");
            threadExecutor.invokeAny(callableList, timeOut,
                TimeUnit.MILLISECONDS);
            XLog.getLog(getClass()).debug("jobClient call is successful");
        } catch (InterruptedException e) {
            XLog.getLog(getClass()).debug("JobClient call got InterruptedExeption");
        } catch (ExecutionException e) {
            XLog.getLog(getClass()).debug("JobClient call got ExecutionException");
        } catch (TimeoutException e) {
            XLog.getLog(getClass()).debug("JobClient call got TimeoutException");
        } catch (Exception e) {
            if (e instanceof IOException)
                throw (IOException)e;
        }
    }

    String getSubmitjobClasspath() {
        File dir = new File(
            System.getProperty("oozie.home.dir") +
                "/oozie-server/webapps/oozie/WEB-INF/lib/");
        if (! dir.isDirectory()) {
            return null;
        }
        FilenameFilter filter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith("jar");
            }
        };

        String[] files = dir.list(filter);
        String cp = "" ;
        for (int i = 0; i < files.length; ++i) {
            cp = cp + ":" + dir.getAbsolutePath() + "/" + files[i];
        }

        return cp;
    }

    RunningJob submitJobDirectly(final JobConf job) throws
        FileNotFoundException,
        IOException {
        Callable <RunningJob> callTask = new Callable<RunningJob>() {
            public RunningJob call() throws Exception {
                return MaprJobClient.super.submitJob(job);
            }
        };
        List<Callable<RunningJob>> callableList =
            new ArrayList<Callable<RunningJob>>();
        callableList.add(callTask);

        RunningJob runJob = null;
        try {
            XLog.getLog(getClass()).debug("Making direct jobSubmit call");
            runJob = threadExecutor.invokeAny(callableList, timeOut,
                TimeUnit.MILLISECONDS);
            XLog.getLog(getClass()).debug("jobClient call is successful");
        } catch (InterruptedException e) {
            XLog.getLog(getClass()).debug("JobClient call got InterruptedExeption");
        } catch (ExecutionException e) {
            XLog.getLog(getClass()).debug("JobClient call got ExecutionException");
        } catch (TimeoutException e) {
            XLog.getLog(getClass()).debug("JobClient call got TimeoutException");
        } catch (Exception e) {
            if (e instanceof FileNotFoundException)
                throw (FileNotFoundException) e;
            if (e instanceof IOException)
                throw (IOException) e;
        }

        return runJob;
    }

    public RunningJob submitJob(final JobConf job) throws FileNotFoundException, IOException {

        if (System.getProperty("user.name").compareTo(job.getUser()) == 0) {
            //if oozie server is running as job user, directly call the jobSubmit
            //directly.
            return submitJobDirectly(job);
        }

        // Submit the job from another process with owner set to job user.
        // Run task-controller binary to run 'submitJob' as job user.

        // 1) Serialize jobConf object.
        String oozieTmpDir = System.getProperty("oozie.data.dir");

    /* Throw exception if oozie.data.dir is not set */
        if (oozieTmpDir == null || oozieTmpDir.equalsIgnoreCase(""))
        {
            XLog.getLog(getClass()).error("oozie.data.dir property not set, set oozie.data.dir to valid directory");
            throw new IOException("Unable to create JobIdFile because oozie.data.dir is not set");
        }

        File jobConfFile =
            new File(oozieTmpDir +"/"+ job.get("oozie.job.id") + "-jobconf.dat");
        File jobIdFile =
            new File(oozieTmpDir +"/"+ job.get("oozie.job.id") + "-jobid.dat");
        JobID newJobId;

        try {

            String suexecExe = System.getProperty("oozie.home.dir") +
                "/../../server/oozieexecute";
            File suexecFile = new File(suexecExe);
            if (!suexecFile.exists()) {
                //Missing suexec binary. Can not impersonate an user.
                XLog.getLog(getClass()).warn("Missing file: " + suexecExe);
                throw new IOException("Can not impersonate different user");
            }

            //Change the owner of action directory to get perms to create
            //'output' directory.


            jobIdFile.createNewFile();
            jobIdFile.setWritable(true, false);

            DataOutputStream outputFile = new DataOutputStream((OutputStream)new FileOutputStream(jobConfFile));
            job.write((DataOutput)outputFile);
            outputFile.flush();
            outputFile.close();

            // 2) Run the task-controller to submit job in the timeout frame work.

            String[] commandArray;

            List<String> command = new ArrayList<String>();
            command.add(suexecExe);
            command.add(job.getUser());
            command.add(job.get("oozie.action.dir.path"));
            command.add(jobConfFile.toString());
            command.add(jobIdFile.toString());

            command.add("-classpath");
            command.add(getSubmitjobClasspath());

            String libraryPath = System.getProperty("java.library.path");
            command.add("-Djava.library.path=" + libraryPath);

            commandArray = command.toArray(new String[0]);
            XLog.getLog(getClass()).debug("sudo cmd: " + command.toString());
            final ShellCommandExecutor shExec = new ShellCommandExecutor(commandArray,
                null, null);

            Callable <Void> callTask = new Callable<Void>() {
                public Void call() throws Exception {
                    shExec.execute();
                    return null;
                }
            };

            List<Callable<Void>> callableList =
                new ArrayList<Callable<Void>>();
            callableList.add(callTask);

            try {
                XLog.getLog(getClass()).debug("Making jobClient call");
                threadExecutor.invokeAny(callableList, timeOut,
                    TimeUnit.MILLISECONDS);
                XLog.getLog(getClass()).debug("jobClient call is successful");
            } catch (InterruptedException e) {
                XLog.getLog(getClass()).debug("JobClient call got InterruptedExeption");
            } catch (ExecutionException e) {
                XLog.getLog(getClass()).debug("JobClient call got ExecutionException");
            } catch (TimeoutException e) {
                XLog.getLog(getClass()).debug("JobClient call got TimeoutException");
            } catch (Exception e) {
                if (e instanceof FileNotFoundException)
                    throw (FileNotFoundException) e;
                if (e instanceof IOException)
                    throw (IOException) e;
                if (e instanceof ExitCodeException) {
                    int exitCode = shExec.getExitCode();
                    if (exitCode != 1) {
                        XLog.getLog(getClass()).debug(
                            "Exit code from taskcontroller : " + exitCode);
                        XLog.getLog(getClass()).debug(shExec.getOutput());
                        throw new IOException("Task controller setup failed because of " +
                            "invalid permissions/ownership with exit code " + exitCode, e);
                    }
                }
            }
            XLog.getLog(getClass()).debug("MaprSubmitJob output: " + shExec.getOutput());
            // 3) Deserialize jobId from the file which is output of step2.
            DataInputStream inputFile = new DataInputStream((InputStream)new FileInputStream(jobIdFile));
            newJobId = new JobID();
            newJobId.readFields((DataInput)inputFile);
        } finally {
            // 4) Cleanup the temp files.
            try {
                jobConfFile.delete();
                jobIdFile.delete();
            } catch (Exception e) {
                XLog.getLog(getClass()).debug("MaprSubmitJob cleanup got exception: " +
                    e);
            }
        }

        // 5) Get the running job using jobId and return.
        return getJob(newJobId);

    }

  /*
   * Not required now
    public Token<DelegationTokenIdentifier>
    getDelegationToken(Text renewer) throws IOException, InterruptedException
  */
}