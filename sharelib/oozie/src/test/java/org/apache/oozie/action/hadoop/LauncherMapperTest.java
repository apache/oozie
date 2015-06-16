package org.apache.oozie.action.hadoop;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.*;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ FileSystem.class, OutputStreamWriter.class, Path.class, LauncherMapper.class })
@SuppressWarnings({ "rawtypes", "deprecation" })
public class LauncherMapperTest {
    public static final String MAPRED_JOB_ID_KEY = "mapred.job.id";

    private String actionDirPath = "dir_path";
    private String actionRecoveryId = "recovery_id";
    private String mapredJobId = "job_id";

    private JobConf setupConf(String actionDirPath, String actionRecoveryId, String mapredJobId) {
        JobConf jobConf = new JobConf();

        jobConf.set(LauncherMapper.OOZIE_ACTION_DIR_PATH, actionDirPath);
        jobConf.set(LauncherMapper.OOZIE_ACTION_RECOVERY_ID, actionRecoveryId);
        jobConf.set(MAPRED_JOB_ID_KEY, mapredJobId);

        return jobConf;
    }

    private Path mockRecoveryId(String actionDirPath, String actionRecoveryId) throws Exception {
        Path actionDir = new Path(actionDirPath);
        Path recoveryId = new Path(actionDir, actionRecoveryId);

        whenNew(Path.class).withArguments(actionDirPath).thenReturn(actionDir);
        whenNew(Path.class).withArguments(actionDir, actionRecoveryId).thenReturn(recoveryId);

        return recoveryId;
    }

    private FileSystem mockFileSystem(Path recoveryId, JobConf jobConf) throws IOException {
        FileSystem fs = mock(FileSystem.class);

        mockStatic(FileSystem.class);
        when(FileSystem.get(recoveryId.toUri(), jobConf)).thenReturn(fs);

        return fs;
    }

    private OutputStreamWriter mockWriter(FileSystem fs, Path recoveryId) throws Exception {
        FSDataOutputStream fsdos = new FSDataOutputStream(new ByteArrayOutputStream());
        OutputStreamWriter writerMock = spy(new OutputStreamWriter(fsdos));

        when(fs.create(recoveryId)).thenReturn(fsdos);
        whenNew(OutputStreamWriter.class).withArguments(fsdos).thenReturn(writerMock);

        return writerMock;
    }

    private InputStreamReader mockInput(FileSystem fs, Path recoveryId, byte[] bytes) throws Exception {
        when(fs.open(recoveryId)).thenReturn(null);
        InputStreamReader ios = new InputStreamReader(new ByteArrayInputStream(bytes));
        whenNew(InputStreamReader.class).withArguments(null).thenReturn(ios);

        return ios;
    }

    @Test
    public void testRecoveryIdDoesntExist() throws Exception {
        JobConf jobConf = setupConf(actionDirPath, actionRecoveryId, mapredJobId);
        Path recoveryId = mockRecoveryId(actionDirPath, actionRecoveryId);
        FileSystem fs = mockFileSystem(recoveryId, jobConf);

        when(fs.exists(recoveryId)).thenReturn(false);

        OutputStreamWriter writerMock = mockWriter(fs, recoveryId);

        LauncherMapper mapper = PowerMockito.spy(new LauncherMapper());
        mapper.configure(jobConf);

        verify(writerMock, times(1)).write(mapredJobId);
    }

    @Test
    public void testRecoveryIdExistsButNull() throws Exception {
        JobConf jobConf = setupConf(actionDirPath, actionRecoveryId, mapredJobId);
        Path recoveryId = mockRecoveryId(actionDirPath, actionRecoveryId);
        FileSystem fs = mockFileSystem(recoveryId, jobConf);

        when(fs.exists(recoveryId)).thenReturn(true);

        OutputStreamWriter writerMock = mockWriter(fs, recoveryId);
        mockInput(fs, recoveryId, new byte[] {});

        LauncherMapper mapper = PowerMockito.spy(new LauncherMapper());
        PowerMockito.doNothing().when(mapper, "failLauncher", anyInt(), anyString(), any(Throwable.class));

        mapper.configure(jobConf);

        verify(writerMock, times(1)).write(mapredJobId);
        verifyPrivate(mapper, times(0)).invoke("failLauncher", anyInt(), anyString(), any(Throwable.class));
    }

    @Test
    public void testRecoveryIdExistsAndEqualToJobId() throws Exception {
        JobConf jobConf = setupConf(actionDirPath, actionRecoveryId, mapredJobId);
        Path recoveryId = mockRecoveryId(actionDirPath, actionRecoveryId);
        FileSystem fs = mockFileSystem(recoveryId, jobConf);

        when(fs.exists(recoveryId)).thenReturn(true);

        OutputStreamWriter writerMock = mockWriter(fs, recoveryId);
        mockInput(fs, recoveryId, mapredJobId.getBytes());

        LauncherMapper mapper = PowerMockito.spy(new LauncherMapper());
        PowerMockito.doNothing().when(mapper, "failLauncher", anyInt(), anyString(), any(Throwable.class));

        mapper.configure(jobConf);

        verify(writerMock, times(0)).write(mapredJobId);
        verifyPrivate(mapper, times(0)).invoke("failLauncher", anyInt(), anyString(), any(Throwable.class));
    }

    @Test
    public void testRecoveryIdExistsAndNotEqualToJobId() throws Exception {
        JobConf jobConf = setupConf(actionDirPath, actionRecoveryId, mapredJobId);
        Path recoveryId = mockRecoveryId(actionDirPath, actionRecoveryId);
        FileSystem fs = mockFileSystem(recoveryId, jobConf);

        when(fs.exists(recoveryId)).thenReturn(true);

        OutputStreamWriter writerMock = mockWriter(fs, recoveryId);
        mockInput(fs, recoveryId, "something_other".getBytes());

        LauncherMapper mapper = PowerMockito.spy(new LauncherMapper());
        PowerMockito.doNothing().when(mapper, "failLauncher", anyInt(), anyString(), any(Throwable.class));

        mapper.configure(jobConf);

        verify(writerMock, times(0)).write(mapredJobId);
        verifyPrivate(mapper, times(1)).invoke("failLauncher", anyInt(), anyString(), any(Throwable.class));
    }

}