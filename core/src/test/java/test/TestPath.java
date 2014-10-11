package test;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.junit.Test;

public class TestPath {
	@Test
	public void testPath() {
		Path p = new Path("file://user/oozie/share/lib");
		System.out.println(p.toUri().getAuthority());
	}

//	String[] cmd = {"bash", "-c", "id -Gn oozie" };
	String[] cmd = {"bash","-c","sudo -u bdws ./test.sh" };

	@Test
	public void testCmd() {
		try{
		ShellCommandExecutor shellCommandExecutor = new ShellCommandExecutor(
				cmd);
		shellCommandExecutor.execute();
		System.out.println(shellCommandExecutor.getOutput());
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
