import org.apache.hadoop.fs.Path;
import org.junit.Test;


public class TestPath {
  @Test
  public void testPath()
  {
    Path p =new Path("file://user/oozie/share/lib");
    System.out.println(p.toUri().getAuthority());
  }
}
