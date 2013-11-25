package cascading.hive;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;
import junitx.framework.FileAssert;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/*
 */
public class RCFileTest {

    private FlowConnector connector;

    private String rc, txt;


    @Before
    public void setup() {
        connector = new HadoopFlowConnector(new Properties());
        rc = "src/test/resources/data/test.rc";
        txt = "src/test/resources/data/test.txt";
    }

    @AfterClass
    public static void tearDown() throws IOException
    {
        FileUtils.deleteDirectory(new File("output"));
    }

    @Test
    public void testRead() throws Exception {
        Lfs input = new Lfs(new RCFile("col1 int, col2 string, col3 string"), rc);
        Pipe pipe = new Pipe("convert");
        Lfs output = new Lfs(new TextDelimited(true, ","), "output/rc_read/", SinkMode.REPLACE);
        Flow flow = connector.connect(input, output, pipe);
        flow.complete();
        FileAssert.assertEquals(new File(txt), new File("output/rc_read/part-00000"));
    }

    @Test
    public void testWrite() throws Exception {
        Lfs input = new Lfs(new TextDelimited(true, ","), txt);
        Pipe pipe = new Pipe("convert");
        Lfs output = new Lfs(new RCFile(new String[]{"col1", "col2", "col3"},
                new String[] {"int","string","string"}), "output/rc_write/", SinkMode.REPLACE);
        Flow flow = connector.connect(input, output, pipe);
        flow.complete();

        TupleEntryIterator it1 = output.openForRead(flow.getFlowProcess());
        TupleEntryIterator it2 = new Lfs(new RCFile("col1 int, col2 string, col3 string"), rc).openForRead(flow.getFlowProcess());
        while(it1.hasNext() && it2.hasNext()) {
            Tuple actual = it1.next().getTuple();
            Tuple expected = it2.next().getTuple();
            assertEquals(expected.getInteger(0), actual.getInteger(0));
            assertEquals(expected.getString(1), actual.getString(1));
            assertEquals(expected.getString(2), actual.getString(2));
        }
    }
}
