package cascading.hive;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import junitx.framework.FileAssert;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;

public class ORCFileMapTest {

    private FlowConnector flowConnector = null;

    private String txtMapFileInput = null;
    private String orcFilePath = null;

    private String txtMapFileExpected = null;

    @Before
    public void setUp() {
        flowConnector = new HadoopFlowConnector(new Properties());

        txtMapFileExpected = "src/test/resources/data/test_map_expected.txt";

        txtMapFileInput = "src/test/resources/data/test_map.txt";
        orcFilePath = "src/test/resources/data/test_map_expected.orc";
    }

    @Test
    public void testMapReadPrimitive() {
        String orcSchema = "col1 int, col2 string, col3 MAP<int,string>";
        String outputPath = "output/orc_read/";

        Lfs input = new Lfs(new ORCFile(orcSchema), orcFilePath);
        Pipe pipe = new Pipe("convert");
        Lfs output = new Lfs(new TextDelimited(true, "$"), outputPath, SinkMode.REPLACE);

        Flow flow = flowConnector.connect(input, output, pipe);
        flow.complete();

        FileAssert.assertEquals(new File(txtMapFileExpected), new File(outputPath + "part-00000"));
    }

    @Test
    public void testMapWritePrimitiveString() throws IOException {
        String[] orcHeaderFields = new String[] { "col1", "col2", "col3" };
        String[] orcHeaderDataTypes = new String[] { "int", "string", "MAP<int,string>" };

        String outputPath = "output/orc_write/";

        Lfs input = new Lfs(new TextDelimited(true, "$"), txtMapFileInput);
        Pipe pipe = new Pipe("convert");
        Lfs output = new Lfs(new ORCFile(orcHeaderFields, orcHeaderDataTypes), outputPath, SinkMode.REPLACE);

        Flow flow = flowConnector.connect(input, output, pipe);
        flow.complete();

        TupleEntryIterator actualTupleEntryIterator = output.openForRead(flow.getFlowProcess());
        TupleEntryIterator expectedTupleEntryIterator = new Lfs(new ORCFile("col1 int, col2 string, col3 MAP<int,string>"),
                orcFilePath).openForRead(flow.getFlowProcess());
        while (actualTupleEntryIterator.hasNext() && expectedTupleEntryIterator.hasNext()) {
            Tuple expected = expectedTupleEntryIterator.next().getTuple();
            Tuple actual = actualTupleEntryIterator.next().getTuple();

            assertEquals(actual.getInteger(0), expected.getInteger(0));
            assertEquals(actual.getString(1), expected.getString(1));
            assertEquals(actual.getString(2), expected.getString(2));

        }
    }

    @After
    public void tearDown() {

    }

}
