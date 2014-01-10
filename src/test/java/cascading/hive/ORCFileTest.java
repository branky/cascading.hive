package cascading.hive;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.CountBy;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;
import junitx.framework.FileAssert;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/*
 */
public class ORCFileTest {

    private FlowConnector connector;

    private String orc, txt;


    @Before
    public void setup() {
        connector = new Hadoop2MR1FlowConnector(new Properties());
        orc = "src/test/resources/data/test.orc";
        txt = "src/test/resources/data/test.txt";
    }

    @AfterClass
    public static void tearDown() throws IOException
    {
     //   FileUtils.deleteDirectory(new File("output"));
    }

    @Test
    public void testRead() throws Exception {
        Lfs input = new Lfs(new ORCFile("col1 int, col2 string, col3 string"), orc);
        Pipe pipe = new Pipe("convert");
        Lfs output = new Lfs(new TextDelimited(true, ","), "output/orc_read/", SinkMode.REPLACE);
        Flow flow = connector.connect(input, output, pipe);
        flow.complete();
        FileAssert.assertEquals(new File(txt), new File("output/orc_read/part-00000"));
    }


    @Test
    public void test() throws Exception {
        Lfs input = new Lfs(new TextDelimited(new Fields("cal_date",
                "day_of_week",
                "week_in_year_id",
                "week_beg_date",
                "week_ind",
                "week_end_date",
                "prd_id",
                "year_id",
                "prd_ind",
                "prd_flag",
                "week_num_desc",
                "prd_desc",
                "qtr_id",
                "qtr_ind",
                "qtr_desc",
                "cre_date",
                "year_ind",
                "upd_date",
                "cre_user",
                "upd_user",
                "week_beg_end_desc_mdy",
                "week_beg_end_desc_md",
                "retail_week",
                "retail_year",
                "retail_start_date",
                "retail_wk_end_date"),
                false, "\t"), "/Users/bishao/git/plumbum/output/dw_calendar");

        Pipe pipe = new Pipe("convert");
        Lfs output = new Lfs(new ORCFile("cal_date STRING,\n" +
                "        day_of_week STRING,\n" +
                "        week_in_year_id STRING,\n" +
                "        week_beg_date STRING,\n" +
                "        week_ind STRING,\n" +
                "        week_end_date STRING,\n" +
                "        prd_id STRING,\n" +
                "        year_id STRING,\n" +
                "        prd_ind STRING,\n" +
                "        prd_flag STRING,\n" +
                "        week_num_desc STRING,\n" +
                "        prd_desc STRING,\n" +
                "        qtr_id STRING,\n" +
                "        qtr_ind STRING,\n" +
                "        qtr_desc STRING,\n" +
                "        cre_date STRING,\n" +
                "        year_ind STRING,\n" +
                "        upd_date STRING,\n" +
                "        cre_user STRING,\n" +
                "        upd_user STRING,\n" +
                "        week_beg_end_desc_mdy STRING,\n" +
                "        week_beg_end_desc_md STRING,\n" +
                "        retail_week BIGINT,\n" +
                "        retail_year BIGINT,\n" +
                "        retail_start_date STRING,\n" +
                "        retail_wk_end_date STRING"), "output/orc_test", SinkMode.REPLACE);
        Flow flow = connector.connect(input, output, pipe);
        flow.complete();

        Lfs output2 = new Lfs(new TextDelimited(), "output/orc_test2", SinkMode.REPLACE);
        Flow flow2 = connector.connect(output, output2, pipe);
        flow2.complete();

        FileAssert.assertEquals(new File("/Users/bishao/git/plumbum/output/dw_calendar/part-00000"), new File("output/orc_test2/part-00000"));
    }

    @Test
    public void testWrite() throws Exception {
        Lfs input = new Lfs(new TextDelimited(true, ","), txt);
        Pipe pipe = new Pipe("convert");
        Lfs output = new Lfs(new ORCFile(new String[]{"col1", "col2", "col3"},
                new String[] {"int","string","string"}), "output/orc_write/", SinkMode.REPLACE);
        Flow flow = connector.connect(input, output, pipe);
        flow.complete();

        TupleEntryIterator it1 = output.openForRead(flow.getFlowProcess());
        TupleEntryIterator it2 = new Lfs(new ORCFile("col1 int, col2 string, col3 string"), orc).openForRead(flow.getFlowProcess());
        while(it1.hasNext() && it2.hasNext()) {
            Tuple actual = it1.next().getTuple();
            Tuple expected = it2.next().getTuple();
            assertEquals(expected.getInteger(0), actual.getInteger(0));
            assertEquals(expected.getString(1), actual.getString(1));
            assertEquals(expected.getString(2), actual.getString(2));
        }
    }

    @Test
    public void testCompress() throws IOException {
        Properties p = new Properties();
        p.put("mapred.output.compress", "true");
        p.put("mapred.output.compression.type", "BLOCK");
//        GzipCodec needs native lib, otherwise the output can be read.
//        p.put("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        connector = new Hadoop2MR1FlowConnector(p);

        Lfs input = new Lfs(new TextDelimited(true, ","), txt);
        Pipe pipe = new Pipe("convert");
        Lfs output = new Lfs(new ORCFile(new String[]{"col1", "col2", "col3"},
                new String[] {"int","string","string"}), "output/orc_compress/", SinkMode.REPLACE);
        Flow flow = connector.connect(input, output, pipe);
        flow.complete();
        //compare result with uncompressed ones
        TupleEntryIterator it1 = output.openForRead(flow.getFlowProcess());
        TupleEntryIterator it2 = new Lfs(new ORCFile("col1 int, col2 string, col3 string"), orc).openForRead(flow.getFlowProcess());
        while(it1.hasNext() && it2.hasNext()) {
            Tuple actual = it1.next().getTuple();
            Tuple expected = it2.next().getTuple();
            assertEquals(expected.getInteger(0), actual.getInteger(0));
            assertEquals(expected.getString(1), actual.getString(1));
            assertEquals(expected.getString(2), actual.getString(2));
        }
    }
    @Test
    public void testProjection() throws IOException {
        Lfs input = new Lfs(new ORCFile("col1 int, col2 string, col3 string", "0"), orc);
        Pipe pipe = new Pipe("testProjection");
        pipe = new CountBy(pipe, new Fields("col1"), new Fields("cnt"));
        Lfs output = new Lfs(new TextDelimited(true, ","), "output/orc_count/", SinkMode.REPLACE);
        Flow flow = connector.connect(input, output, pipe);
        flow.complete();
        TupleEntryIterator it = output.openForRead(flow.getFlowProcess());
        Tuple[] expected = new Tuple[] {
                new Tuple("1", "3"),
                new Tuple("2", "3"),
                new Tuple("3", "1"),
                new Tuple("4", "3"),
                new Tuple("5", "3")
        };
        int i = 0;
        while (it.hasNext()) {
            Tuple actual = it.next().getTuple();
            assertEquals(expected[i++], actual);
        }
    }
}
