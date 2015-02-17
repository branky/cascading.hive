/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.hive;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.CountBy;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.*;
import junitx.framework.FileAssert;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Properties;

import static org.junit.Assert.*;

/*
 */
public class ORCFileTest {

    private FlowConnector connector;

    private String orc, txt;
    
    private String wcOrc, wcTxt;


    @Before
    public void setup() {
        connector = new Hadoop2MR1FlowConnector(new Properties());
        orc = "src/test/resources/data/test.orc";
        txt = "src/test/resources/data/test.txt";
        
        wcOrc = "src/test/resources/data/wc.orc";
        wcTxt = "src/test/resources/data/wc.txt";
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
        p.put("orc.compress", "SNAPPY");
        p.put("orc.create.index", "false");
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
    
    @Test
    public void testSchemaInference() throws IOException {
        // basic read & write: no schema is specified
        //
        Lfs orcInput = new Lfs(new ORCFile(), orc);
        Lfs orcOutput = new Lfs(new ORCFile(), "output/infer/rw/", SinkMode.REPLACE);
        Lfs txtInput = new Lfs(new TextDelimited(true, ","), txt);
        TupleEntryIterator iter1 = orcInput.openForRead(new HadoopFlowProcess());
        TupleEntryIterator iter2 = txtInput.openForRead(new HadoopFlowProcess());
        TupleEntry entry1 = null;
        TupleEntry entry2 = null;
        TupleEntryCollector write = orcOutput.openForWrite(new HadoopFlowProcess());
        while (iter1.hasNext()) {
            entry1 = iter1.next();
            entry2 = iter2.next();
            assertEquals(entry1.getInteger(0), entry2.getTuple().getInteger(0));
            assertEquals(entry1.getString(1), entry2.getTuple().getString(1));
            assertEquals(entry1.getString(2), entry2.getTuple().getString(2));
            write.add(entry1.getTuple());
        }
        assertEquals(entry1.getFields().get(0), "_col0");   // schema name extracted from the input file
        assertEquals(entry1.getFields().get(1), "_col1");
        assertEquals(entry1.getFields().get(2), "_col2");
        iter1.close();
        iter2.close();
        write.close();
        
        orcInput = new Lfs(new ORCFile(), "output/infer/rw/");
        TupleEntryIterator read = orcInput.openForRead(new HadoopFlowProcess());
        TupleEntry entry = read.next();
        assertTrue(entry.getObject(0) instanceof Integer);
        assertEquals(entry.getFields().get(1), ORCFile.DEFAULT_COL_PREFIX + "1");   // default column name
        assertTrue(entry.getObject(1) instanceof String);
        read.close();
        
        // test types
        //
        orcOutput = new Lfs(new ORCFile(), "output/infer/type/", SinkMode.REPLACE);
        write = orcOutput.openForWrite(new HadoopFlowProcess());
        Tuple tuple = new Tuple(1024, false, (byte) 16, (short) 128, 65536L, 1.23f, 3.14d, new BigDecimal(870721), "orcfield");
        write.add(tuple);
        write.close();
        orcInput = new Lfs(new ORCFile(), "output/infer/type/");
        read = orcInput.openForRead(new HadoopFlowProcess());
        entry = read.next();
        assertEquals(entry.getObject(0), new Integer(1024));
        assertEquals(entry.getObject(6), new Double(3.14));
        assertEquals(entry.getObject(7), new BigDecimal(870721));
        assertEquals(entry.getObject(8), "orcfield");
        read.close();
        
        // test projection
        //
        orcInput = new Lfs(new ORCFile(null, "0,2"), orc);
        txtInput = new Lfs(new TextDelimited(true, ","), txt);
        iter1 = orcInput.openForRead(new HadoopFlowProcess());
        iter2 = txtInput.openForRead(new HadoopFlowProcess());
        while (iter1.hasNext()) {
            entry1 = iter1.next();
            entry2 = iter2.next();
            assertEquals(entry1.getInteger(0), entry2.getTuple().getInteger(0));
            assertNull(entry1.getObject(1));    // null for the pruned column
            assertEquals(entry1.getString(2), entry2.getTuple().getString(2));
        }
        iter1.close();
        iter2.close();
        
        // input path does not exist
        //
        orcInput = new Lfs(new ORCFile(), "inputpathdoesnotexist/");
        try {
            iter1 = orcInput.openForRead(new HadoopFlowProcess());
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("inputpathdoesnotexist"));
        }
    }
    
    @Test
    public void testSchemaInferenceIt() throws IOException {
        // wordcount: integrated test with flow controller
        //

        // create source and sink taps
        Tap docTap = new Hfs( new ORCFile(null, "1"), wcOrc );  // field: text
        Tap wcTap = new Hfs( new ORCFile(), "output/infer/it/", SinkMode.REPLACE);

        // specify a regex operation to split the "document" text lines into a token stream
        Fields token = new Fields( "token" );
        Fields text = new Fields( "text" );
        RegexSplitGenerator splitter = new RegexSplitGenerator( token, "[ \\[\\]\\(\\),.]" );
        // only returns "token"
        Pipe docPipe = new Each( "token", text, splitter, Fields.RESULTS );

        // determine the word counts
        Pipe wcPipe = new Pipe( "wc", docPipe );
        wcPipe = new GroupBy( wcPipe, token );
        wcPipe = new Every( wcPipe, Fields.ALL, new Count(), Fields.ALL );

        // connect the taps, pipes, etc., into a flow
        FlowDef flowDef = FlowDef.flowDef()
         .setName( "wc" )
         .addSource( docPipe, docTap )
         .addTailSink( wcPipe, wcTap );

        // write a DOT file and run the flow
        Flow wcFlow = connector.connect( flowDef );
        wcFlow.complete();
        
        Tap resultTap = new Hfs(new ORCFile(), "output/infer/it/");
        TupleEntryIterator read = resultTap.openForRead(new HadoopFlowProcess());
        TupleEntry entry = read.next();
        assertEquals(entry.getFields().get(0), "token");    // inferred from the planner
        assertEquals(entry.getFields().get(1), "count");
        assertEquals(entry.getObject(0), "");
        assertEquals(entry.getObject(1), 9L);
        
        entry = read.next();
        assertEquals(entry.getObject(0), "A");
        assertEquals(entry.getObject(1), 3L);
        entry = read.next();
        assertEquals(entry.getObject(0), "Australia");
        assertEquals(entry.getObject(1), 1L);
        
        read.close();
    }
    
    
    
    
}
