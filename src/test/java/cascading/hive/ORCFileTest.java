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
import cascading.flow.hadoop.HadoopFlowConnector;
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
        connector = new HadoopFlowConnector(new Properties());
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
        connector = new HadoopFlowConnector(p);

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
