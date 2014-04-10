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
package cascading.hcatalog;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.planner.PlannerException;
import cascading.operation.Identity;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Coerce;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;
import junitx.framework.FileAssert;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HCatTapTest {
	private FlowConnector connector;
	private String hcatOut;
	private String hcatIn;
	private String hcatResultFields;
	private String resultPath;

	@Before
	public void setUp() throws Exception {
		connector = new HadoopFlowConnector(new Properties());
		hcatOut = "src/test/resources/data/hcatout.txt";
		hcatIn = "src/test/resources/data/sample_07.csv";
		hcatResultFields = "src/test/resources/data/hcat_result_fields.txt";
		resultPath = "output/";
	}

	@Test
	public void testDataIn() {
		HCatTap source = new HCatTap("sample_07");
		Lfs output = new Lfs(new TextDelimited(false, "|"), resultPath + "testDataIn",
				SinkMode.REPLACE);
        Each pipe = new Each("test", new Identity(new Fields("code", "description", "total_emp", "salary")));
		Flow flow = connector.connect(source, output, pipe);
		flow.complete();

		FileAssert.assertEquals(new File(hcatOut),
				new File(resultPath + "testDataIn/part-00000"));
	}

	@Test
	public void testDataOut() {
		Lfs input = new Lfs(new TextDelimited(new Fields("code", "description",
				"total_emp", "salary"), "|"), hcatOut);
		HCatTap output = new HCatTap("sample_08", resultPath + "testDataOut");

		Coerce pipe = new Coerce(new Pipe("test"), new Fields("total_emp"),
				Integer.class);
		pipe = new Coerce(pipe, new Fields("salary"), Integer.class);

		Flow flow = connector.connect(input, output, pipe);
		flow.complete();

		FileAssert.assertEquals(new File(resultPath + "testDataOut/part-00000"), new File(hcatIn));
	}

	@Test
	public void testDataInWithSouceFields() {
		HCatTap source = new HCatTap("sample_07", new Fields("code", "salary"));
		Lfs output = new Lfs(new TextDelimited(false, "|"), resultPath + "testDataInWithSouceFields",
				SinkMode.REPLACE);

		Flow flow = connector.connect(source, output, new Pipe("convert"));
		flow.complete();

		FileAssert.assertEquals(new File(hcatResultFields),
				new File(resultPath + "testDataInWithSouceFields/part-00000"));
	}
	
	@Test(expected = PlannerException.class)
	public void testDataInWithInvalidSouceFields() {
		HCatTap source = new HCatTap("sample_07", new Fields("a", "b"));
		Lfs output = new Lfs(new TextDelimited(false, "|"), resultPath + "testDataInWithInvalidSouceFields",
				SinkMode.REPLACE);

		Flow flow = connector.connect(source, output, new Pipe("convert"));
		flow.complete();
	}

    @Test
    public void testOrcInOut() throws IOException {
        HCatTap source = new HCatTap("test_orc");
        HCatTap output = new HCatTap("test_orc", resultPath + "testOrcInOut");
        Pipe pipe = new Pipe("testOrc");
        pipe = new Each(pipe, new Fields("col1"), new ExpressionFilter("col1 > 3", Integer.TYPE));
        Flow flow = connector.connect(source, output, pipe);
        flow.complete();

        Tuple[] expected = new Tuple[] {
                new Tuple(1, "a", "A"),
                new Tuple(1, "b", "B"),
                new Tuple(1, "c", "C"),
                new Tuple(2, "b", "B"),
                new Tuple(2, "c", "C"),
                new Tuple(2, "d", "D"),
                new Tuple(3, "c", "C")
        };

        TupleEntryIterator iterator = output.openForRead(flow.getFlowProcess());
        int i = 0;

        while (iterator.hasNext()) {
            Tuple actual = iterator.next().getTuple();
            assertEquals(expected[i++], actual);
        }
        assertTrue(i == 7);

        List<String> location = CascadingHCatUtil.getDataStorageLocation(
                MetaStoreUtils.DEFAULT_DATABASE_NAME, "test_orc", null, (JobConf) flow.getFlowProcess().getConfigCopy());
        assertEquals(location.get(0), resultPath + "testOrcInOut");
    }

    @Test
    public void testParquetIn() throws IOException {
        HCatTap source = new HCatTap("test_parquet");
        Lfs output = new Lfs(new TextDelimited(false, "|"), resultPath + "testParquetIn", SinkMode.REPLACE);
        Pipe pipe = new Pipe("testParquet");
        pipe = new Each(pipe, new Fields("col1"), new ExpressionFilter("col1 != 1", Integer.TYPE));
        Flow flow = connector.connect(source, output, pipe);
        flow.complete();

        TupleEntryIterator it = output.openForRead(flow.getFlowProcess());
        Tuple[] expected = new Tuple[] {
                new Tuple("1", "a", "A"),
                new Tuple("1", "b", "B"),
                new Tuple("1", "c", "C"),  //it's read from text file, then all values are in string
        };
        int i = 0;
        while (it.hasNext()) {
            Tuple actual = it.next().getTuple();
            assertEquals(expected[i++], actual);
        }
    }

    @Test
    public void testOrcInParquetOut() throws IOException {
        HCatTap source = new HCatTap("test_orc");
        HCatTap output = new HCatTap("test_parquet", resultPath + "testOrcInParquetOut");
        Pipe pipe = new Pipe("testParquet");
        pipe = new Each(pipe, new Fields("col1"), new ExpressionFilter("col1 != 1", Integer.TYPE));
        Flow flow = connector.connect(source, output, pipe);
        flow.complete();

        TupleEntryIterator it = output.openForRead(flow.getFlowProcess());
        Tuple[] expected = new Tuple[] {
                new Tuple(1, "a", "A"),
                new Tuple(1, "b", "B"),
                new Tuple(1, "c", "C"),
        };
        int i = 0;
        while (it.hasNext()) {
            Tuple actual = it.next().getTuple();
            assertEquals(expected[i++], actual);
        }
        assertEquals(i, 3);
    }
}
