package cascading.hcatalog;

import java.io.File;
import java.util.Properties;

import junitx.framework.FileAssert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.flow.planner.PlannerException;
import cascading.hcatalog.HCatTap;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Coerce;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;

public class HCatTapTest {
	private FlowConnector connector;
	private String hcatOut;
	private String hcatIn;
	private String hcatResultFields;
	private String resultPath;

	@Before
	public void setUp() throws Exception {
		connector = new Hadoop2MR1FlowConnector(new Properties());
		hcatOut = "src/test/resources/data/hcatout.txt";
		hcatIn = "src/test/resources/data/sample_07.csv";
		hcatResultFields = "src/test/resources/data/hcat_result_fields.txt";
		resultPath = "output/part-00000";
	}

	@After
	public void tearDown() throws Exception {
		connector = null;
		hcatOut = null;
		hcatIn = null;
		hcatResultFields = null;
		resultPath = null;
	}

	@Test
	public void testDataIn() {
		HCatTap source = new HCatTap("sample_07");
		Lfs output = new Lfs(new TextDelimited(false, "|"), "output/",
				SinkMode.REPLACE);

		Flow flow = connector.connect(source, output, new Pipe("convert"));
		flow.complete();

		FileAssert.assertEquals(new File(hcatOut),
				new File(resultPath));
	}

	@Test
	public void testDataOut() {
		Lfs input = new Lfs(new TextDelimited(new Fields("code", "description",
				"total_emp", "salary"), "|"), hcatOut);
		HCatTap output = new HCatTap("sample_08", "output");

		Coerce pipe = new Coerce(new Pipe("test"), new Fields("total_emp"),
				Integer.class);
		pipe = new Coerce(pipe, new Fields("salary"), Integer.class);

		Flow flow = connector.connect(input, output, pipe);
		flow.complete();

		FileAssert
				.assertEquals(new File(resultPath), new File(hcatIn));
	}

	@Test
	public void testDataInWithSouceFields() {
		HCatTap source = new HCatTap("sample_07", new Fields("code", "salary"));
		Lfs output = new Lfs(new TextDelimited(false, "|"), "output/",
				SinkMode.REPLACE);

		Flow flow = connector.connect(source, output, new Pipe("convert"));
		flow.complete();

		FileAssert.assertEquals(new File(hcatResultFields),
				new File(resultPath));
	}
	
	@Test(expected = PlannerException.class)
	public void testDataInWithInvalidSouceFields() {
		HCatTap source = new HCatTap("sample_07", new Fields("a", "b"));
		Lfs output = new Lfs(new TextDelimited(false, "|"), "output/",
				SinkMode.REPLACE);

		Flow flow = connector.connect(source, output, new Pipe("convert"));
		flow.complete();
	}
}
