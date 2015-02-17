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
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.functions.ConvertToHiveJavaType;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.CountBy;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

public class RCFileTest {
  private static final String RC_FILE = "src/test/resources/data/rc_test.rc";
  private static final String TXT_FILE = "src/test/resources/data/rc_test.txt";
  private static final String OUTPUT_DIR = "output";

  private static final String[] COLUMNS = {"intCol", "bigintCol", "floatCol", "doubleCol",
      "decimalCol", "booleanCol", "binaryCol", "stringCol", "timestampCol"};
  private static final String[] TYPES = {"int", "bigint", "float", "double", "decimal", "boolean",
      "binary", "string", "timestamp"};
  private static final String HIVE_SCHEMA;
  static
  {
    List<String> schemaParts = new ArrayList<String>(COLUMNS.length);
    for (int i = 0; i < COLUMNS.length; i++) {
      schemaParts.add(COLUMNS[i] + " " + TYPES[i]);
    }
    HIVE_SCHEMA = Joiner.on(", ").join(schemaParts);
  }

  private FlowConnector connector;

  @Before
  public void setup() {
    Preconditions.checkArgument(COLUMNS.length == TYPES.length);

    connector = new Hadoop2MR1FlowConnector(new Properties());
  }

  @AfterClass
  public static void tearDown() throws IOException
  {
    // Comment out the below line if you want to inspect the output of these tests.
    FileUtils.deleteDirectory(new File("output"));
  }

  @Test
  public void testRCFileScheme() throws Exception {
    Fields fields = new Fields(COLUMNS);

    Lfs input = new Lfs(new TextDelimited(fields, true, ","), TXT_FILE);
    Pipe pipe = new Pipe("convert");
    pipe = new Each(pipe, new ConvertToHiveJavaType(fields, TYPES));
    // Writing to an RCFile scheme.
    Lfs output = new Lfs(new RCFile(COLUMNS, TYPES), OUTPUT_DIR + "/rc_test", SinkMode.REPLACE);
    Flow flow = connector.connect(input, output, pipe);
    flow.complete();

    // Reading from an RCFile scheme.
    TupleEntryIterator it1 = new Lfs(new RCFile(HIVE_SCHEMA), RC_FILE).openForRead(
        flow.getFlowProcess());
    TupleEntryIterator it2 = output.openForRead(flow.getFlowProcess());
    while (it1.hasNext() && it2.hasNext()) {
      Tuple expected = it1.next().getTuple();
      Tuple actual = it2.next().getTuple();
      assertTupleEquals(expected, actual);
    }
    assertTrue(!it1.hasNext() && !it2.hasNext()); // Make sure iterators reach their ends.
  }

  @Test
  public void testCompress() throws IOException {
    Properties p = new Properties();
    p.put("mapred.output.compress", "true");
    p.put("mapred.output.compression.type", "BLOCK");
    // GzipCodec needs native lib, otherwise the output can be read.
    // p.put("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
    connector = new Hadoop2MR1FlowConnector(p);

    Fields fields = new Fields(COLUMNS);

    Lfs input = new Lfs(new TextDelimited(fields, true, ","), TXT_FILE);
    Pipe pipe = new Pipe("convert");
    pipe = new Each(pipe, new ConvertToHiveJavaType(fields, TYPES));
    Lfs output = new Lfs(new RCFile(COLUMNS, TYPES), OUTPUT_DIR + "/rc_compress", SinkMode.REPLACE);
    Flow flow = connector.connect(input, output, pipe);
    flow.complete();

    // Compare results with uncompressed ones.
    TupleEntryIterator it1 = new Lfs(new RCFile(HIVE_SCHEMA), RC_FILE).openForRead(
        flow.getFlowProcess());
    TupleEntryIterator it2 = output.openForRead(flow.getFlowProcess());
    while (it1.hasNext() && it2.hasNext()) {
      Tuple expected = it1.next().getTuple();
      Tuple actual = it2.next().getTuple();
      assertTupleEquals(expected, actual);
    }
    assertTrue(!it1.hasNext() && !it2.hasNext());
  }

  @Test
  public void testCountBy() throws IOException {
    Lfs input = new Lfs(new RCFile(HIVE_SCHEMA, "0"), RC_FILE);
    Pipe pipe = new Pipe("testCountBy");
    pipe = new CountBy(pipe, new Fields(COLUMNS[0]), new Fields("cnt"));
    Lfs output = new Lfs(new TextDelimited(true, ","), OUTPUT_DIR + "/rc_count", SinkMode.REPLACE);
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
    assertEquals(expected.length, i);
  }

  /**
   * Assert if two {@link Tuple}s are equal. We cannot use the {@link Tuple#equals(Object)} method
   * directly since the tuple can contain arrays, and arrays are not compared by their content with
   * their {@link Object#equals(Object)} method.
   */
  private static void assertTupleEquals(Tuple expected, Tuple actual) {
    assertArrayEquals(toArray(expected), toArray(actual));
  }

  /**
   * Convert a {@link Tuple} to an array.
   */
  private static Object[] toArray(Tuple tuple) {
    Object[] result = new Object[tuple.size()];
    for (int i = 0 ; i < tuple.size(); i++) {
      result[i] = tuple.getObject(i);
    }
    return result;
  }
}
