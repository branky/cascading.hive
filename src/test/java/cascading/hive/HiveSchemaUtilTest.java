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

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

/**
 */
public class HiveSchemaUtilTest {
  @Test
  public void testParsePrimitive() {
    String schema = "id INT, name STRING, price DOUBLE, \n description STRING";
    List<String>[] lists = HiveSchemaUtil.parse(schema);
    List<String> nameList = lists[0];
    List<String> typeList = lists[1];

    List<String> expectedNames = new ArrayList<String>(4);
    expectedNames.add("id");
    expectedNames.add("name");
    expectedNames.add("price");
    expectedNames.add("description");
    List<String> expectedTypes = new ArrayList<String>(4);
    expectedTypes.add("int");
    expectedTypes.add("string");
    expectedTypes.add("double");
    expectedTypes.add("string");

    assertEquals(expectedNames, nameList);
    assertEquals(expectedTypes, typeList);
  }

  @Test
  public void testParsePrimitiveLower() {
    String schema = "col1 int, col2 string";
    List<String>[] lists = HiveSchemaUtil.parse(schema);
    List<String> nameList = lists[0];
    List<String> typeList = lists[1];

    List<String> expectedNames = new ArrayList<String>(4);
    expectedNames.add("col1");
    expectedNames.add("col2");
    List<String> expectedTypes = new ArrayList<String>(4);
    expectedTypes.add("int");
    expectedTypes.add("string");

    assertEquals(expectedNames, nameList);
    assertEquals(expectedTypes, typeList);
  }

  @Test
  public void testParseArray() {
    String schema = "`complex` ARRAY<STRING>";
    List<String>[] lists = HiveSchemaUtil.parse(schema);
    List<String> nameList = lists[0];
    List<String> typeList = lists[1];

    List<String> expectedNames = new ArrayList<String>(4);
    expectedNames.add("complex");
    List<String> expectedTypes = new ArrayList<String>(4);
    expectedTypes.add("array");

    assertEquals(expectedNames, nameList);
    assertEquals(expectedTypes, typeList);
  }

  @Test
  public void testParseComplex() {
    String schema = "`id` INT, `name` STRING, `arr` ARRAY<STRUCT<`one`:STRING,`two`:BIGINT,`three`:STRUCT<`four`:STRING>,`five`:STRING>>";
    List<String>[] lists = HiveSchemaUtil.parse(schema);
    List<String> nameList = lists[0];
    List<String> typeList = lists[1];

    List<String> expectedNames = new ArrayList<String>(4);
    expectedNames.add("id");
    expectedNames.add("name");
    expectedNames.add("arr");
    List<String> expectedTypes = new ArrayList<String>(4);
    expectedTypes.add("int");
    expectedTypes.add("string");
    expectedTypes.add("array<struct<one:string,two:bigint,three:struct<four:string>,five:string>>");

    assertEquals(expectedNames, nameList);
    assertEquals(expectedTypes, typeList);
  }
}
