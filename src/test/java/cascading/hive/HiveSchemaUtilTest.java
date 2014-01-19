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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;

/**
 */
public class HiveSchemaUtilTest {

    @Test
    public void testParse() {
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
}
