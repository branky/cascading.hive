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
