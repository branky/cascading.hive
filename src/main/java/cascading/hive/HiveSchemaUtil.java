package cascading.hive;

import java.util.ArrayList;

/**
 *
 */
public class HiveSchemaUtil {

    /**
     * The method to parse hive schema string, returns an array of two list instances, the first is for field names, the
     * second for types.
     * @param schema hive scheme
     * @return List of String
     */
    public static ArrayList<String>[] parse(String schema) {
        String[] pairs = schema.split(",");
        ArrayList<String> names = new ArrayList<String>(pairs.length);
        ArrayList<String> types = new ArrayList<String>(pairs.length);
        ArrayList[] ret = new ArrayList[] {names, types};

        for (String str : pairs)  {
           String[] pair = str.trim().split(" ");
           if (pair.length != 2) {
               throw new RuntimeException("malformed <name,type> pair found: " + str );
           }
           names.add(pair[0].trim().toLowerCase());
           types.add(pair[1].trim().toLowerCase());
        }
        if (names.size() == 0) {
            throw new RuntimeException("No name/type found, maybe malformed shema: " + schema );
        }

        return ret;
    }
}
