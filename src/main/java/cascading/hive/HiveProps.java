package cascading.hive;

import org.apache.hadoop.hive.serde.serdeConstants;

/**
 */
public class HiveProps {

    public static final String HIVE_COLUMNS = serdeConstants.LIST_COLUMNS;
    public static final String HIVE_COLUMN_TYPES =  serdeConstants.LIST_COLUMN_TYPES;

    public static final String HIVE_COLUMN_NUMBER = org.apache.hadoop.hive.ql.io.RCFile.COLUMN_NUMBER_CONF_STR;
}
