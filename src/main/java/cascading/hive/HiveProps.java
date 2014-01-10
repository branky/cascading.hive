package cascading.hive;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;

/**
 */
public class HiveProps {

    public static final String HIVE_COLUMNS = serdeConstants.LIST_COLUMNS;
    public static final String HIVE_COLUMN_TYPES =  serdeConstants.LIST_COLUMN_TYPES;

    public static final String HIVE_COLUMN_NUMBER = org.apache.hadoop.hive.ql.io.RCFile.COLUMN_NUMBER_CONF_STR;
    /* ONLY work for ORC -- will uncomment once ORC supported
    public static final String HIVE_SELECTD_COLUMNS = ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR;
    */
    public static final String HIVE_SELECTD_COLUMN_IDS = ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR;
    public static final String HIVE_READ_ALL_COLUMNS = "hive.io.file.read.all.columns";
}
