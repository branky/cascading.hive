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
