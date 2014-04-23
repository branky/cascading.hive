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

import cascading.cascade.CascadeException;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

/**
 * 
 * @author txiao
 * 
 */
public class CascadingHCatUtil {
	private static final Logger LOG = LoggerFactory
			.getLogger(CascadingHCatUtil.class);

	/**
	 * 
	 * @param db
	 * @param table
	 * @param filter
	 * @param jobConf
	 * @return A list of locations
	 */
	public static List<String> getDataStorageLocation(String db, String table,
			String filter, JobConf jobConf) {
		Preconditions.checkNotNull(table, "Table name must not be null");

		HiveMetaStoreClient client = null;
		List<String> locations = new ArrayList<String>();

		try {
			client = getHiveMetaStoreClient(jobConf);
			Table hiveTable = HCatUtil.getTable(client, db, table);

			// Partition is required
			if (null != StringUtils.stripToNull(filter)) {
				if (hiveTable.getPartitionKeys().size() != 0) {
					// Partitioned table
					List<Partition> parts = client.listPartitionsByFilter(db,
							table, filter, (short) -1);

					if (parts.size() > 0) {
						// Return more than one partitions when filter is
						// something
						// like ds >= 1234
						for (Partition part : parts) {
							locations.addAll(getFilesInHivePartition(part, jobConf));
						}
					} else {
						logError("Table " + hiveTable.getTableName()
								+ " doesn't have the specified partition:"
								+ filter, null);
					}
				} else {
					logError(
							"Table " + hiveTable.getTableName()
									+ " doesn't have the specified partition:"
									+ filter, null);
				}
			} else {
				locations.add(hiveTable.getTTable().getSd().getLocation());
			}
		} catch (IOException e) {
			logError("Error occured when getting hiveconf", e);
		} catch (MetaException e) {
			logError("Error occured when getting HiveMetaStoreClient", e);
		} catch (NoSuchObjectException e) {
			logError("Table doesn't exist in HCatalog: " + table, e);
		} catch (TException e) {
			logError("Error occured when getting Table", e);
		} finally {
			HCatUtil.closeHiveClientQuietly(client);
		}

		return locations;
	}

    protected static List<String> getFilesInHivePartition(Partition part, JobConf jobConf) {
        List<String> result = newArrayList();

        try {
            Path partitionDirPath = new Path(part.getSd().getLocation());
            FileStatus[] partitionContent = partitionDirPath.getFileSystem(jobConf).listStatus(partitionDirPath);
            for(FileStatus currStatus : partitionContent) {
                if(!currStatus.isDir()) {
                    result.add(currStatus.getPath().toUri().getPath());
                }
            }

        } catch (IOException e) {
            logError("Unable to read the content of partition '" + part.getSd().getLocation() + "'", e);
        }

        return result;
    }

	/**
	 * 
	 * @param db
	 * @param table
	 * @param filter
	 * @param path
	 * @param jobConf
	 * @return 
	 */
	public static boolean setDataStorageLocation(String db, String table,
			String filter, String path, JobConf jobConf) {
		Preconditions.checkNotNull(table, "Table name must not be null");

		HiveMetaStoreClient client = null;
		List<String> locations = new ArrayList<String>();

		try {
			client = getHiveMetaStoreClient(jobConf);
			
			Table hiveTable = HCatUtil.getTable(client, db, table);
			hiveTable.setDataLocation(new URI(path));
			
			client.alter_table(db, table, hiveTable.getTTable());
		} catch (IOException e) {
			logError("Error occured when getting hiveconf", e);
		} catch (MetaException e) {
			logError("Error occured when getting HiveMetaStoreClient", e);
		} catch (NoSuchObjectException e) {
			logError("Table doesn't exist in HCatalog: " + table, e);
		} catch (TException e) {
			logError("Error occured when getting Table", e);
		} catch (URISyntaxException e) {
			logError("Error occured when getting uri from path:" + path, e);
		} finally {
			HCatUtil.closeHiveClientQuietly(client);
		}

		return true;
	}

	public static Table getHiveTable(String db, String table, JobConf conf) {
		HiveMetaStoreClient client = null;
		Table hiveTable = null;

		try {
			client = getHiveMetaStoreClient(conf);
			hiveTable = HCatUtil.getTable(client, db, table);
		} catch (IOException e) {
			logError("Error occured when getting hiveconf", e);
		} catch (MetaException e) {
			logError("Error occured when getting HiveMetaStoreClient", e);
		} catch (NoSuchObjectException e) {
			logError("Table doesn't exist in HCatalog: " + table, e);
		} catch (TException e) {
			logError("Error occured when getting Table", e);
		} finally {
			HCatUtil.closeHiveClientQuietly(client);
		}

		return hiveTable;
	}

	private static HiveMetaStoreClient getHiveMetaStoreClient(JobConf jobConf)
			throws IOException, MetaException {
		HiveConf hiveConf = HCatUtil.getHiveConf(jobConf);

		return HCatUtil.getHiveClient(hiveConf);
	}

	/**
	 * Build {@link org.apache.hive.hcatalog.data.schema.HCatSchema} of table
	 * from a list of {@link org.apache.hadoop.hive.metastore.api.FieldSchema}
	 * 
	 * @param columns
	 * @return
	 */
	public static HCatSchema buildHCatSchema(List<FieldSchema> columns) {
		HCatSchema schema = null;
		
		try {
			schema = new HCatSchema(HCatUtil.getHCatFieldSchemaList(columns));
		} catch (HCatException e) {
			logError("Error occured when building table schema", e);
		}
		
		return schema;
	}

	private static void logError(String message, Exception e) {
		LOG.error(message, e);
		throw new CascadeException(e);
	}
	
	/**
	 * Assign HCatalog default db value to db if it is null
	 * 
	 * @param db
	 * @return
	 */
	public static String hcatDefaultDBIfNull(String db) {
		return db == null ? MetaStoreUtils.DEFAULT_DATABASE_NAME : db;
	}
}
