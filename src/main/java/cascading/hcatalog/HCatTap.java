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

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author txiao
 * 
 */
@SuppressWarnings("serial")
public class HCatTap extends Tap<JobConf, RecordReader, OutputCollector> {
    public static final String IGNORE_FILE_IN_PARTITION_REGEX = "hive-tap.path.partition.file.ignore-regex";

    /** Field LOG */
	private static final Logger LOG = LoggerFactory.getLogger(HCatTap.class);

	private String db;
	private String table;
	private String filter;
	private String path;
	private Tap<JobConf, RecordReader, OutputCollector> tap;
    private String identifier;

	public HCatTap(String table) {
		this(null, table, null, null, null, null, SinkMode.REPLACE);
	}

	public HCatTap(String table, String path) {
		this(null, table, null, null, path, null, SinkMode.REPLACE);
	}

	public HCatTap(String table, Fields sourceFields) {
		this(null, table, null, null, null, sourceFields, SinkMode.REPLACE);
	}

	public HCatTap(String db, String table, String filter) {
		this(db, table, filter, null, null, null, SinkMode.REPLACE);
	}

	public HCatTap(String db, String table, String filter, String path) {
		this(db, table, filter, null, path, null, SinkMode.REPLACE);
	}

/**
	 * Construct a new HCatTap instance.
	 * 
	 * @param db
	 * @param table
	 * @param filter
	 *            Partition filter. The filter string should look like:
	 *            "ds=20120401" where the datestamp "ds" is the partition column
	 *            name and "20120401" is the value you want to read (year,
	 *            month, and day). A filter can contain the operators 'and', 'or', 'like', 
	 *                '()', '=', '<>' (not equal), '<', '>', '<=' and '>=' if the filter
	 *                is Scan filter. only operator '=' is allowed is the filter is
	 *                write filter
	 * @param hCatScheme 
	 * @param path 
	 * @param sinkMode 
	 */
	public HCatTap(String db, String table, String filter,
                   Scheme<JobConf,RecordReader,OutputCollector,?,?> hCatScheme,
                   String path,
                   Fields sourceField,
			       SinkMode sinkMode) {
		super(hCatScheme, sinkMode);

		// Use the default scheme if it is null
		if (hCatScheme == null) {
			setScheme(new DefaultHCatScheme(db, table, filter, sourceField));
		}

		this.db = CascadingHCatUtil.hcatDefaultDBIfNull(db);
		this.table = table;
		this.filter = filter;
		this.path = path;

        resolveIdentifier();
	}

    private void resolveIdentifier() {
        StringBuilder sb = new StringBuilder("hcatalog://").
                append(this.db).append(".").append(this.table);
        if (this.path != null) {
            sb.append("?path=").append(this.path);
        }
        if (this.filter != null) {
            sb.append(this.filter);
        }
        identifier = sb.toString();
    }

	@Override
	public void sourceConfInit(FlowProcess<JobConf> process, JobConf conf) {
		tap = TapFactory.createSourceTap(getScheme(),
                CascadingHCatUtil.getDataStorageLocation(db, table, filter,
                        conf));

		tap.sourceConfInit(process, conf);
	}

	@Override
	public void sinkConfInit(FlowProcess<JobConf> process, JobConf conf) {
		List<String> pathes = new ArrayList<String>();

		// Write to the same location as the where current table is stored
		if (path == null) {
			pathes = CascadingHCatUtil.getDataStorageLocation(db, table,
					filter, conf);
		} else {
			pathes.add(path);
		}

		tap = TapFactory.createSinkTap(getScheme(), pathes);
		tap.sinkConfInit(process, conf);
	}

	@Override
	public String getIdentifier() {
        return identifier;
	}

	@Override
	public TupleEntryIterator openForRead(FlowProcess<JobConf> flowProcess,
			RecordReader input) throws IOException {
		return tap.openForRead(flowProcess, input);
	}

	@Override
	public TupleEntryCollector openForWrite(FlowProcess<JobConf> flowProcess,
			OutputCollector output) throws IOException {
		return tap.openForWrite(flowProcess, output);
	}

	@Override
	public boolean createResource(JobConf conf) throws IOException {
		return tap.createResource(conf);
	}

	@Override
	public boolean deleteResource(JobConf conf) throws IOException {
		return tap.deleteResource(conf);
	}

	@Override
	public boolean resourceExists(JobConf conf) throws IOException {
		return tap.resourceExists(conf);
	}

	@Override
	public long getModifiedTime(JobConf conf) throws IOException {
		return tap.getModifiedTime(conf);
	}

	@Override
	public boolean commitResource(JobConf conf) throws IOException {
        if (tap.commitResource(conf)) {
            if (path != null) {
                // Set the path as the new table location
                return CascadingHCatUtil.setDataStorageLocation(db, table, filter, path, conf);
            }
            return true;
		} else {
            return false;
        }
    }
}
