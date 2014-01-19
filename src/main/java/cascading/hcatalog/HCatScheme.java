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
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.*;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@SuppressWarnings({ "serial", "rawtypes" })
public abstract class HCatScheme extends
		Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {

	private static final Logger LOG = LoggerFactory
			.getLogger(HCatScheme.class);

	private String db;
	private String table;
	private String filter;
	private int randomNumber;
	private Table hiveTable;
	private HCatSchema hCatSchema;
	private Fields sourceFields;

/**
	 * 
	 * @param db
	 * @param table
	 * @param filter Partition filter. The filter string should look like:
	 *               "ds=20120401" where the datestamp "ds" is the partition column
	 *                name and "20120401" is the value you want to read (year,
	 *                month, and day). A filter can contain the operators 'and', 'or', 'like', 
	 *                '()', '=', '<>' (not equal), '<', '>', '<=' and '>=' if the filter
	 *                is Scan filter. only operator '=' is allowed is the filter is
	 *                write filter
	 */
	public HCatScheme(String db, String table, String filter,
			Fields sourceFields) {
		this.db = CascadingHCatUtil.hcatDefaultDBIfNull(db);
		this.table = table;
		this.filter = filter;
		this.sourceFields = sourceFields;

		randomNumber = new Random(System.currentTimeMillis()).nextInt();
	}

	@Override
	public void sourceConfInit(FlowProcess<JobConf> flowProcess,
			Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {

		hiveTable = CascadingHCatUtil.getHiveTable(db, table, conf);
		conf.setInputFormat(getTableInputFormat(hiveTable, filter, conf));

		hCatSchema = getTableHCatSchema(hiveTable, filter, conf);
		Fields fieldsFromSchema = new Fields(createFieldsArray(hCatSchema));

		if (sourceFields == null) {
			setSourceFields(fieldsFromSchema);
		} else {
			validate(fieldsFromSchema);
			setSourceFields(sourceFields);
		}
	}

	private void validate(Fields fieldsFromSchema) {
		if (!fieldsFromSchema.contains(sourceFields)) {
			throw new IllegalArgumentException("Source fields:" + sourceFields
					+ " must match table schema:" + fieldsFromSchema);
		}
	}

	private String[] createFieldsArray(HCatSchema hcatSchema) {
		List<String> fields = hcatSchema.getFieldNames();
		String[] fieldsArray = fields.toArray(new String[fields.size()]);
		return fieldsArray;
	}

	/**
	 * @param hiveTable
	 * @param filter
	 * @param conf
	 * @return
	 */
	protected abstract Class<? extends InputFormat> getTableInputFormat(
			Table hiveTable, String filter, JobConf conf);

	/**
	 * @param hiveTable
	 * @param filter
	 * @param conf
	 * @return
	 */
	protected abstract HCatSchema getTableHCatSchema(Table hiveTable,
			String filter, JobConf conf);

	@Override
	public void sinkConfInit(FlowProcess<JobConf> flowProcess,
			Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {

		hiveTable = CascadingHCatUtil.getHiveTable(db, table, conf);
		conf.setOutputFormat(getTableOutputFormat(hiveTable, filter, conf));

		hCatSchema = getTableHCatSchema(hiveTable, filter, conf);
	}

	/**
	 * @param hiveTable
	 * @param filter
	 * @param conf
	 * @return
	 */
	protected abstract Class<? extends OutputFormat> getTableOutputFormat(
			Table hiveTable, String filter, JobConf conf);

	@Override
	public void sourcePrepare(FlowProcess<JobConf> flowProcess,
			SourceCall<Object[], RecordReader> sourceCall) throws IOException {
		Object[] pair = new Object[] { sourceCall.getInput().createKey(),
				sourceCall.getInput().createValue() };

		sourceCall.setContext(pair);
	}

	@Override
	public void sinkPrepare(FlowProcess<JobConf> flowProcess,
			SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
		List<TypeInfo> colTypes = new ArrayList<TypeInfo>();

		for (HCatFieldSchema fieldSchema : getHCatSchema().getFields()) {
			colTypes.add(TypeInfoUtils.getTypeInfoFromTypeString(fieldSchema
					.getTypeString()));
		}

		StructTypeInfo rowTypeInfo = (StructTypeInfo) TypeInfoFactory
				.getStructTypeInfo(getHCatSchema().getFieldNames(), colTypes);
		ObjectInspector rowOI = TypeInfoUtils
				.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);

		sinkCall.setContext(new Object[] { rowOI });
	}

	@Override
	public boolean source(FlowProcess<JobConf> flowProcess,
			SourceCall<Object[], RecordReader> sourceCall) throws IOException {

		if (!sourceReadInput(sourceCall)) {
			return false;
		}

		Tuple tuple = sourceCall.getIncomingEntry().getTuple();
		tuple.clear();

		Object value = sourceCall.getContext()[1];
		readValue(tuple, value);

		return true;
	}

	/**
	 * @param tuple
	 * @param value
	 */
	protected abstract void readValue(Tuple tuple, Object value);

	private boolean sourceReadInput(
			SourceCall<Object[], RecordReader> sourceCall) throws IOException {
		Object[] context = sourceCall.getContext();

		return sourceCall.getInput().next(context[0], context[1]);
	}

	@Override
	public void sink(FlowProcess<JobConf> flowProcess,
			SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
		TupleEntry tupleEntry = sinkCall.getOutgoingEntry();

		writeValue(tupleEntry.getTuple(), tupleEntry.getFields(),
				sinkCall.getContext(), sinkCall.getOutput());
	}

	/**
	 * 
	 * @param tuple
	 * @param fields
	 *            The fields that are bound to tuple entry
	 * @param context
	 * @param output
	 * @throws IOException
	 */
	protected abstract void writeValue(Tuple tuple, Fields fields,
			Object[] context, OutputCollector output) throws IOException;

	@Override
	public void sourceCleanup(FlowProcess<JobConf> flowProcess,
			SourceCall<Object[], RecordReader> sourceCall) {
		sourceCall.setContext(null);
	}

	@Override
	public void sinkCleanup(FlowProcess<JobConf> flowProcess,
			SinkCall<Object[], OutputCollector> sinkCall) {
		sinkCall.setContext(null);
	}

	protected String getFilter() {
		return filter;
	}

	protected Table getHiveTable() {
		return hiveTable;
	}

	protected HCatSchema getHCatSchema() {
		return hCatSchema;
	}

	// Got to override here as to avoid 'no such vertex in graph' issue.
	// Need to ensure hashcode and equals dont change even internal state
	// changes
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = randomNumber;
		result = prime * result + ((db == null) ? 0 : db.hashCode());
		result = prime * result + ((filter == null) ? 0 : filter.hashCode());
		result = prime * result + ((table == null) ? 0 : table.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (getClass() != obj.getClass())
			return false;
		HCatScheme other = (HCatScheme) obj;
		if (db == null) {
			if (other.db != null)
				return false;
		} else if (!db.equals(other.db))
			return false;
		if (filter == null) {
			if (other.filter != null)
				return false;
		} else if (!filter.equals(other.filter))
			return false;
		if (table == null) {
			if (other.table != null)
				return false;
		} else if (!table.equals(other.table))
			return false;
		return true;
	}
}
