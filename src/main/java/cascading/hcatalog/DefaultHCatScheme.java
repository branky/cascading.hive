package cascading.hcatalog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.LazyHCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import cascading.cascade.CascadeException;
import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.tuple.Fields;
import cascading.tuple.FieldsResolverException;
import cascading.tuple.Tuple;

/**
 * Expose the underlying table information, like the
 * {@link org.apache.hadoop.mapred.InputFormat} the underlying data use. This
 * implementation assumes that partition shares same input/output format of
 * table
 * 
 * @author txiao
 * 
 */
public class DefaultHCatScheme extends HCatScheme {

	public DefaultHCatScheme(String table) {
		this(table, null, null, null);
	}
	
	public DefaultHCatScheme(String table, String filter) {
		this(table, filter, null, null);
	}
	
	public DefaultHCatScheme(String table, Fields sourceFields) {
		this(null, table, null, sourceFields);
	}
	
	public DefaultHCatScheme(String db, String table, String filter) {
		this(db, table, filter, null);
	}
	
	public DefaultHCatScheme(String table, String filter, Fields sourceFields) {
		this(null, table, filter, sourceFields);
	}
	
	public DefaultHCatScheme(String db, String table, String filter, Fields sourceFields) {
		super(db, table, filter, sourceFields);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * cascading.hcatalog.scheme.HCatScheme#getTableInputFormat(java.lang.String
	 * , java.lang.String, java.lang.String, org.apache.hadoop.mapred.JobConf)
	 */
	@Override
	protected Class<? extends InputFormat> getTableInputFormat(Table table,
			String filter, JobConf conf) {
		return table.getInputFormatClass();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * cascading.hcatalog.scheme.HCatScheme#getTableHCatSchema(java.lang.String,
	 * java.lang.String, java.lang.String, org.apache.hadoop.mapred.JobConf)
	 */
	@Override
	protected HCatSchema getTableHCatSchema(Table table, String filter,
			JobConf conf) {

		return CascadingHCatUtil.buildHCatSchema(table.getAllCols());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * cascading.hcatalog.scheme.HCatScheme#deserializeValue(java.lang.String,
	 * java.lang.String, java.lang.String, java.lang.Object)
	 */
	@Override
	protected void readValue(Tuple tuple, Object value) {
		Table table = getHiveTable();
		HCatSchema hCatSchema = getHCatSchema();

		Deserializer deserializer = table.getDeserializer();

		try {
			Object object = deserializer.deserialize((Writable) value);
			HCatRecord record = new LazyHCatRecord(object,
					deserializer.getObjectInspector());

			Fields fields = getSourceFields();

			for (int i = 0; i < fields.size(); i++) {
				tuple.add(record.get((String) fields.get(i), hCatSchema));
			}
		} catch (Exception e) {
			throw new CascadeException(
					"Error occured when deserializing value", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * cascading.hcatalog.scheme.HCatScheme#getTableOutputFormat(org.apache.
	 * hadoop.hive.ql.metadata.Table, java.lang.String,
	 * org.apache.hadoop.mapred.JobConf)
	 */
	@Override
	protected Class<? extends OutputFormat> getTableOutputFormat(
			Table hiveTable, String filter, JobConf conf) {
		return hiveTable.getOutputFormatClass();
	}

	@Override
	public void sinkPrepare(FlowProcess<JobConf> flowProcess,
			SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
		super.sinkPrepare(flowProcess, sinkCall);

		List<Object> context = new ArrayList<Object>(Arrays.asList(sinkCall.getContext()));
		context.add(new ArrayList<Object>());
		context.add(getHCatSchema().getFields());
		
		sinkCall.setContext(context.toArray());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * cascading.hcatalog.scheme.HCatScheme#writeValue(cascading.tuple.Tuple,
	 * org.apache.hadoop.mapred.OutputCollector)
	 */
	@Override
	protected void writeValue(Tuple tuple, Fields fields, Object[] context, OutputCollector output) throws IOException {
		Table table = getHiveTable();
		List<HCatFieldSchema> tableFields = (List<HCatFieldSchema>) context[2];
		
		List<Object> content = (List<Object>) context[1];
		content.clear();
		
		for (HCatFieldSchema tableField : tableFields) {
			try {
				int pos = fields.getPos(tableField.getName());
				
				content.add(tuple.getObject(pos));
			} catch (FieldsResolverException e) {
				// Table field doesn't exist in tuple. Use default value
				// It is fine if tuple field doesn't exist in table, just ignore
				// TODO need to handle more situation
				content.add(null);
			}
		}
		
		// Deserializer and Serializer of the same table
		// should be the same.
		SerDe serializer = (SerDe) table.getDeserializer();
		try {
			output.collect(null, serializer.serialize(content, (ObjectInspector) context[0]));
		} catch (SerDeException e) {
			throw new CascadeException("Error occured when writing data out", e);
		} catch (IOException e) {
			throw new CascadeException("Error occured when writing data out", e);
		}
	}
//	
//	private Object castField(Object field, Type from, String to) throws IOException, ClassNotFoundException {
//        if (field == null) {
//            return null; // just leave it empty
//        }
//        if (from instanceof CoercibleType) {
//            CoercibleType<?> coercible = (CoercibleType<?>) from;
//            return coercible.coerce(field, Class.forName(to));
//        }
//        
//        return field;
//    }
}
