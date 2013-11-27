package cascading.hive;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.type.CoercibleType;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarStruct;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Properties;

/**
 * This is a {@link Scheme} subclass. RCFile (Record Columnar File) format can partition the data horizontally(rows) and
 * vertically(columns) and allows to fetch only the specific columns during the processing and avoid the Disk IO penalty
 * with all the columns.
 * This class mainly developed for writing Cascading output to RCFile format to be consumed by Hive afterwards. It also
 * support read RCFile format, but it's not optimized as Hive.
 */
public class RCFile extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {

    private transient ColumnarSerDe serde;
    private String[] types;

    /**Construct an instance of RCFile using specified array of field names and types.
    * @param names field names
    * @param types field types
    * */
    public RCFile(String[] names, String[] types) {
        super(new Fields(names), new Fields(names));
        this.types = types;
        validate();
    }

    /**
     * Construct an instance of RCFile using hive table scheme. Table schema should be a space and comma separated string
     * describing the Hive schema, e.g.:
     * uid BIGINT, name STRING, description STRING
     * specifies 3 fields
    * @param hiveScheme hive table scheme
    */
    public RCFile(String hiveScheme) {
        ArrayList<String>[] lists = HiveSchemaUtil.parse(hiveScheme);
        Fields fields = new Fields(lists[0].toArray(new String[lists[0].size()]));
        setSinkFields(fields);
        setSourceFields(fields);
        this.types = lists[1].toArray(new String[lists[1].size()]);
        validate();
    }

    private void validate() {
        if (types.length != getSourceFields().size()) {
            throw new IllegalArgumentException("fields size and length of fields types not match.");
        }

    }

    @Override
    public void sourceConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
        conf.setInputFormat(RCFileInputFormat.class);
    }

    @Override
    public void sourcePrepare(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall ) throws IOException {
        if (serde == null) {
            try {
                serde = new ColumnarSerDe();
                serde.initialize(flowProcess.getConfigCopy(), getProps());
            } catch (SerDeException e) {
                throw new RuntimeException("Unable to initialize SerDe.");
            }
        }
        sourceCall.setContext(new Object[2]);

        sourceCall.getContext()[0] = sourceCall.getInput().createKey();
        sourceCall.getContext()[1] = sourceCall.getInput().createValue();
    }

    private Properties getProps() {
        Properties props = new Properties();
        Fields fields = getSourceFields();
        StringBuilder sb = new StringBuilder();
        StringBuilder sbType = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            sb.append(fields.get(i)).append(",");
            sbType.append(types[i]).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sbType.deleteCharAt(sbType.length() - 1);

        props.put(HiveProps.HIVE_COLUMNS, sb.toString());
        props.put(HiveProps.HIVE_COLUMN_TYPES, sbType.toString());
        return props;
    }

    @Override
    public void sourceCleanup(FlowProcess<JobConf> flowProcess,
                              SourceCall<Object[], RecordReader> sourceCall) {
        sourceCall.setContext(null);
    }

    private boolean sourceReadInput(
            SourceCall<Object[], RecordReader> sourceCall) throws IOException {
        Object[] context = sourceCall.getContext();
        return sourceCall.getInput().next(context[0], context[1]);
    }

    @Override
    public boolean source(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {
        if (!sourceReadInput(sourceCall))
            return false;
        Tuple tuple = sourceCall.getIncomingEntry().getTuple();
        BytesRefArrayWritable value = (BytesRefArrayWritable) sourceCall.getContext()[1];
        try {
            ColumnarStruct struct = (ColumnarStruct) serde.deserialize(value);
            ArrayList<Object> objects = struct.getFieldsAsList();
            tuple.clear();
            tuple.addAll(objects.toArray());
            return true;
        } catch (SerDeException e) {
            throw new IOException(e);
        }

    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
        conf.setOutputKeyClass(WritableComparable.class);
        conf.setOutputValueClass(BytesRefArrayWritable.class);
        conf.setOutputFormat(RCFileOutputFormat.class );
        conf.set(HiveProps.HIVE_COLUMN_NUMBER, String.valueOf(getSinkFields().size()));
    }

    @Override
    public void sinkPrepare(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall ) throws IOException {
        sinkCall.setContext(new Object[3]);
        sinkCall.getContext()[0] = new ByteStream.Output();
        sinkCall.getContext()[1] = new BytesRefArrayWritable();
        sinkCall.getContext()[2] =  new BytesRefWritable[getSinkFields().size()];
    }

    @Override
    public void sinkCleanup(FlowProcess<JobConf> flowProcess,
                            SinkCall<Object[], OutputCollector> sinkCall) {
        sinkCall.setContext(null);
    }

    @Override
    public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
        Tuple tuple = sinkCall.getOutgoingEntry().getTuple();

        ByteStream.Output byteStream = (ByteStream.Output) sinkCall.getContext()[0];
        BytesRefArrayWritable rowWritable = (BytesRefArrayWritable) sinkCall.getContext()[1];
        BytesRefWritable[] colValRefs = (BytesRefWritable[]) sinkCall.getContext()[2];
        if (tuple.size() != colValRefs.length) {
            throw new RuntimeException("fields size and length of column buffer not match.");
        }

        byteStream.reset();
        int startPos = 0;
        for (int i = 0; i < colValRefs.length; i++) {
            colValRefs[i] = new BytesRefWritable();
            rowWritable.set(i, colValRefs[i]);

            sinkField(byteStream, tuple.getObject(i), tuple.getTypes()[i]);
            colValRefs[i].set(byteStream.getData(), startPos, byteStream.getCount() - startPos);
            startPos = byteStream.getCount();
        }

        sinkCall.getOutput().collect(null, rowWritable);
    }

    private void sinkField(OutputStream out, Object field, Type fieldType) throws IOException {
        if (field == null) {
            return; // just leave it empty
        }
        if (fieldType instanceof CoercibleType) {
            CoercibleType<?> coercible = (CoercibleType<?>) fieldType;
            out.write(coercible.coerce(field, String.class).toString().getBytes());
        } else {
            out.write(field.toString().getBytes());
        }
        //TODO: need handle more cases


    }

}

