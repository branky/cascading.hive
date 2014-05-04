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

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

/**
 * This is a {@link Scheme} subclass. ORC(Optimized Record Columnar File) file format can be seen as "advanced" RCFile,
 * provides a more efficient way to store Hive data.
 * This class mainly developed for writing Cascading output to ORCFile format to be consumed by Hive afterwards. It also
 * support read, also support optimization as Hive(less HDFS_BYTES_READ less CPU).
 */
public class ORCFile extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {

    static enum Type { STRING, TINYINT, BOOLEAN, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, BIGDECIMAL;}

    /*columns' ids(start from zero), concatenated with comma.*/
    private String selectedColIds = null;
    /*regular expression for comma separated string for column ids.*/
    private static final Pattern COMMA_SEPARATED_IDS = Pattern.compile("^([0-9]+,)*[0-9]+$");
    private String[] types;
    private transient OrcSerde serde;

    private HashMap<String, Type> typeMapping = new HashMap<String, Type>(10);


    /**Construct an instance of ORCFile using specified array of field names and types.
     * @param names field names
     * @param types field types
     * */
    public ORCFile(String[] names, String[] types) {
        this(names, types, null);
    }

    /**Construct an instance of ORCFile using specified array of field names and types.
     * @param names field names
     * @param types field types
     * @param selectedColIds a list of column ids (started from 0) to explicitly specify which columns will be used
     * */
    public ORCFile(String[] names, String[] types, String selectedColIds) {
        super(new Fields(names), new Fields(names));
        this.types = types;
        this.selectedColIds = selectedColIds;
        validate();
        initTypeMapping();
    }

    /**
     * Construct an instance of ORCFile using hive table scheme. Table schema should be a space and comma separated string
     * describing the Hive schema, e.g.:
     * uid BIGINT, name STRING, description STRING
     * specifies 3 fields
     * @param hiveScheme hive table scheme
     * @param selectedColIds a list of column ids (started from 0) to explicitly specify which columns will be used
     */
    public ORCFile(String hiveScheme, String selectedColIds) {
        ArrayList<String>[] lists = HiveSchemaUtil.parse(hiveScheme);
        Fields fields = new Fields(lists[0].toArray(new String[lists[0].size()]));
        setSinkFields(fields);
        setSourceFields(fields);
        this.types = lists[1].toArray(new String[lists[1].size()]);
        this.selectedColIds = selectedColIds;
        validate();
        initTypeMapping();
    }

    /**
     * Construct an instance of ORCFile using hive table scheme. Table schema should be a space and comma separated string
     * describing the Hive schema, e.g.:
     * uid BIGINT, name STRING, description STRING
     * specifies 3 fields
     * @param hiveScheme hive table scheme
     */
    public ORCFile(String hiveScheme) {
        this(hiveScheme, null);
    }

    private void validate() {
        if (types.length != getSourceFields().size()) {
            throw new IllegalArgumentException("fields size and length of fields types not match.");
        }
        if (selectedColIds != null) {
            if (!COMMA_SEPARATED_IDS.matcher(selectedColIds).find()) {
                throw new IllegalArgumentException("selected column ids must in comma separated formatted");
            }
        }

    }

    private void initTypeMapping() {
        typeMapping.put("int", Type.INT);
        typeMapping.put("string", Type.STRING);
        typeMapping.put("tinyint", Type.TINYINT);
        typeMapping.put("bigdecimal", Type.BIGDECIMAL);
        typeMapping.put("double", Type.DOUBLE);
        typeMapping.put("float", Type.FLOAT);
        typeMapping.put("bigint", Type.BIGINT);
        typeMapping.put("smallint", Type.SMALLINT);
        typeMapping.put("boolean", Type.BOOLEAN);
    }


    @Override
    public void sourcePrepare(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall ) throws IOException {
        sourceCall.setContext(new Object[2]);

        sourceCall.getContext()[0] = sourceCall.getInput().createKey();
        sourceCall.getContext()[1] = sourceCall.getInput().createValue();
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
    public void sourceConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
        conf.setInputFormat(OrcInputFormat.class);
        if (selectedColIds != null) {
            conf.set(HiveProps.HIVE_SELECTD_COLUMN_IDS, selectedColIds);
            conf.set(HiveProps.HIVE_READ_ALL_COLUMNS, "false");
        }
    }

    @Override
    public boolean source(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {
        if (!sourceReadInput(sourceCall))
            return false;
        Tuple tuple = sourceCall.getIncomingEntry().getTuple();
        OrcStruct o = (OrcStruct)sourceCall.getContext()[1];
        tuple.clear();
        struct2Tuple(o, tuple);
        return true;
    }

    private void struct2Tuple(OrcStruct struct, Tuple tuple) {
        Object value = null;
        for (int i = 0; i < types.length; i++) {
            switch(typeMapping.get(types[i].toLowerCase())) {
                case INT:
                    value = struct.getFieldValue(i) == null ? null : ((IntWritable)struct.getFieldValue(i)).get();
                    break;
                case BOOLEAN:
                    value = struct.getFieldValue(i) == null ? null : ((BooleanWritable)struct.getFieldValue(i)).get();
                    break;
                case TINYINT:
                    value = struct.getFieldValue(i) == null ? null : ((ByteWritable)struct.getFieldValue(i)).get();
                    break;
                case SMALLINT:
                    value = struct.getFieldValue(i) == null ? null : ((ShortWritable)struct.getFieldValue(i)).get();
                    break;
                case BIGINT:
                    value = struct.getFieldValue(i) == null ? null : ((LongWritable)struct.getFieldValue(i)).get();
                    break;
                case FLOAT:
                    value = struct.getFieldValue(i) == null ? null : ((FloatWritable)struct.getFieldValue(i)).get();
                    break;
                case DOUBLE:
                    value = struct.getFieldValue(i) == null ? null : ((DoubleWritable)struct.getFieldValue(i)).get();
                    break;
                case BIGDECIMAL:
                    value = struct.getFieldValue(i) == null ? null :
                            ((HiveDecimalWritable)struct.getFieldValue(i)).getHiveDecimal().bigDecimalValue();
                    break;
                case STRING:
                default:
                    value = struct.getFieldValue(i) == null ? null : struct.getFieldValue(i).toString();
            }

            tuple.add(value);
        }
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
        conf.setOutputKeyClass(NullWritable.class);
        conf.setOutputValueClass(OrcSerde.OrcSerdeRow.class);
        conf.setOutputFormat(OrcOutputFormat.class );
    }

    @Override
    public void sinkPrepare(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall ) throws IOException {
        if (serde == null) {
            serde = new OrcSerde();
        }
        sinkCall.setContext(new Object[2]);
        sinkCall.getContext()[0] = new OrcStruct(getSinkFields().size());
        sinkCall.getContext()[1] = createObjectInspector();
    }

    @Override
    public void sinkCleanup(FlowProcess<JobConf> flowProcess,
                            SinkCall<Object[], OutputCollector> sinkCall) {
        sinkCall.setContext(null);
    }

    @Override
    public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
        Tuple tuple = sinkCall.getOutgoingEntry().getTuple();
        OrcStruct struct = (OrcStruct)sinkCall.getContext()[0];
        tuple2Struct(tuple, struct);
        Writable row = serde.serialize(struct, (ObjectInspector)sinkCall.getContext()[1]);
        sinkCall.getOutput().collect(null, row);
    }

    private void tuple2Struct(Tuple tuple, OrcStruct struct) {
        Object value = null;
        for (int i = 0; i < types.length; i++) {
            switch(typeMapping.get(types[i].toLowerCase())) {
                case INT:
                    value = tuple.getObject(i) == null ? null : new IntWritable(tuple.getInteger(i));
                    break;
                case BOOLEAN:
                    value = tuple.getObject(i) == null ? null : new BooleanWritable(tuple.getBoolean(i));
                    break;
                case TINYINT:
                    value = tuple.getObject(i) == null ? null : new ByteWritable(Byte.valueOf(tuple.getString(i)));
                    break;
                case SMALLINT:
                    value = tuple.getObject(i) == null ? null : new ShortWritable(tuple.getShort(i));
                    break;
                case BIGINT:
                    value = tuple.getObject(i) == null ? null : new LongWritable(tuple.getLong(i));
                    break;
                case FLOAT:
                    value = tuple.getObject(i) == null ? null : new FloatWritable(tuple.getFloat(i));
                    break;
                case DOUBLE:
                    value = tuple.getObject(i) == null ? null : new DoubleWritable(tuple.getDouble(i));
                    break;
                case BIGDECIMAL:
                    value = tuple.getObject(i) == null ? null :
                            new HiveDecimalWritable(new HiveDecimal(new BigDecimal(tuple.getString(i))));
                    break;
                case STRING:
                default:
                    value = tuple.getObject(i) == null ? null : new Text(tuple.getString(i));
            }
            struct.setFieldValue(i, value);
        }
    }



    private ObjectInspector createObjectInspector() {
        int size = getSinkFields().size();
        List<OrcProto.Type> orcTypes = new ArrayList<OrcProto.Type>(size + 1);//the extra one for struct itself

        OrcProto.Type.Builder builder = OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRUCT);
        for (int i = 0; i < size; i++) {
            builder.addFieldNames(getSinkFields().get(i).toString());
            builder.addSubtypes(i + 1);
        }
        orcTypes.add(builder.build());

        for (String type : types) {
            switch(typeMapping.get(type.toLowerCase())) {
                case INT:
                    orcTypes.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.INT).build());
                    break;
                case BOOLEAN:
                    orcTypes.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.BOOLEAN).build());
                    break;
                case TINYINT:
                    orcTypes.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.BYTE).build());
                    break;
                case SMALLINT:
                    orcTypes.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.SHORT).build());
                    break;
                case BIGINT:
                    orcTypes.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.LONG).build());
                    break;
                case FLOAT:
                    orcTypes.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.FLOAT).build());
                    break;
                case DOUBLE:
                    orcTypes.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.DOUBLE).build());
                    break;
                case BIGDECIMAL:
                    orcTypes.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.DECIMAL).build());
                    break;
                case STRING:
                default:
                    orcTypes.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRING).build());
            }
        }

        return OrcStruct.createObjectInspector(0, orcTypes);
    }


}


