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

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.CompositeTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.google.common.base.Splitter;

/**
 * This is a {@link Scheme} subclass. ORC(Optimized Record Columnar File) file format can be seen as "advanced" RCFile,
 * provides a more efficient way to store Hive data.
 * This class mainly developed for writing Cascading output to ORCFile format to be consumed by Hive afterwards. It also
 * support read, also support optimization as Hive(less HDFS_BYTES_READ less CPU).
 */
public class ORCFile extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {

    static enum Type { STRING, TINYINT, BOOLEAN, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, BIGDECIMAL, MAP;}

    /* columns' ids(start from zero), concatenated with comma. */
    private String selectedColIds = null;
    /* regular expression for comma separated string for column ids. */
    private static final Pattern COMMA_SEPARATED_IDS = Pattern.compile("^([0-9]+,)*[0-9]+$");
    private static final String MAP_FORMAT_REGEX = "map[\\<].*,.*[\\>]";
    private String[] types;
    private transient OrcSerde serde;

    private HashMap<String, Type> typeMapping = new HashMap<String, Type>(10);
    
    private static final PathFilter filter = new PathFilter() {
        @Override
        public boolean accept(Path path) {
            return !path.getName().startsWith("_");
        }
    };
    static final String DEFAULT_COL_PREFIX = "_col";
    
    /**
     * Construct an instance of ORCFile without specifying field names or
     * types. Infer struct info from ORC file or Cascading planner
     * 
     * */
    public ORCFile() {
        this(null);
    }

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
        if (hiveScheme != null) {
            ArrayList<String>[] lists = HiveSchemaUtil.parse(hiveScheme);
            Fields fields = new Fields(lists[0].toArray(new String[lists[0].size()]));
            setSinkFields(fields);
            setSourceFields(fields);
            this.types = lists[1].toArray(new String[lists[1].size()]);
            
            validate();
        }
        
        this.selectedColIds = selectedColIds;
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
        typeMapping.put("map", Type.MAP);
    }

    private void inferSchema(FlowProcess<JobConf> flowProcess, Tap tap) throws IOException {
        if (tap instanceof CompositeTap) {
            tap = (Tap) ((CompositeTap) tap).getChildTaps().next();
        }
        final String path = tap.getIdentifier();
        Path p = new Path(path);
        final FileSystem fs = p.getFileSystem(flowProcess.getConfigCopy());
        // Get all the input dirs
        List<FileStatus> statuses = new LinkedList<FileStatus>(Arrays.asList(fs.globStatus(p, filter)));
        // Now get all the things that are one level down
        for (FileStatus status : new LinkedList<FileStatus>(statuses)) {
            if (status.isDir())
                for (FileStatus child : Arrays.asList(fs.listStatus(status.getPath(), filter))) {
                    if (child.isDir()) {
                        statuses.addAll(Arrays.asList(fs.listStatus(child.getPath(), filter)));
                    } else if (fs.isFile(child.getPath())) {
                        statuses.add(child);
                    }
                }
        }
        for (FileStatus status : statuses) {
            Path statusPath = status.getPath();
            if (fs.isFile(statusPath)) {
                Reader reader = OrcFile.createReader(fs, statusPath);
                StructObjectInspector soi = (StructObjectInspector) reader.getObjectInspector();
                if (soi.getAllStructFieldRefs().size() != 0) {
                    extractSourceFields(soi);
                    break;
                }
            }
        }
        
        if (getSourceFields() == null || getSourceFields().size() == 0) {
            throw new IOException("unable to infer schema. please specify manually");
        }
    }
    
    private void extractSourceFields(StructObjectInspector soi) {
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        
        int[] colIds = new int[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            colIds[i] = i;
        }
        
        types = new String[colIds.length];
        Fields srcFields = new Fields();
        for (int i = 0; i < types.length; i++) {
            types[i] = fields.get(colIds[i]).getFieldObjectInspector().getTypeName();
            if (types[i].startsWith(PrimitiveCategory.DECIMAL.name().toLowerCase())) {
                types[i] = Type.BIGDECIMAL.name().toLowerCase();
            }
            srcFields = srcFields.append(new Fields(fields.get(colIds[i]).getFieldName()));
        }
        setSourceFields(srcFields);
    }
    
    /**
     * Method retrieveSourceFields notifies a Scheme when it is appropriate to dynamically
     * update the fields it sources. Schema will be inferred if type info is not specified
     * <p/>
     * The {@code FlowProcess} presents all known properties resolved by the current planner.
     * <p/>
     * The {@code tap} instance is the parent {@link Tap} for this Scheme instance.
     *
     * @param flowProcess of type FlowProcess
     * @param tap         of type Tap
     * @return Fields
     */
    @Override
    public Fields retrieveSourceFields(FlowProcess<JobConf> flowProcess, Tap tap) {
        if (types == null) {    // called by planner
            try {
                inferSchema(flowProcess, tap);
                validate();
            } catch (Exception e) {
                throw new RuntimeException("error inferring schema from input: " + tap.getIdentifier(), e);
            }
        }
     
        return getSourceFields();
    }

    @Override
    public void sourcePrepare(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall ) throws IOException {
        sourceCall.setContext(new Object[3]);

        sourceCall.getContext()[0] = sourceCall.getInput().createKey();
        sourceCall.getContext()[1] = sourceCall.getInput().createValue();
        sourceCall.getContext()[2] = createObjectInspector(getSourceFields());
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
        
        if (types == null) {    // never initialized by the planner
            try {
                inferSchema(flowProcess, tap);
                validate();
            } catch (Exception e) {
                throw new RuntimeException("error inferring schema from input: " + tap.getIdentifier(), e);
            }
        }
    }

    @Override
    public boolean source(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {
        if (!sourceReadInput(sourceCall))
            return false;
        Tuple tuple = sourceCall.getIncomingEntry().getTuple();
        OrcStruct o = (OrcStruct)sourceCall.getContext()[1];
        tuple.clear();
        struct2Tuple(o, tuple, (ObjectInspector)sourceCall.getContext()[2]);
        return true;
    }

    private void struct2Tuple(OrcStruct struct, Tuple tuple, ObjectInspector oi) {
        Object value = null;
        StructObjectInspector soi = (StructObjectInspector) oi;
        List<Object> values = soi.getStructFieldsDataAsList(struct);
        
        for (int i = 0; i < types.length; i++) {
            String type = types[i].toLowerCase();
            if (type.startsWith("map")) {
                type = "map";
            }

            switch (typeMapping.get(type)) {
            case INT:
                value = values.get(i) == null ? null : ((IntWritable) values.get(i)).get();
                break;
            case BOOLEAN:
                value = values.get(i) == null ? null : ((BooleanWritable) values.get(i)).get();
                break;
            case TINYINT:
                value = values.get(i) == null ? null : ((ByteWritable) values.get(i)).get();
                break;
            case SMALLINT:
                value = values.get(i) == null ? null : ((ShortWritable) values.get(i)).get();
                break;
            case BIGINT:
                value = values.get(i) == null ? null : ((LongWritable) values.get(i)).get();
                break;
            case FLOAT:
                value = values.get(i) == null ? null : ((FloatWritable) values.get(i)).get();
                break;
            case DOUBLE:
                value = values.get(i) == null ? null : ((DoubleWritable) values.get(i)).get();
                break;
            case BIGDECIMAL:
            	value =  values.get(i) == null ? null :
                	((HiveDecimalWritable) values.get(i)).getHiveDecimal().bigDecimalValue();
                break;
            case MAP:
                value = values.get(i) == null ? null : ((Map<?, ?>) values.get(i));
                break;
            case STRING:
            default:
                value = values.get(i) == null ? null : values.get(i).toString();
            }

            tuple.add(value);
        }
    }
    
    /**
     * fix the output path issue of OrcOutputFormats
     */
    public static class OrcSchemeOutputFormat extends OrcOutputFormat {
        @Override
        public RecordWriter getRecordWriter(FileSystem fileSystem,
                JobConf conf, String name, Progressable reporter)
                throws IOException {
            Path file = FileOutputFormat.getTaskOutputPath(conf, name);
            return super.getRecordWriter(fileSystem, conf, file.toString(),
                    reporter);
        }
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
        conf.setOutputKeyClass(NullWritable.class);
        conf.setOutputValueClass(Writable.class);
        conf.setOutputFormat(OrcSchemeOutputFormat.class );
    }

    @Override
    public void sinkPrepare(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall ) throws IOException {
        if (serde == null) {
            serde = new OrcSerde();
        }
        sinkCall.setContext(new Object[2]);
        if (types != null) {
            SettableStructObjectInspector oi = (SettableStructObjectInspector) createObjectInspector(getSinkFields());
            OrcStruct struct = (OrcStruct) oi.create();
            struct.setNumFields(getSinkFields().size());
            sinkCall.getContext()[0] = struct;
            sinkCall.getContext()[1] = oi;
        } else {    // lazy-initialize since we have no idea about the schema now
            sinkCall.getContext()[0] = null;
            sinkCall.getContext()[1] = null;
        }
    }

    @Override
    public void sinkCleanup(FlowProcess<JobConf> flowProcess,
                            SinkCall<Object[], OutputCollector> sinkCall) {
        sinkCall.setContext(null);
    }

    @Override
    public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
        TupleEntry entry = sinkCall.getOutgoingEntry();
        if (sinkCall.getContext()[0] == null) { // initialize
            extractSinkFields(entry);
            SettableStructObjectInspector oi = (SettableStructObjectInspector) createObjectInspector(getSinkFields());
            OrcStruct struct = (OrcStruct) oi.create();
            struct.setNumFields(getSinkFields().size());
            sinkCall.getContext()[0] = struct;
            sinkCall.getContext()[1] = oi;
        }
        
        Tuple tuple = entry.getTuple();
        OrcStruct struct = (OrcStruct)sinkCall.getContext()[0];
        tuple2Struct(tuple, struct, (ObjectInspector)sinkCall.getContext()[1]);
        Writable row = serde.serialize(struct, (ObjectInspector)sinkCall.getContext()[1]);
        sinkCall.getOutput().collect(null, row);
    }
    
    private void extractSinkFields(TupleEntry entry) {
        Fields fields = entry.getFields();
        if (fields == Fields.UNKNOWN) {
            fields = new Fields();
            for (int i = 0; i < entry.size(); i++) {
                fields = fields.append(new Fields(DEFAULT_COL_PREFIX + i));
            }
        }
        setSinkFields(fields);
        
        types = new String[entry.size()];
        for (int i = 0; i < entry.size(); i++) {
            Object o = entry.getObject(i);
            if (o instanceof Integer) {
                types[i] = Type.INT.name().toLowerCase();
            } else if (o instanceof Boolean) {
                types[i] = Type.BOOLEAN.name().toLowerCase();
            } else if (o instanceof Byte) {
                types[i] = Type.TINYINT.name().toLowerCase();
            } else if (o instanceof Short) {
                types[i] = Type.SMALLINT.name().toLowerCase();
            } else if (o instanceof Long) {
                types[i] = Type.BIGINT.name().toLowerCase();
            } else if (o instanceof Float) {
                types[i] = Type.FLOAT.name().toLowerCase();
            } else if (o instanceof Double) {
                types[i] = Type.DOUBLE.name().toLowerCase();
            } else if (o instanceof BigDecimal) {
                types[i] = Type.BIGDECIMAL.name().toLowerCase();
            } else if (o instanceof Map<?, ?>) {
                types[i] = Type.MAP.name().toLowerCase();
            } else {
                types[i] = Type.STRING.name().toLowerCase();
            }
        }
        
        // field types have higher priority (if they are not null)
        java.lang.reflect.Type[] fieldTypes = fields.getTypes();
        if (fieldTypes == null) {
            return;
        }
        for (int i = 0; i < fieldTypes.length; i++) {
            if (fieldTypes[i] == null) {
                continue;
            }
            if (fieldTypes[i] == Integer.class) {
                types[i] = Type.INT.name().toLowerCase();
            } else if (fieldTypes[i] == Boolean.class) {
                types[i] = Type.BOOLEAN.name().toLowerCase();
            } else if (fieldTypes[i] == Byte.class) {
                types[i] = Type.TINYINT.name().toLowerCase();
            } else if (fieldTypes[i] == Short.class) {
                types[i] = Type.SMALLINT.name().toLowerCase();
            } else if (fieldTypes[i] == Long.class) {
                types[i] = Type.BIGINT.name().toLowerCase();
            } else if (fieldTypes[i] == Float.class) {
                types[i] = Type.FLOAT.name().toLowerCase();
            } else if (fieldTypes[i] == Double.class) {
                types[i] = Type.DOUBLE.name().toLowerCase();
            } else if (fieldTypes[i] == BigDecimal.class) {
                types[i] = Type.BIGDECIMAL.name().toLowerCase();
            } else if (fieldTypes[i] == String.class) {
                types[i] = Type.STRING.name().toLowerCase();
            } else if (fieldTypes[i] == Map.class) {
                types[i] = Type.MAP.name().toLowerCase();
            }
        }
    }

    private void tuple2Struct(Tuple tuple, OrcStruct struct, ObjectInspector oi) {
        Object value = null;
        SettableStructObjectInspector orcOi = (SettableStructObjectInspector) oi;
        List<StructField> structFields = (List<StructField>) orcOi.getAllStructFieldRefs();
        
        for (int i = 0; i < types.length; i++) {
            String type = types[i].toLowerCase();
            if (type.startsWith("map")) {
                type = "map";
            }
            switch (typeMapping.get(type)) {
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
                value = tuple.getObject(i) == null ? null : new HiveDecimalWritable(HiveDecimal.create(new BigDecimal(
                        tuple.getString(i))));
                break;
            case MAP:
                value = tuple.getObject(i) == null ? null : createMapWritable(splitToMap(tuple.getString(i)), types[i]);
                break;
            case STRING:
            default:
                value = tuple.getObject(i) == null ? null : new Text(tuple.getString(i));
            }
            
            orcOi.setStructFieldData(struct, structFields.get(i), value);
        }
    }



    private ObjectInspector createObjectInspector(Fields fields) {
        int size = fields.size();
        List<String> fieldNames = new ArrayList<String>(size);
        List<TypeInfo> typeInfos = new ArrayList<TypeInfo>(size);
        
        for (int i = 0; i < size; i++) {
            fieldNames.add(fields.get(i).toString());
            typeInfos.add(getTypeInfo(types[i]));
        }
        TypeInfo structInfo = TypeInfoFactory.getStructTypeInfo(fieldNames, typeInfos);

        return OrcStruct.createObjectInspector(structInfo);
    }

    /**
     * Split the given string format to Map
     * 
     * @param input
     *            The map format
     * @return Map instance
     */
    private Map<?, ?> splitToMap(String input) {
        String mapFormat = input.substring(input.indexOf("{") + 1, input.lastIndexOf("}"));
        if (mapFormat == null || mapFormat.isEmpty()) {
            return null;
        }
        return Splitter.on(",").withKeyValueSeparator(":").split(mapFormat);
    }

    /**
     * Creates MapWritable instance with key and value
     * 
     * @param map
     *            Map instance
     * @param mapString
     *            Map format used to parse the type of key and value
     * @return MapWritable
     */
    private MapWritable createMapWritable(Map<?, ?> map, String mapString) {
        MapWritable result = new MapWritable();

        if (map != null) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                TypeInfo[] types = getMapValueTypes(mapString);
                result.put(createObject(entry.getKey(), types[0]), createObject(entry.getValue(), types[1]));
            }
        }
        return result;
    }

    /**
     * Creates appropriate Writable instance with value. The default will be string.
     * 
     * @param obj
     *            The value
     * @param typeInfo
     *            Type of value
     * @return Writable
     */
    private Writable createObject(Object obj, TypeInfo typeInfo) {
        Type type = typeMapping.get(typeInfo.getTypeName()) == null ? Type.STRING : typeMapping.get(typeInfo
                .getTypeName());
        String value = obj.toString();
        switch (type) {
        case INT:
            return new IntWritable(Integer.parseInt(value));
        case BOOLEAN:
            return new BooleanWritable(Boolean.valueOf(value));
        case TINYINT:
            return new ByteWritable(Byte.valueOf(value));
        case SMALLINT:
            return new ShortWritable(Short.parseShort(value));
        case BIGINT:
            return new LongWritable(Long.parseLong(value));
        case FLOAT:
            return new FloatWritable(Float.parseFloat(value));
        case DOUBLE:
            return new DoubleWritable(Double.parseDouble(value));
        case BIGDECIMAL:
            return new HiveDecimalWritable(HiveDecimal.create(new BigDecimal(value)));
        case STRING:
        default:
            return new Text((String) value);
        }
    }

    /**
     * Parse the given map format string to array of TypeInfo which contains key
     * type and value type respectively
     * 
     * @param map
     *            The input map format
     * @return Array of TypeInfo
     */
    private TypeInfo[] getMapValueTypes(String map) {

        validateMapString(map);

        TypeInfo[] typeInfos = new TypeInfo[2];

        String orginal = map.toLowerCase().replaceAll("[map<>]", "").trim();
        String[] types = orginal.split(",");

        for (int count = 0; count < 2; count++) {
            typeInfos[count] = getTypeInfo(types[count].trim());
        }
        return typeInfos;
    }

    /**
     * Check the string is a valid map format
     * 
     * @param mapString
     * @throws IllegalArgumentException
     *             if the map format is not valid
     */
    private void validateMapString(String mapString) {
        if (mapString == null || mapString.isEmpty() || (!mapString.toLowerCase().trim().matches(MAP_FORMAT_REGEX))) {
            throw new IllegalArgumentException("Invalid map format: " + mapString);
        }
    }

    /**
     * Gets the appropriate instance of {@link TypeInfo} from the given string.
     * Default type is STRING
     * 
     * @param field
     * @return TypeInfo
     */
    private TypeInfo getTypeInfo(String field) {

        if (field == null) {
            return null;
        }

        String fieldType = field;
        if (fieldType.toLowerCase().startsWith("map")) {
            fieldType = "map";
        }
        
        Type type = typeMapping.get(fieldType.toLowerCase()) == null ? Type.STRING : typeMapping.get(fieldType
                .toLowerCase());

        switch (type) {
        case INT:
            return TypeInfoFactory.intTypeInfo;
        case BOOLEAN:
            return TypeInfoFactory.booleanTypeInfo;
        case TINYINT:
            return TypeInfoFactory.byteTypeInfo;
        case SMALLINT:
            return TypeInfoFactory.shortTypeInfo;
        case BIGINT:
            return TypeInfoFactory.longTypeInfo;
        case FLOAT:
            return TypeInfoFactory.floatTypeInfo;
        case DOUBLE:
            return TypeInfoFactory.doubleTypeInfo;
        case BIGDECIMAL:
            return TypeInfoFactory.decimalTypeInfo;
        case MAP:
            TypeInfo[] mapFieldTypes = getMapValueTypes(field);
            return TypeInfoFactory.getMapTypeInfo(mapFieldTypes[0], mapFieldTypes[1]);
        case STRING:
        default:
            return TypeInfoFactory.stringTypeInfo;
        }
    }
}


