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

package cascading.functions;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.google.common.base.Preconditions;
import java.sql.Timestamp;
import org.apache.hadoop.hive.common.type.HiveDecimal;

/**
 * {@link Function} to convert a {@link Tuple} of {@link String}s to a Java object recognizable by
 * Hive.
 */
public class ConvertToHiveJavaType extends BaseOperation implements Function {
  private final String[] types;

  public ConvertToHiveJavaType(Fields fields, String[] types) {
    super(types.length, fields);
    this.types = types;
  }

  @Override public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    Tuple input = functionCall.getArguments().getTuple();

    Preconditions.checkArgument(input.size() == types.length);

    Tuple output = new Tuple();
    for (int i = 0; i < types.length; i++) {
      String type = types[i].toLowerCase();
      if (type.equals("int")) {
        output.add(input.getInteger(i));
      } else if (type.equals("bigint")) {
        output.add(input.getLong(i));
      } else if (type.equals("float")) {
        output.add(input.getFloat(i));
      } else if (type.equals("double")) {
        output.add(input.getDouble(i));
      } else if (type.equals("decimal")) {
        output.add(HiveDecimal.create(input.getString(i)));
      } else if (type.equals("boolean")) {
        output.add(input.getBoolean(i));
      } else if (type.equals("binary")) {
        output.add(input.getString(i).getBytes());
      } else if (type.equals("string")) {
        output.add(input.getString(i));
      } else if (type.equals("timestamp")) {
        output.add(Timestamp.valueOf(input.getString(i)));
      } else {
        throw new IllegalArgumentException(
            String.format("%s is not a currently supported Hive type", type));
      }
    }
    functionCall.getOutputCollector().add(output);
  }
}
