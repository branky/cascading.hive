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

import java.util.ArrayList;
import java.util.List;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenRewriteStream;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.ql.parse.ASTErrorNode;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveLexer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 *
 */
public class HiveSchemaUtil {
  public static ArrayList<String>[] parse(String schema) {
    HiveLexer lexer = new HiveLexer(new ExpressionTree.ANTLRNoCaseStringStream(schema));
    HiveParser parser = new HiveParser(new TokenRewriteStream(lexer));
    parser.setTreeAdaptor(new CommonTreeAdaptor() {
      @Override
      public Object create(Token payload) {
        return new ASTNode(payload);
      }

      @Override
      public Object dupNode(Object t) { return create(((CommonTree)t).token); }

      @Override
      public Object errorNode(TokenStream input, Token start, Token stop, RecognitionException e) {
        return new ASTErrorNode(input, start, stop, e);
      }
    });
    HiveParser.columnNameTypeList_return columns;
    try {
      columns = parser.columnNameTypeList();
    } catch (RecognitionException e) {
      throw new RuntimeException("malformed <name/type> pair found: " + parser.getErrorHeader(e) + " " + e.getMessage());
    }
    ASTNode root = (ASTNode) columns.getTree();
    List<FieldSchema> list;
    try {
      list = BaseSemanticAnalyzer.getColumns(root, false);
    } catch (SemanticException e) {
      throw new RuntimeException("malformed <name/type> pair found: " + e.getMessage());
    }
    ArrayList<String> names = new ArrayList<String>();
    ArrayList<String> types = new ArrayList<String>();
    ArrayList[] ret = new ArrayList[] {names, types};

    for (FieldSchema fs : list) {
      names.add(fs.getName());
      types.add(fs.getType());
    }
    return ret;
  }
}
