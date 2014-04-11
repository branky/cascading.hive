package com.twitter.scalding

import scala.collection.JavaConversions._

import cascading.scheme.Scheme
import cascading.tap.SinkMode
import cascading.tuple.Fields
import com.twitter.scalding
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

////
// trait ColumnarSerDeScheme -
// @columns:                 array of column names
// @columnTypes:             array of column type names (parallel to @columns)
// @defaultType:             default hive type name for missing types
trait ColumnarSerDeScheme extends SchemedSource {
    // override this to name the source columns
    val columns:List[String]
    // override this to associate a type with each column name
    val columnTypes:List[String] = Nil
    // override this to change the default type name for columns
    val defaultType:String = "string"
 
    ////
    // typeNames() - convenience method which adds missing type info
    //
    // Return:
    //   @columnTypes plus any missing values set to @defaultType
    def typeNames:List[String] = columnTypes ++ List.fill(columns.size)(defaultType)
}

////
// case class RCFile - RCFile ColumnarSerDeScheme source
// @p:           source path
// @columns:     column names
// @columnTypes: column type names
// @sinkMode:    cascading sink mode option
case class RCFile(p:String
                 ,override val columns:List[String]
                 ,override val columnTypes:List[String] = Nil
                 ,override val sinkMode:SinkMode = SinkMode.REPLACE)
    extends FixedPathSource(p)
    with ColumnarSerDeScheme {
    ////
    // @hdfsScheme: specifies the hdfs scheme to be an RCFile
    override val hdfsScheme = new cascading.hive.RCFile(columns.toArray, typeNames.toArray)
    .asInstanceOf[Scheme[JobConf, RecordReader[_,_], OutputCollector[_,_],_,_]]
}

////
// case class ORCFile - ORCFile ColumnarSerDeScheme source
// @p:            source path
// @columns:      column names
// @columnTypes:  column type names
// @sinkMode:     cascading sink mode option
case class ORCFile(p:String
                  ,override val columns:List[String]
                  ,override val columnTypes:List[String] = Nil
                  ,override val sinkMode:SinkMode = SinkMode.REPLACE)
    extends FixedPathSource(p)
    with ColumnarSerDeScheme {
    ////
    // @hdfsScheme: specifies the hdfs scheme to be an ORCFile
    override val hdfsScheme = new cascading.hive.ORCFile(columns.toArray, typeNames.toArray)
    .asInstanceOf[Scheme[JobConf, RecordReader[_,_], OutputCollector[_,_],_,_]]
}
