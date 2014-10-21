package com.twitter.scalding

import cascading.hcatalog.{HCatTap, DefaultHCatScheme}
import cascading.scheme.Scheme
import cascading.tap.Tap
import cascading.tuple.Fields

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

/**
  * HCatalog source for Scalding applications
  *
  * This is distinct from the [[com.twitter.scalding.ORCFile]] and
  * [[com.twitter.scalding.RCFile]] sources included in this library because
  * this accesses the table via HCatalog, rather than directly opening the ORC
  * or RCFile. This preserves the advantage of storage format independence
  * provided by HCatalog.
  *
  * This source doesn't support Cascading's own local mode because the
  * underlying [[cascading.hcatalog.HCatTap]] requires Hadoop dependencies. It
  * can still work in Hadoop's local mode however, so local/non-cluster usage
  * is possible.
  *
  * @param table Name of the HCatalog table to read from or write to.
  * @param db Name of the database that contains the target table. Defaults to null, meaning use HCatalog's default database.
  * @param filter Partition filter predicate; see the filter parameter on the [[cascading.hcatalog.HCatTap]] constructor.
  * @param path HDFS path where the table should be stored; mainly useful when creating new tables. Uses the table's current location by default.
  * @param sourceFields List of Cascading fields for the tap. Reads the fields from the table's metadata by default.
  */
case class HCatalog(table: String, db: String = null, filter: String = null, path: String = null, sourceFields: Fields = null) extends SchemedSource {
  override val hdfsScheme = new DefaultHCatScheme(db, table, filter, sourceFields)
    .asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    mode match {
      case Hdfs(_, _) => new HCatTap(db, table, filter, hdfsScheme, path, sourceFields, sinkMode).asInstanceOf[Tap[_, _, _]]
      case _ => throw ModeException("Cascading local mode not supported for: " + toString)
    }
  }
}
