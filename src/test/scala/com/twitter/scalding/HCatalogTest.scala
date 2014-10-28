package com.twitter.scalding

import cascading.hcatalog.{DefaultHCatScheme, HCatScheme, HCatTap}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.{JobConf, OutputCollector, RecordReader}

import org.specs2.mutable._
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner


@RunWith(classOf[JUnitRunner])  // Allows the Maven Surefire plugin to run these tests
class HCatalogSourceTest extends Specification {
  import Dsl._

  "A HCatalog Source" should {
    val testSource = HCatalog("sample_07")
    val conf = new Configuration()
    val hdfsMode = Hdfs(false, conf)
    val localMode = Local(false)

    // These tests are not idiomatic Specs tests to work around a type error
    // from the Scala compiler which I was unable to solve. The compiler was
    // throwing an invariant type error on the wildcard types defined in
    // Cascading's Scheme and Tap classes with the values that Specs appears
    // to be supplying for those parameters.

    "return a HCatScheme" in {
      val scheme = testSource.hdfsScheme
      if (scheme.isInstanceOf[DefaultHCatScheme]) success else failure
    }
    "return a HCatTap whether reading or writing in HDFS mode" in {
      val readTap = testSource.createTap(Read)(hdfsMode)
      val writeTap = testSource.createTap(Write)(hdfsMode)
      if (readTap.isInstanceOf[HCatTap] && writeTap.isInstanceOf[HCatTap]) success else failure
    }
    "throw an exception when creating a tap in non-HDFS mode" in {
      try {
        val tap = testSource.createTap(Read)(localMode)
        failure("failed to throw expected exception")
      } catch {
        case e: ModeException => success
        case _ => failure
      }
    }
  }
}
