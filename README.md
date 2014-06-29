# Welcome #

 This is the Cascading.Hive module.

 It provides Cascading Tap/Scheme for HCatalog and Scheme for Hive native file formats(RCFile and ORC).



# Notes #


Maven dependency
----------------
```
<dependency>
  <groupId>com.ebay</groupId>
  <artifactId>cascading-hive</artifactId>
  <version>0.0.3-SNAPSHOT</version>
  <scope>compile</scope>
</dependency> 
```
 

Hive version
------------
Currently, this module works with Apache Hive 0.12 (version 0.0.2-SNAPSHOT) and 0.13 (version 0.0.3-SNAPSHOT). If you want to use it with other versions of Hive, you need to patch [few classes](https://github.com/branky/cascading.hive/tree/master/src/main/java/org/apache/hadoop/hive).




Projection pushdown
-------------------
Both RC and ORC support projection pushdown to reduce read I/O when only a subset of fields needed.


You can enalbe this either by creating the scheme using additional argument to indicate the selected columns, e.g.

```
//only col1 and col4 will be read
Scheme rcScheme = new RCFile("col1 int, col2 string, col3 string, col4 long", "0,3");

Scheme orcScheme = new ORCFile("col1 int, col2 string, col3 string, col4 long", "0,3");

```

or by setting Hive specific properties for your flow:

```
hive.io.file.read.all.columns=false
hive.io.file.readcolumn.ids=0,3
```

HCatalog usage
--------------
To talk with your production HCatalog, you have to include real hive-site.xml in your artifact. Once you build a fat jar artifact, you need to add additional libs into CLASSPATH, because they are excluded from this artifact.

```
hadoop jar $your_fat_jar -libjars $HIVE_HOME/lib/hive-metastore.jar,$HIVE_HOME/lib/datanucleus-core-x.y.z.jar,$HIVE_HOME/lib/datanucleus-rdbms-x.y.z.jar,$HIVE_HOME/lib/datanucleus-api-jdo-x.y.z.jar $your_options
```

Scalding usage
--------------
To use RCFile/ORC with Scalding, check out [ColumnarSerDeSource.scala](https://github.com/branky/cascading.hive/blob/master/src/main/scala/com/twitter/scalding/ColumnarSerDeSource.scala). It requires Scalding 0.9.1.


