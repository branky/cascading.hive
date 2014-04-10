This is patch for reading/writing ORC with non-Hive frameworks.
Related Jira:
https://issues.apache.org/jira/browse/HIVE-5728
https://issues.apache.org/jira/browse/HIVE-6163
https://issues.apache.org/jira/browse/HIVE-6565

https://github.com/branky/hive/commit/20c62b3ec73a55d84eeffca6eab4af066e04e22a

OrcProto.java is copied from ql/src/gen/protobuf/gen-java/org/apache/hadoop/hive/ql/io/orc

It also includes patch for ORC bugs which fixed on Hive 0.13
https://issues.apache.org/jira/browse/HIVE-6369
