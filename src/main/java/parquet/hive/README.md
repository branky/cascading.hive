This is patch for writing Parquet through the integration of Cascading and HCatalog.

MapredParquetOutputFormat#getRecordWriter is not enabled by default, so we have to enable it.
