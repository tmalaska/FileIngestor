package com.cloudera.sa.fileingestor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;

import java.io.*;

public class ListFilesInAvroMain {

  public static void main(String[] args) throws Exception {

    if (args.length == 0) {
      System.out.println("ListFilesInAvroMain <pathToAvroFile>");
      return;
    }

    final String FIELD_FILENAME = "filename";

    Configuration config = new Configuration();
    FileSystem hdfs = FileSystem.get(config);
    Path destFile = new Path(args[0]);
    InputStream is = hdfs.open(destFile);

    DataFileStream<Object> reader = new DataFileStream<Object>(is,
        new GenericDatumReader<Object>());

    int counter = 0;
    for (Object o : reader) {
      GenericRecord r = (GenericRecord) o;
      System.out.println(counter++ + ":" + r.get(FIELD_FILENAME).toString());
    }
    IOUtils.cleanup(null, is);
    IOUtils.cleanup(null, reader);
  }

}
