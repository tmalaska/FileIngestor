package com.cloudera.sa.fileingestor;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GetSmallFileInAvroByKeyMain {
  public static void main(String[] args) throws Exception {

    if (args.length == 0) {
      System.out.println("GetSmallFileInAvroByKeyMain <pathToAvroFile> <nameOfFileToGet> <outputFilePath>");
      return;
    }

    final String FIELD_FILENAME = "filename";
    final String FIELD_CONTENTS = "contents";

    Configuration config = new Configuration();
    FileSystem hdfs = FileSystem.get(config);
    Path destFile = new Path(args[0]);
    InputStream is = hdfs.open(destFile);

    DataFileStream<Object> reader = new DataFileStream<Object>(is,
        new GenericDatumReader<Object>());

    String keyToLookFor = args[1];

    boolean foundFile = false;

    for (Object o : reader) {
      GenericRecord r = (GenericRecord) o;      
      if (r.get(FIELD_FILENAME).toString().equals(keyToLookFor)) {
        BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(new File(args[2])));
        output.write(((ByteBuffer) r.get(FIELD_CONTENTS)).array());
        output.flush();
        output.close();
        foundFile = true;
      }
    }

    if (!foundFile) {
      System.out.println("The file '" + keyToLookFor + "' was not found in the Avro file '" + args[0] + "'");
    }
    else {
      System.out.println("The file '" + keyToLookFor + "' was found in the Avro file '" + args[0] + "'"
          + " and was copied to '" + args[2] + "'");
    }
  }
}
