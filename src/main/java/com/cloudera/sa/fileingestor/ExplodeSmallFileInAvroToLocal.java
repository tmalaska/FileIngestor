package com.cloudera.sa.fileingestor;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.cloudera.sa.fileingestor.action.common.AvroConts;

public class ExplodeSmallFileInAvroToLocal {
static Logger logger = Logger.getLogger(ExplodeSmallFileInAvroToLocal.class);
  
  public static void main (String[] args) throws IOException {
    if (args.length == 0) {
      System.out.println("ExplodeSmallFileInAvroToLocal <pathToSequenceFile> <outputFilePath>");
      return;
    }

    String inputFile = args[0];
    String outputLocalFile = args[1];
    
    Configuration config = new Configuration();

    FileSystem fs = FileSystem.get(config);
    
    InputStream is = fs.open(new Path(inputFile));
    DataFileStream<Object> reader = new DataFileStream<Object>(is,
        new GenericDatumReader<Object>());

    
    int counter = 0;
    
    for (Object o : reader) {
      GenericRecord r = (GenericRecord) o; 
      
      File newFile = new File(outputLocalFile + "/" + r.get(AvroConts.FIELD_FILENAME).toString());
      
      File parantFolder = new File(newFile.getParent());
      
      if (!parantFolder.exists()) {
        if (!parantFolder.mkdirs()) {
          logger.error("Unable to make folder :" + parantFolder.toString());
          throw new RuntimeException("Unable to make folder :" + parantFolder.toString());
        }
        logger.info("Made dir: " + parantFolder);
      }
      
      BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(newFile));
      output.write(((ByteBuffer) r.get(AvroConts.FIELD_CONTENTS)).array());
      output.flush();
      output.close();
      logger.info("Wrote: " + newFile);
      counter++;
    }

    reader.close();
    logger.info("Finished (Total Files:" + counter + ")");
  }
}
