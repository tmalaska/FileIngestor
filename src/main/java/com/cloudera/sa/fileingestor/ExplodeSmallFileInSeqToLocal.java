package com.cloudera.sa.fileingestor;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.log4j.Logger;

public class ExplodeSmallFileInSeqToLocal {
  
  static Logger logger = Logger.getLogger(ExplodeSmallFileInSeqToLocal.class);
  
  public static void main (String[] args) throws IOException {
    if (args.length == 0) {
      System.out.println("ExplodeSmallFileInSeqToLocal <pathToSequenceFile> <outputFilePath>");
      return;
    }

    String inputFile = args[0];
    String outputLocalFile = args[1];
    
    Configuration config = new Configuration();

    Option fileOption = SequenceFile.Reader.file(new Path(inputFile));

    SequenceFile.Reader out = new SequenceFile.Reader(config, fileOption);

    Text key = new Text();
    BytesWritable value = new BytesWritable();
    
    int counter = 0;
    
    while (out.next(key, value)) {
      
      File newFile = new File(outputLocalFile + "/" + key.toString());
      
      File parantFolder = new File(newFile.getParent());
      
      if (!parantFolder.exists()) {
        if (!parantFolder.mkdirs()) {
          logger.error("Unable to make folder :" + parantFolder.toString());
          throw new RuntimeException("Unable to make folder :" + parantFolder.toString());
        }
        logger.info("Made dir: " + parantFolder);
      }
      
      BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(newFile));
      output.write(value.copyBytes());
      output.flush();
      output.close();
      logger.info("Wrote: " + newFile);
      counter++;
    }

    out.close();
    logger.info("Finished (Total Files:" + counter + ")");
  }
}
