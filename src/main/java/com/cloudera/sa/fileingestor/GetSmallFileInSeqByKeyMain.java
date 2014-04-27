package com.cloudera.sa.fileingestor;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader.Option;

public class GetSmallFileInSeqByKeyMain {
  public static void main(String[] args) throws Exception {

    if (args.length == 0) {
      System.out.println("GetSmallFileInSeqByKeyMain <pathToSequenceFile> <nameOfFileToGet> <outputFilePath>");
      return;
    }

    String keyToLookFor = args[1];
    
    Configuration config = new Configuration();

    Option fileOption = SequenceFile.Reader.file(new Path(args[0]));

    SequenceFile.Reader out = new SequenceFile.Reader(config, fileOption);

    Text key = new Text();
    BytesWritable value = new BytesWritable();

    boolean foundFile = false;
    
    while (out.next(key, value)) {
      if (key.toString().equals(keyToLookFor)) {
        BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(new File(args[2])));
        output.write(value.copyBytes());
        output.flush();
        output.close();
      }
    }

    out.close();
    
    if (!foundFile) {
      System.out.println("The file '" + keyToLookFor + "' was not found in the Seq file '" + args[0] + "'");
    }
  }
}
