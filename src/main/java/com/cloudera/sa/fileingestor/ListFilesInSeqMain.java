package com.cloudera.sa.fileingestor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.io.Text;

public class ListFilesInSeqMain {
  
  public static void main(String[] args) throws Exception {
    
    if (args.length == 0) {
      System.out.println("ListFilesInSeqMain <pathToSequenceFile>");
      return;
    }
    
    Configuration config = new Configuration();
    
    Option fileOption = SequenceFile.Reader.file(new Path(args[0]));
    
    SequenceFile.Reader out = new SequenceFile.Reader(config, fileOption);
   
    Text key = new Text();
    
    int counter = 0;
    while (out.next(key)) {
      System.out.println(counter++ + ":" + key.toString());
    }
    
    out.close();
  }
  
}
