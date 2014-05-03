package com.cloudera.sa.fileingestor;

import com.cloudera.sa.fileingestor.aftercopy.CopyFromStaging;
import com.cloudera.sa.fileingestor.aftercopy.CopyToStaging;


public class Main {
  
  public static void main(String[] args) throws Exception {
    String command = args[0];
    
    String[] subArgs = new String[args.length - 1];
    System.arraycopy(args, 1, subArgs, 0, args.length - 1);
    
    if (command.equals("Ingest")) {
      IngestMain.main(subArgs);
    } else if (command.equals("listFilesInSeq")) {
      ListFilesInSeqMain.main(subArgs);
    } else if (command.equals("getSmallFileInSeqByKey")) {
      GetSmallFileInSeqByKeyMain.main(subArgs);
    } else if (command.equals("explodSmallFileSeqToLocal")) {
      
    } else if (command.equals("listFilesInAvro")) {
      ListFilesInAvroMain.main(subArgs);
    } else if (command.equals("getSmallFileInAvroByKey")) {
      GetSmallFileInAvroByKeyMain.main(subArgs);
    } else if (command.equals("explodSmallFileAvroToLocal")) {
      
    } else if (command.equals("combineSmallFileSeqFiles")) {
      CombineSmallFileSeqFiles.main(subArgs);
    } else if (command.equals("combineSmallFileAvroFiles")) {
      
    } else if (command.equals("CopyToStaging")) {
      CopyToStaging.main(subArgs);
    } else if (command.equals("CopyFromStaging")) {
      CopyFromStaging.main(subArgs);
    } else {
      System.out.println("Unknown command: " + command);
      System.out.println("----------------------------");
      System.out.println("- Possible Commands -");
      System.out.println("----------------------------");
      System.out.println("Ingest - Ingestion files from a given directory");
      System.out.println("listFilesInSeq - Outputs the list of files in a Seq file");
      System.out.println("getSmallFileInSeqByKey - This will get a small file out of a seq file");
      System.out.println("explodSmallFileSeqToLocal - This will explode all small files in a seq file to local");
      System.out.println("listFilesInAvro - Outputs the list of files in a Avro file");
      System.out.println("getSmallFileInAvroByKey - This will get a small file out of a seq file");
      System.out.println("explodSmallFileAvroToLocal - This will explode all small files in a seq file to local");
      System.out.println("combineSmallFileSeqFiles - This will take N number of seq files in HDFS and combine them to N less number of files");
      System.out.println("combineSmallFileAvroFiles - This will take N number of avro files in HDFS and combine them to N less number of files");
      System.out.println("CopyToStaging - This will copy a directory in HDFS into a staging directory in HDFS");
      System.out.println("CopyFromStaging - This will copy from staging directory in HDFS into a target directory also in HDFS");
    }
  }
}
