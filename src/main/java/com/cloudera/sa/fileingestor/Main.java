package com.cloudera.sa.fileingestor;


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
      
    } else if (command.equals("getSmallFileInAvroByKey")) {
      
    } else if (command.equals("explodSmallFileAvroToLocal")) {
      
    } 
  }
}
