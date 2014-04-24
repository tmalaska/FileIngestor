package com.cloudera.sa.fileingestor;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.apache.log4j.Logger;

import com.cloudera.sa.fileingestor.action.BigFileIngestToHDFSAction;
import com.cloudera.sa.fileingestor.action.CreateLocalWorkingDirAction;
import com.cloudera.sa.fileingestor.action.DistCpCopyAction;
import com.cloudera.sa.fileingestor.action.LittleFileIntoSeqCopyFromLocalAction;
import com.cloudera.sa.fileingestor.action.LittleFileIntoAvroCopyFromLocalAction;
import com.cloudera.sa.fileingestor.model.IngestionPlanPojo;
import com.cloudera.sa.fileingestor.model.IngestionPlanPojo.FileIngestionType;
import com.cloudera.sa.fileingestor.model.IngestionPlanPojo.HdfsCopyMethodType;
import com.cloudera.sa.fileingestor.plan.IngestionPlanFactory;

public class Main {
  
  static Logger logger = Logger.getLogger(Main.class);
  
  public static void main(String[] args) throws Exception {
    
    if (args.length == 0) {
      System.out.println("fileIngestor <propertiesFilePath>");
      return;
    }
    
    Properties p = new Properties();
    p.load(new FileInputStream(new File(args[0])));
    
    IngestionPlanPojo planPojo = IngestionPlanFactory.getInstance(p);
    
    CreateLocalWorkingDirAction createLocalWorkingDirAction = new CreateLocalWorkingDirAction(planPojo);
    createLocalWorkingDirAction.run();
    
    ArrayList<FileStatus> fileStatusList = new ArrayList<FileStatus>();
    
    logger.info("Version 0.01");
    logger.info("About to copy to HDFS: " + planPojo.getFileIngestionType());
    
    if (planPojo.getFileIngestionType().equals(FileIngestionType.BIG_FILES)) {
      BigFileIngestToHDFSAction bigFileCopyFromLocalAction = new BigFileIngestToHDFSAction(planPojo);
      bigFileCopyFromLocalAction.run();
      fileStatusList = bigFileCopyFromLocalAction.getCopiedFileStatuses();
    } else if (planPojo.getFileIngestionType().equals(FileIngestionType.SEQ_SMALL_FILES)) {
      LittleFileIntoSeqCopyFromLocalAction littleFileIntoSeqCopyFromLocalAction = new LittleFileIntoSeqCopyFromLocalAction(planPojo);
      littleFileIntoSeqCopyFromLocalAction.run();
      fileStatusList = littleFileIntoSeqCopyFromLocalAction.getCopiedFileStatuses();
    } else if (planPojo.getFileIngestionType().equals(FileIngestionType.AVRO_SMALL_FILES)) {
		LittleFileIntoAvroCopyFromLocalAction littleFileIntoAvroCopyFromLocalAction = new LittleFileIntoAvroCopyFromLocalAction(planPojo);
		littleFileIntoAvroCopyFromLocalAction.run();
		fileStatusList = littleFileIntoAvroCopyFromLocalAction.getCopiedFileStatuses();
	} else {
      throw new RuntimeException("not support operation yet. " + planPojo.getFileIngestionType());
    }
    logger.info("Files Copied to HDFS: " + fileStatusList.size());
    
    if (fileStatusList.size() > 0) {
      if (planPojo.getHdfsCopyMethod().equals(HdfsCopyMethodType.DISTCP)) {
        
        DistCpCopyAction distCpCopyAction = new DistCpCopyAction(fileStatusList, planPojo);
        distCpCopyAction.run();
      } else {
        throw new RuntimeException("not support operation yet. " + planPojo.getHdfsCopyMethod());
      } 
    }
  }
}
