package com.cloudera.sa.fileingestor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.cloudera.sa.fileingestor.action.BigFileIngestToHDFSAction;
import com.cloudera.sa.fileingestor.action.CreateLocalWorkingDirAction;
import com.cloudera.sa.fileingestor.action.DistCpCopyAction;
import com.cloudera.sa.fileingestor.action.LittleFileIntoAvroCopyFromLocalAction;
import com.cloudera.sa.fileingestor.action.LittleFileIntoSeqCopyFromLocalAction;
import com.cloudera.sa.fileingestor.action.UpdatePrivsOfFirstLandingLocalAction;
import com.cloudera.sa.fileingestor.model.DstPojo;
import com.cloudera.sa.fileingestor.model.IngestionPlanPojo;
import com.cloudera.sa.fileingestor.model.IngestionPlanPojo.FileIngestionType;
import com.cloudera.sa.fileingestor.model.IngestionPlanPojo.HdfsCopyMethodType;
import com.cloudera.sa.fileingestor.plan.IngestionPlanFactory;

public class IngestMain {
static Logger logger = Logger.getLogger(IngestMain.class);
  
  public static void main(String[] args) throws Exception {
    
    String log4jConfPath = "./log4j.properties";
    PropertyConfigurator.configure(log4jConfPath);
    
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
    
    logger.info("Version 0.03");
    logger.info("About to copy to HDFS: " + planPojo.getFileIngestionType());
    
    validateAndCreateIfFoldersExist(planPojo);
    
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
      Exception e = new RuntimeException("not support operation yet. " + planPojo.getFileIngestionType());
      logger.error(e.getMessage());
      throw e;
    }
    logger.info("Files Copied to HDFS: " + fileStatusList.size());
    
    UpdatePrivsOfFirstLandingLocalAction updatePrivs = new UpdatePrivsOfFirstLandingLocalAction(planPojo);
    updatePrivs.run();
    
    if (fileStatusList.size() > 0) {
      if (planPojo.getHdfsCopyMethod().equals(HdfsCopyMethodType.DISTCP)) {
        
        DistCpCopyAction distCpCopyAction = new DistCpCopyAction(fileStatusList, planPojo);
        distCpCopyAction.run();
      } else {
        Exception e = new RuntimeException("not support operation yet. " + planPojo.getHdfsCopyMethod());
        logger.error(e.getMessage());
        throw e;
      } 
    }
  }
  
  public static void validateAndCreateIfFoldersExist(IngestionPlanPojo planPojo) throws IOException{
    FileSystem fs = FileSystem.get(new Configuration());
    for (DstPojo dst: planPojo.getDstList()) {

      String group = dst.getGroup();
      String owner = dst.getOwner();
      String permissions = dst.getPermissions();
      
      Path path = new Path(dst.getPath());
      if (!dst.isCreateDir()) {
        //check is directory exist
        if (!fs.exists(path)) {
          IOException e = new IOException("The folder '" + path + "' doesn't exist and the configs say don't create directories");
          logger.error(e.getMessage());
          throw e;
        }
      }else {
        if (fs.exists(path)) { 
          if (!fs.isDirectory(path)) {
            IOException e = new IOException("The path '" + path + "' is not a folder but it exists.");
            logger.error(e.getMessage());
            throw e;
          }
        } else {
          mkdir(fs, group, owner, permissions, path);
        }
      }
    }
  }
  
  public static void mkdir(FileSystem fs, String group, String owner, String permissions, Path path) throws IOException {
    if (!fs.exists(path.getParent())) {
      mkdir(fs, group, owner, permissions, path.getParent());
    }
    fs.mkdirs(path);
    fs.setOwner(path, owner, group);
    fs.setPermission(path, new FsPermission(Short.parseShort(permissions, 8)));
  }
}
