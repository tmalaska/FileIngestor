package com.cloudera.sa.fileingestor.action;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileStatus;
import org.apache.log4j.Logger;

import com.cloudera.sa.fileingestor.model.DstPojo;
import com.cloudera.sa.fileingestor.model.IngestionPlanPojo;

public abstract class AbstractIngestToHDFSAction {
  
  Logger logger = Logger.getLogger(AbstractIngestToHDFSAction.class);
  
  String sourceDir;
  String processingDir;
  String failureDir;
  String successDir;
  int numberOfThreads;
  DstPojo distination;
  ArrayList<FileStatus> copiedFiles = new ArrayList<FileStatus>();
  
  public static final String PROCESS_DIR = "processDir";
  public static final String FAILURE_DIR = "failureDir";
  public static final String SUCCESS_DIR = "successDir";
  
  public AbstractIngestToHDFSAction(IngestionPlanPojo planPojo) {
    this.sourceDir = planPojo.getSourceLocalDir();
    
    this.processingDir = planPojo.getWorkingLocalDir() + "/" + planPojo.getJobId() + "/processing";
    this.failureDir = planPojo.getWorkingLocalDir() + "/" + planPojo.getJobId() + "/failure";
    this.successDir = planPojo.getWorkingLocalDir() + "/" + planPojo.getJobId() + "/successful";
    
    this.distination = planPojo.getDstList().get(0);
    this.numberOfThreads = planPojo.getNumberOfThreads();
  }
  
  public void run() throws IOException, InterruptedException {
    createLocalFolders();
    moveFilesToProcessFolder();
    ingestDataToHdfsDir();
    
    File processingDirFile = new File(processingDir);
    for (File file: processingDirFile.listFiles()) {
      moveToSucccess(file);
    }
    
    cleanup();
  }
  
  protected void createLocalFolders() {
    logger.info("creating directories");
    makeStagingFolder(processingDir);
    makeStagingFolder(failureDir);
    makeStagingFolder(successDir);
  }

  private void makeStagingFolder(String dir) {
    File dirFile = new File(dir);
    if (dirFile.mkdirs() == false) {
      logger.error("failed to create " + dir);  
    }
    logger.info("successfully created" + dir);
  }
  
  protected void moveFilesToProcessFolder() {
    File sourceDirFile = new File(sourceDir);
    
    for (File file: sourceDirFile.listFiles()) {
      if (file.renameTo(new File(processingDir + "/" + file.getName()))) {
        logger.info("moved " + file.getName() + " to "+ processingDir);
      } else {
        logger.error("unable to moved " + file.getName() + " to "+ processingDir);
      }
    }
  }
  
  abstract protected void ingestDataToHdfsDir() throws IOException, InterruptedException;
  
  protected void moveToSucccess(File localFile) {
    moveTo(localFile, successDir);
  }
  
  protected void moveToFailure(File localFile) {
    moveTo(localFile, failureDir);
  }
  
  protected void moveTo(File localFile, String toFolder) {
    if (localFile.renameTo(new File(toFolder + "/" + localFile.getName()))) {
      logger.info("moved " + localFile.getName() + " to "+ toFolder);
    } else {
      logger.error("unable to moved " + localFile.getName() + " to "+ toFolder);
    }
  }
  
  protected void cleanup() {
    //check the processing directory to make sure nothing is in there.
    
    File processingDirFile = new File(processingDir);
    
    for (File file: processingDirFile.listFiles()) {
      logger.error("file left in processing directory " + file.getName());
    }
  }
  
  public ArrayList<FileStatus> getCopiedFileStatuses() {
    return copiedFiles;
  }
}
