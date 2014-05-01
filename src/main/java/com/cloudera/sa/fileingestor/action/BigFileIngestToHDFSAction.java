package com.cloudera.sa.fileingestor.action;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import org.apache.commons.io.FileUtils;
import com.cloudera.sa.fileingestor.model.DstPojo;
import com.cloudera.sa.fileingestor.model.IngestionPlanPojo;


public class BigFileIngestToHDFSAction extends AbstractIngestToHDFSAction{

  FileSystem fs;
  BigFileIngestToHDFSAction thisObj;


  public BigFileIngestToHDFSAction(IngestionPlanPojo planPojo) {
    super(planPojo);
    thisObj = this;
  }

  @Override
  protected void ingestDataToHdfsDir() throws IOException, InterruptedException {
    fs = FileSystem.get(new Configuration());
    
    ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
    
    for (File file:  FileUtils.listFiles(new File(processingDir), null, true)) {
      executorService.execute(new CopyFileToHDFSThread(file));
    }
    executorService.shutdown();
    executorService.awaitTermination(3, TimeUnit.HOURS);
    fs.close();
  }
  
  final class CopyFileToHDFSThread implements Runnable {
    
    File sourceFile;

    public CopyFileToHDFSThread(File sourceFile) {
      this.sourceFile = sourceFile;
    }

    @Override
    public void run() {
      
      File processingDirFile = new File(processingDir);
      
      Path dstPath = new Path(destination.getPath() + sourceFile.getPath().substring(processingDirFile.getPath().length()));
      
      logger.info("Starting to ingest " + sourceFile + " to destiniation " + destination.getName() + " to location " + dstPath);
      
      try {
        fs.copyFromLocalFile(false, destination.isReplaceIfFileExist(), new Path(sourceFile.getPath()), dstPath);
      } catch (IOException e) {
        logger.error("Problem while coping " + sourceFile, e);
        thisObj.moveToFailure(sourceFile);
      }
      try {
        fs.setOwner(dstPath.getParent(), destination.getOwner(), destination.getGroup());
        fs.setPermission(dstPath.getParent(), new FsPermission(Short.parseShort(destination.getPermissions(), 8)));
        fs.setOwner(dstPath, destination.getOwner(), destination.getGroup());
        fs.setPermission(dstPath, new FsPermission(Short.parseShort(destination.getPermissions(), 8)));
        logger.info("Changing owner and permissions on: " + dstPath + " with octal notation: " + destination.getPermissions());
        copiedFiles.add(fs.getFileStatus(dstPath));
      } catch (IOException e) {
        logger.error("Problem changing permission to owner:" + destination.getOwner() + " group:" +  destination.getGroup() + " on HDFS file " + dstPath, e);
        try {
          fs.delete(dstPath, false);
        } catch (IOException e1) {
          logger.error("Problem removing file from HDFS");
        }
        thisObj.moveToFailure(sourceFile);
      }
      
    }
    

  }



  
}
