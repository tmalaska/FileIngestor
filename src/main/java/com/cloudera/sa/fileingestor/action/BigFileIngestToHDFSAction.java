package com.cloudera.sa.fileingestor.action;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

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
  protected void ingestDataToHdfsDir() throws IOException {
    fs = FileSystem.get(new Configuration());
    
    ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
    
    for (File file: new File(processingDir).listFiles()) {
      executorService.execute(new CopyFileToHDFSThread(file));
    }
    executorService.shutdown();
    fs.close();
  }
  
  final class CopyFileToHDFSThread implements Runnable {
    
    File sourceFile;

    public CopyFileToHDFSThread(File sourceFile) {
      this.sourceFile = sourceFile;
    }

    @Override
    public void run() {
      logger.info("Starting to ingest " + sourceFile);
      Path dstPath = new Path(distination.getPath() + "/" + sourceFile.getName());
      try {
        fs.copyFromLocalFile(false, distination.isReplaceIfFileExist(), new Path(sourceFile.getPath()), dstPath);
      } catch (IOException e) {
        logger.error("Problem while coping " + sourceFile, e);
        thisObj.moveToFailure(sourceFile);
      }
      try {
        fs.setOwner(dstPath, distination.getOwner(), distination.getGroup());
        fs.setPermission(dstPath, new FsPermission(distination.getPermissions()));
        copiedFiles.add(fs.getFileStatus(dstPath));
      } catch (IOException e) {
        logger.error("Problem changing permission to owner:" + distination.getOwner() + " group:" +  distination.getGroup() + " on HDFS file " + dstPath, e);
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
