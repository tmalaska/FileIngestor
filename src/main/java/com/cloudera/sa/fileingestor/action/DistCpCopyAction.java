package com.cloudera.sa.fileingestor.action;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.tools.DistCp;
import org.apache.log4j.Logger;

import com.cloudera.sa.fileingestor.model.DstPojo;
import com.cloudera.sa.fileingestor.model.IngestionPlanPojo;


public class DistCpCopyAction {
  
  Logger logger = Logger.getLogger(DistCpCopyAction.class);
  
  ArrayList<FileStatus> fileStatuses;
  IngestionPlanPojo planPojo;
  FileSystem fs;
  
  public DistCpCopyAction(ArrayList<FileStatus> fileStatuses, IngestionPlanPojo planPojo) {
    this.planPojo = planPojo;
    this.fileStatuses = fileStatuses;
  }
  
  public void run() throws Exception {
    fs = FileSystem.get(new Configuration());
    
    String[] args = new String[fileStatuses.size() + 1];
    int counter = 0;
    for (FileStatus fileStatus: fileStatuses) {
      args[counter++] = fileStatus.getPath().toString();
    }
    
    for (int i = 1; i < planPojo.getDstList().size(); i++) {
      DstPojo dst = planPojo.getDstList().get(i);
      
      args[args.length-1] = dst.getPath();
      DistCp.main(args);
      
      
      FileStatus[] dstFiles = fs.listStatus(new Path(dst.getPath()));
      
      for (FileStatus dstFile: dstFiles) {
        try {
          fs.setOwner(dstFile.getPath(), dst.getOwner(), dst.getGroup());
          
          fs.setPermission(dstFile.getPath(), new FsPermission((short)dst.getPermissions()));
        } catch (IOException e) {
          logger.error("Problem changing permission to owner:" + dst.getOwner() + " group:" +  dst.getGroup() + " on HDFS file " + dstFile.getPath(), e);
          try {
            fs.delete(dstFile.getPath(), false);
          } catch (IOException e1) {
            logger.error("Problem removing file from HDFS");
          }
        }  
      } 
    }
    fs.close();
  }
  
}
