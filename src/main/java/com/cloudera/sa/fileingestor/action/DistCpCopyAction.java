package com.cloudera.sa.fileingestor.action;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.cloudera.sa.fileingestor.action.common.PrivsCommon;
import com.cloudera.sa.fileingestor.model.DstPojo;
import com.cloudera.sa.fileingestor.model.IngestionPlanPojo;

public class DistCpCopyAction {

  public static Logger logger = Logger.getLogger(DistCpCopyAction.class);

  ArrayList<FileStatus> fileStatuses;
  IngestionPlanPojo planPojo;
  FileSystem fs;

  public DistCpCopyAction(ArrayList<FileStatus> fileStatuses,
      IngestionPlanPojo planPojo) {
    this.planPojo = planPojo;
    this.fileStatuses = fileStatuses;
  }

  public void run() throws Exception {
    fs = FileSystem.get(new Configuration());

    String[] args;
    int counter = 1;
    
    if (fs.isDirectory(new Path(planPojo.getDstList().get(0).getPath()))) {
      FileStatus[] fileStatuses = fs.listStatus(new Path(planPojo.getDstList().get(0).getPath()));
      
      
      
      args = new String[fileStatuses.length + 1];
      
      for (FileStatus fileStatus: fileStatuses) {
        args[counter++] = fileStatus.getPath().toString();
      }
    } else {
      args = new String[2];
      args[counter++] = planPojo.getDstList().get(0).getPath();
    }
    
    logger.info("planPojo.getDstList().size(): " + planPojo.getDstList().size());

    
    for (int i = 1; i < planPojo.getDstList().size(); i++) {
      DstPojo dst = planPojo.getDstList().get(i);

      if (dst.isReplaceIfFileExist()) {
        args[0] = "-overwrite";
      } else {
        args[0] = "-update";
      }
      
      if (fileStatuses.size() == 1) {
        args[args.length - 1] = dst.getPath();
      } else {
        args[args.length - 1] = dst.getPath();
      }

      logger.info("Calling DistCp: " + args);

      JobConf job = new JobConf(DistCp.class);
      DistCp distcp = new DistCp(job);
      int res = ToolRunner.run(distcp, args);

      logger.info("Finished DistCp: " + args);
      
        try {
          PrivsCommon.changePervs(fs, dst.getOwner(), dst.getGroup(), dst.getPermissions(), fs.getFileStatus(new Path(dst.getPath())));
        } catch (Exception e) {
          logger.error("Problem changing permission.", e);
        }
    }
    fs.close();
  }
  
}
