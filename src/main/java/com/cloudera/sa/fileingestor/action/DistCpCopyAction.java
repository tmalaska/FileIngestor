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

    String[] args = new String[2];
    int counter = 0;

    args[counter++] = planPojo.getDstList().get(0).getPath();

    logger
        .info("planPojo.getDstList().size(): " + planPojo.getDstList().size());

    try {
      fs.setOwner(new Path(planPojo.getDstList().get(0).getPath()), planPojo
          .getDstList().get(0).getOwner(), planPojo.getDstList().get(0)
          .getGroup());
      fs.setPermission(
          new Path(planPojo.getDstList().get(0).getPath()),
          new FsPermission(Short.parseShort(planPojo.getDstList().get(0)
              .getPermissions(), 8)));
      fs.setOwner(fileStatuses.get(0).getPath(), planPojo.getDstList().get(0)
          .getOwner(), planPojo.getDstList().get(0).getGroup());
      fs.setPermission(
          fileStatuses.get(0).getPath(),
          new FsPermission(Short.parseShort(planPojo.getDstList().get(0)
              .getPermissions(), 8)));
      logger.info("Changing owner and permissions on: "
          + fileStatuses.get(0).getPath() + " with octal notation: "
          + planPojo.getDstList().get(0).getPermissions());
      changePervs(fs, planPojo.getDstList().get(0).getOwner(), planPojo.getDstList().get(0)
              .getGroup(), planPojo.getDstList().get(0)
              .getPermissions(), fs.getFileStatus(new Path(planPojo.getDstList().get(0).getPath())));
    } catch (Exception e2) {
      logger.error("Problem setting owner and permissions on file.");
    }

    for (int i = 1; i < planPojo.getDstList().size(); i++) {
      DstPojo dst = planPojo.getDstList().get(i);

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
          changePervs(fs, dst.getOwner(), dst.getGroup(), dst.getPermissions(), fs.getFileStatus(new Path(dst.getPath())));
        } catch (Exception e) {
          logger.error("Problem changing permission.", e);
        }
    }
    fs.close();
  }
  private static void changePervs(FileSystem fs, String targetOwner, String targetGroup, String targetPerm, FileStatus fileStatus) throws IOException {
	    logger.info("Update File Privs: " + fileStatus.getPath());
	    fs.setOwner(fileStatus.getPath(), targetOwner, targetGroup);	
	    fs.setPermission(fileStatus.getPath(), new FsPermission(Short.parseShort(targetPerm, 8)));
	    if (fileStatus.isDirectory()) {
	      for (FileStatus subFileStatus: fs.listStatus(fileStatus.getPath())) {
	        changePervs(fs, targetOwner, targetGroup, targetPerm, subFileStatus);
	      }
	    }
	  }
}
