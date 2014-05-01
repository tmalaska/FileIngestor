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

  Logger logger = Logger.getLogger(DistCpCopyAction.class);

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

      RemoteIterator<LocatedFileStatus> dstFiles = fs.listFiles(
          new Path(dst.getPath()), true);

      while (dstFiles.hasNext())
        try {
          LocatedFileStatus dstLocFileStatus = dstFiles.next();

          fs.setOwner(dstLocFileStatus.getPath(), dst.getOwner(),
              dst.getGroup());
          fs.setPermission(dstLocFileStatus.getPath(),
              new FsPermission(Short.parseShort(dst.getPermissions(), 8)));

          fs.setOwner(dstLocFileStatus.getPath().getParent(), dst.getOwner(),
              dst.getGroup());
          fs.setPermission(dstLocFileStatus.getPath().getParent(),
              new FsPermission(Short.parseShort(dst.getPermissions(), 8)));

          logger.info("Changing owner and permissions on: "
              + dstLocFileStatus.getPath() + " with octal notation: "
              + dst.getPermissions());

        } catch (Exception e) {
          logger.error(
              "Problem changing permission to owner:" + dst.getOwner()
                  + " group:" + dst.getGroup() + " on HDFS file "
                  + dstFiles.toString(), e);
        }
    }
    fs.close();
  }
}
