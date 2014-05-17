package com.cloudera.sa.fileingestor.action;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

  public DistCpCopyAction(ArrayList<FileStatus> fileStatuses, IngestionPlanPojo planPojo) {
    this.planPojo = planPojo;
    this.fileStatuses = fileStatuses;
  }

  public void run() throws Exception {
    fs = FileSystem.get(new Configuration());

    String[] argsOverwrite;
    String[] argsNoReplace;
    int counter = 0;

    FileStatus[] fileStatuses = fs.listStatus(new Path(planPojo.getDstList().get(0).getPath()));

    argsNoReplace = new String[fileStatuses.length + 1];

    for (FileStatus fileStatus : fileStatuses) {
      argsNoReplace[counter++] = fileStatus.getPath().toString();
    }
    
    argsOverwrite = new String[3];
    argsOverwrite[0] = "-overwrite";
    argsOverwrite[1] = planPojo.getDstList().get(0).getPath();
    

    logger.info("planPojo.getDstList().size(): " + planPojo.getDstList().size());

    for (int i = 1; i < planPojo.getDstList().size(); i++) {
      DstPojo dst = planPojo.getDstList().get(i);

      String[] argsFinal;
      
      if (dst.isReplaceIfFileExist()) {
        argsFinal = argsOverwrite;
      } else {
        argsFinal =argsNoReplace;
      }

      argsFinal[argsFinal.length - 1] = dst.getPath();
      
      logger.info("Calling DistCp: " + Arrays.toString(argsFinal));

      JobConf job = new JobConf(DistCp.class);
      DistCp distcp = new DistCp(job);
      int res = ToolRunner.run(distcp, argsFinal);

      logger.info("Finished DistCp: " + Arrays.toString(argsFinal));

      Path dstPath = new Path(dst.getPath());
      try {

        for (FileStatus fileStatus : fs.listStatus(dstPath)) {
          if (fileStatus.getPath().getName().startsWith("_distcp_log")) {
            logger.info("Removing Distcp Log");
            fs.delete(fileStatus.getPath(), true);
          }
        }
      } catch (Exception e) {
        logger.error("Problem removing distcp logs.", e);
      }
      try {
        PrivsCommon.changePervs(fs, dst.getOwner(), dst.getGroup(), dst.getPermissions(), fs.getFileStatus(dstPath));
      } catch (Exception e) {
        logger.error("Problem changing permission.", e);
      }
    }
    fs.close();
  }

}
