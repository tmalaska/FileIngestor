package com.cloudera.sa.fileingestor.action;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.cloudera.sa.fileingestor.action.common.PrivsCommon;
import com.cloudera.sa.fileingestor.model.IngestionPlanPojo;

public class UpdatePrivsOfFirstLandingLocalAction {
  
  public static Logger logger = Logger.getLogger(UpdatePrivsOfFirstLandingLocalAction.class);

  IngestionPlanPojo planPojo;
  FileSystem fs;
  
  public UpdatePrivsOfFirstLandingLocalAction(IngestionPlanPojo planPojo) {
    this.planPojo = planPojo;
  }
  
  public void run() {
    try {
      fs = FileSystem.get(new Configuration());

      String[] args = new String[2];
      int counter = 0;

      args[counter++] = planPojo.getDstList().get(0).getPath();

      logger
          .info("planPojo.getDstList().size(): " + planPojo.getDstList().size());

      PrivsCommon.changePervs(fs, planPojo.getDstList().get(0).getOwner(), planPojo.getDstList().get(0)
              .getGroup(), planPojo.getDstList().get(0)
              .getPermissions(), fs.getFileStatus(new Path(planPojo.getDstList().get(0).getPath())));
    } catch (Exception e2) {
      logger.error("Problem setting owner and permissions on file.");
    }
  }
}
