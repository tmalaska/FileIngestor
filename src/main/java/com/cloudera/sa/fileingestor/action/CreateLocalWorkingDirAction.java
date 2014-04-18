package com.cloudera.sa.fileingestor.action;

import java.io.File;

import org.apache.log4j.Logger;

import com.cloudera.sa.fileingestor.model.IngestionPlanPojo;

public class CreateLocalWorkingDirAction {
  
  Logger logger = Logger.getLogger(CreateLocalWorkingDirAction.class);
  
  IngestionPlanPojo planPojo;
  
  public CreateLocalWorkingDirAction(IngestionPlanPojo planPojo) {
    this.planPojo = planPojo;
  }
  
  public void run() {
    File workingDir = new File(planPojo.getWorkingLocalDir());
    
    if (!workingDir.exists()) {
      if (!workingDir.mkdirs()) {
        throw new RuntimeException("Unable to create working directory '" + workingDir + "'");
      }
    }
    
    boolean wasDirCreated = false;
    long jobId = -1;
    int counter = 0;
    
    while (wasDirCreated == false) {
      
      if (counter++ > 5) {
        throw new RuntimeException("Failed to create job directory after 5 tries");
      }
      
      jobId = System.currentTimeMillis();
      File jobDir = new File(workingDir + "/" + jobId);
      wasDirCreated = jobDir.mkdirs();
      
      if (wasDirCreated) {
        logger.info("create job directory: " + jobDir);
      } else {
        logger.info("failed to job directory: '" + jobDir + "' on attempt:" + counter);
      }
    }
    
    planPojo.setJobId(jobId);
  }
}
