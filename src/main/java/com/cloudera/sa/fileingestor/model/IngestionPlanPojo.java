package com.cloudera.sa.fileingestor.model;

import java.util.ArrayList;

public class IngestionPlanPojo {

  String sourceLocalDir;
  String workingLocalDir;
  long jobId;
  int numberOfThreads;
  FileIngestionType fileIngestionType;
  HdfsCopyMethod hdfsCopyMethod;
  ArrayList<DstPojo> dstList = new ArrayList<DstPojo>();
  
  public class FileIngestionType {
    public static final int BIG_FILES = 1;
    public static final int SEQ_SMALL_FILES = 2;
    public static final int AVRO_SMALL_FILES = 3;
    public static final int HBASE_SMALL_FILES = 4;
  }

  public class HdfsCopyMethod {
    public static final int DISTCP = 1;
    public static final int MULTI_THREADED = 2;
  }
  
  
  
  public int getNumberOfThreads() {
    return numberOfThreads;
  }

  public void setNumberOfThreads(int numberOfThreads) {
    this.numberOfThreads = numberOfThreads;
  }

  public long getJobId() {
    return jobId;
  }

  public void setJobId(long jobId) {
    this.jobId = jobId;
  }

  public String getSourceLocalDir() {
    return sourceLocalDir;
  }

  public void setSourceLocalDir(String sourceLocalDir) {
    this.sourceLocalDir = sourceLocalDir;
  }

  public String getWorkingLocalDir() {
    return workingLocalDir;
  }

  public void setWorkingLocalDir(String workingLocalDir) {
    this.workingLocalDir = workingLocalDir;
  }

  public FileIngestionType getFileIngestionType() {
    return fileIngestionType;
  }

  public void setFileIngestionType(FileIngestionType fileIngestionType) {
    this.fileIngestionType = fileIngestionType;
  }

  public HdfsCopyMethod getHdfsCopyMethod() {
    return hdfsCopyMethod;
  }

  public void setHdfsCopyMethod(HdfsCopyMethod hdfsCopyMethod) {
    this.hdfsCopyMethod = hdfsCopyMethod;
  }

  public ArrayList<DstPojo> getDstList() {
    return dstList;
  }

  public void setDstList(ArrayList<DstPojo> dstList) {
    this.dstList = dstList;
  }
  
  
  
}
