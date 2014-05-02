package com.cloudera.sa.fileingestor.model;

import java.util.ArrayList;
import java.util.HashMap;

public class IngestionPlanPojo {

  String sourceLocalDir;
  String workingLocalDir;
  String smallContainerFileNameOverride;
  long jobId;
  int numberOfThreads;
  FileIngestionType fileIngestionType;
  HdfsCopyMethodType hdfsCopyMethodType;
  ArrayList<DstPojo> dstList = new ArrayList<DstPojo>();

  public enum FileIngestionType {
    BIG_FILES("BIG_FILE"),
    SEQ_SMALL_FILES("SEQ_SMALL_FILES"),
    AVRO_SMALL_FILES("AVRO_SMALL_FILES"),
    HBASE_SMALL_FILES("HBASE_SMALL_FILES");

    public static HashMap<String, FileIngestionType> map ;
    String value;
    
    FileIngestionType(String value) {
      this.value = value;
    }
    
    public static FileIngestionType getValue(String key) {
      if (map == null) {
        initMapping();
      }
      return map.get(key);
    }

    private static void initMapping() {
      map = new HashMap<String, FileIngestionType>();
      for (FileIngestionType s : values()) {
        map.put(s.toString(), s);
      }
    }
  }

  public enum HdfsCopyMethodType {
    DISTCP("DISTCP"),
    MULTI_THREADED("MULTI_THREADED");

    public static HashMap<String, HdfsCopyMethodType> map ;
    String value;
    
    HdfsCopyMethodType(String value) {
      this.value = value;
    }
    
    public static HdfsCopyMethodType getValue(String key) {
      if (map == null) {
        initMapping();
      }
      return map.get(key);
    }

    private static void initMapping() {
      map = new HashMap<String, HdfsCopyMethodType>();
      for (HdfsCopyMethodType s : values()) {
        map.put(s.toString(), s);
      }
    }
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

  public HdfsCopyMethodType getHdfsCopyMethod() {
    return hdfsCopyMethodType;
  }

  public void setHdfsCopyMethod(HdfsCopyMethodType hdfsCopyMethodType) {
    this.hdfsCopyMethodType = hdfsCopyMethodType;
  }

  public ArrayList<DstPojo> getDstList() {
    return dstList;
  }

  public void setDstList(ArrayList<DstPojo> dstList) {
    this.dstList = dstList;
  }

  public void setSmallContainerFileNameOverride(String smallContainerFileNameOverride) {
    this.smallContainerFileNameOverride = smallContainerFileNameOverride;
    
  }
  
  public String getSmallContainerFileNameOverride() {
    return smallContainerFileNameOverride;
  }
  
  

}
