package com.cloudera.sa.fileingestor.plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.cloudera.sa.fileingestor.model.DstPojo;
import com.cloudera.sa.fileingestor.model.IngestionPlanPojo;
import com.cloudera.sa.fileingestor.model.IngestionPlanPojo.FileIngestionType;

public class IngestionPlanFactory {
  
  static Logger logger = Logger.getLogger(IngestionPlanFactory.class);
  
  public static String LOCAL_SCR_DIR = "local.scr.dir";
  public static String LOCAL_WORKING_DIR = "local.working.dir";
  public static String FILE_INGESTION_TYPE = "file.ingestion.type";
  public static String NUMBER_OF_THREADS = "number.of.threads";
  public static String HDFS_COPY_TYPE = "hdfs.copy.type";
  public static String DST_PREFIX = "dst.";
  public static String DST_PATH = "path";
  public static String DST_OWNER = "owner";
  public static String DST_GROUP = "group";
  public static String DST_PERMISSION = "permission";
  public static String DST_CREATE_DIR = "createDir";
  public static String DST_REPLACE_EXISTING_FILE = "replaceExistingFile";
  public static String SMALL_CONTAINER_FILE_NAME_OVERRIDE = "small.container.file.name.override";
  
  
  public static IngestionPlanPojo getInstance(Properties p) {
    IngestionPlanPojo result = new IngestionPlanPojo();
    
    String localScrDir = p.getProperty(LOCAL_SCR_DIR);
    if (localScrDir == null || localScrDir.isEmpty()) {
      logger.error(LOCAL_SCR_DIR + " has no value");
      throw new RuntimeException(LOCAL_SCR_DIR + " has no value");
    }
    logger.info(LOCAL_SCR_DIR + " = " + localScrDir);
    result.setSourceLocalDir(localScrDir);
    
    String localWorkingDir = p.getProperty(LOCAL_WORKING_DIR);
    if (localWorkingDir == null || localWorkingDir.isEmpty()) {
      logger.error(LOCAL_WORKING_DIR + " has no value");
      throw new RuntimeException(LOCAL_WORKING_DIR + " has no value");
    }
    logger.info(LOCAL_WORKING_DIR + " = " + localWorkingDir);    
    result.setWorkingLocalDir(localWorkingDir);
    
    result.setFileIngestionType(IngestionPlanPojo.FileIngestionType.getValue(p.getProperty(FILE_INGESTION_TYPE)));
    if (result.getFileIngestionType() == null) {
      logger.error(FILE_INGESTION_TYPE + " has no value");
      throw new RuntimeException(FILE_INGESTION_TYPE + " has no value");
    }
    logger.info(FILE_INGESTION_TYPE + " = " + result.getFileIngestionType());    
    
    result.setHdfsCopyMethod(IngestionPlanPojo.HdfsCopyMethodType.getValue(p.getProperty(HDFS_COPY_TYPE)));
    if (result.getHdfsCopyMethod() == null) {
      logger.error(HDFS_COPY_TYPE + " has no value");
      throw new RuntimeException(HDFS_COPY_TYPE + " has no value");
    }
    logger.info(HDFS_COPY_TYPE + " = " + result.getHdfsCopyMethod());
    
    String numberOfThreads = p.getProperty(NUMBER_OF_THREADS);
    int numOfThreads = 1;
    if (numberOfThreads != null && !numberOfThreads.isEmpty()) {
      try {
        numOfThreads = Integer.parseInt(numberOfThreads);
      } catch (Exception e) {
        logger.error(NUMBER_OF_THREADS + " not a valid number '" + numberOfThreads + "'");
        throw new RuntimeException(NUMBER_OF_THREADS + " not a valid number '" + numberOfThreads + "'");
      }
    }
    logger.info(NUMBER_OF_THREADS + " = " + numberOfThreads); 
    result.setNumberOfThreads(numOfThreads);
    
    String smallContainerFileNameOverride = p.getProperty(SMALL_CONTAINER_FILE_NAME_OVERRIDE);
    logger.info(SMALL_CONTAINER_FILE_NAME_OVERRIDE + " = " + smallContainerFileNameOverride);
    result.setSmallContainerFileNameOverride(smallContainerFileNameOverride);
    
    HashMap<String, DstPojo> dstMap = new HashMap<String, DstPojo>();
    
    for (Entry<Object, Object> entry: p.entrySet()) {
      String key = entry.getKey().toString();
      if (key.startsWith(DST_PREFIX)) {
        String[] parts = key.split("\\.");
        if (parts.length == 3) {
          DstPojo dst = dstMap.get(parts[1]);
          if (dst == null) {
            dst = new DstPojo(parts[1]);
            dstMap.put(parts[1], dst);
          }
          
          try {
            if (parts[2].equals(DST_PATH)){
              dst.setPath(entry.getValue().toString());
              
              logger.info("adding path '" + dst.getPath() + "' to dst " + parts[1] + " to ");
              
            } else if (parts[2].equals(DST_OWNER)){
              dst.setOwner(entry.getValue().toString());
            } else if (parts[2].equals(DST_GROUP)){
              dst.setGroup(entry.getValue().toString());
            } else if (parts[2].equals(DST_PERMISSION)){
              dst.setPermissions(entry.getValue().toString());
            } else if (parts[2].equals(DST_CREATE_DIR)){
              dst.setCreateDir(Boolean.parseBoolean(entry.getValue().toString()));
            } else if (parts[2].equals(DST_REPLACE_EXISTING_FILE)){
              dst.setReplaceIfFileExist(Boolean.parseBoolean(entry.getValue().toString()));
            } 
          } catch (Exception e) {
            throw new RuntimeException(key + " value is malformed. '" + entry.getValue() + "' is not a valid value.");
          }
        } else {
          throw new RuntimeException(key + " is malformed ");
        }
      }
    }
    
    ArrayList<DstPojo> dstList = new ArrayList<DstPojo>();
    
    if (dstMap.size() == 0) {
      throw new RuntimeException("no distinations defined");
    }
    
    
    
    for (Entry<String, DstPojo> entry: dstMap.entrySet()) {
      logger.info("adding destriniation: " + entry.getKey() + " " + entry.getValue());
      dstList.add(entry.getValue());
    }
    
    //put any replace existing files first
    if (dstList.get(0).isReplaceIfFileExist() == false) {
      for (int i = 1; i < dstList.size(); i++) {
        DstPojo pojo = dstList.get(i);
        if (pojo.isReplaceIfFileExist() == true) {
          DstPojo tmp = dstList.get(0);
          dstList.set(0, pojo);
          dstList.set(i, tmp);
          break;
        }
      }
    }
    
    result.setDstList(dstList);
    
    return result;
  }
}
