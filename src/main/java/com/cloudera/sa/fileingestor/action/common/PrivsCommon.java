package com.cloudera.sa.fileingestor.action.common;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;


public class PrivsCommon {
  
  public static Logger logger = Logger.getLogger(PrivsCommon.class);
  
  public static void changePervs(FileSystem fs, String targetOwner, String targetGroup, String targetPerm, FileStatus fileStatus) throws IOException {
    logger.info("Update File Privs: " + fileStatus.getPath());
    if (targetOwner != null) {
      fs.setOwner(fileStatus.getPath(), targetOwner, targetGroup);
    }
    fs.setPermission(fileStatus.getPath(), new FsPermission(Short.parseShort(targetPerm, 8)));
    if (fileStatus.isDirectory()) {
      for (FileStatus subFileStatus: fs.listStatus(fileStatus.getPath())) {
        changePervs(fs, targetOwner, targetGroup, targetPerm, subFileStatus);
      }
    }
  }
  
  public static void changePervs(FileSystem fs, String targetGroup, String targetPerm, FileStatus fileStatus) throws IOException {
    changePervs(fs, null, targetGroup, targetPerm, fileStatus);
  }
  
  public static void changePervs(FileSystem fs, String targetGroup, FileStatus fileStatus) throws IOException {
    changePervs(fs, null, targetGroup, null, fileStatus);
  }
}
