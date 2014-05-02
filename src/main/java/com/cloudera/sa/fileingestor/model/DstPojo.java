package com.cloudera.sa.fileingestor.model;

public class DstPojo {
  String name;
  String path;
  String owner;
  String group;
  String permissions;
  boolean createDir;
  boolean replaceIfFileExist;
  
  
  
  public DstPojo(String name) {
    this.name = name;
    owner = "hdfs";
    group = "supergroup";
    path = "/tmp";
    permissions = "777";
  };
  
  public DstPojo(String name, String path, String owner, String group, String permissions, boolean createDir, boolean replaceIfFileExist) {
    super();
    this.name = name;
    this.path = path;
    this.owner = owner;
    this.group = group;
    this.permissions = permissions;
    this.createDir = createDir;
    this.replaceIfFileExist = replaceIfFileExist;
  }
  
  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }
  public String getPath() {
    return path;
  }
  public void setPath(String path) {
    this.path = path;
  }
  public String getOwner() {
    return owner;
  }
  public void setOwner(String owner) {
    this.owner = owner;
  }
  public String getGroup() {
    return group;
  }
  public void setGroup(String group) {
    this.group = group;
  }
  public String getPermissions() {
    return permissions;
  }
  public void setPermissions(String permissions) {
    this.permissions = permissions;
  }
  public boolean isCreateDir() {
    return createDir;
  }
  public void setCreateDir(boolean createDir) {
    this.createDir = createDir;
  }
  public boolean isReplaceIfFileExist() {
    return replaceIfFileExist;
  }
  public void setReplaceIfFileExist(boolean replaceIfFileExist) {
    this.replaceIfFileExist = replaceIfFileExist;
  }
  
  public String toString() {
    return name + "|" + 
        path + "|" + 
        owner + "|" + 
        group + "|" + 
        permissions + "|" + 
        createDir + "|" + 
        replaceIfFileExist + "|";
  }

  
}
