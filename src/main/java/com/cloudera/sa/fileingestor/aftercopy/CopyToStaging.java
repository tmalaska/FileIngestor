package com.cloudera.sa.fileingestor.aftercopy;


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

/**
 * HDFS Copy Utility - Copy from Source to Staging Directory
 *
 */

public class CopyToStaging {


  public static Logger logger = Logger.getLogger(CopyToStaging.class);
  
  
	public static void main( String[] args ) throws IOException
	{

	  logger.info( "Starting CopyToStaging." );	
		FileSystem fs = FileSystem.get(new Configuration());

		String srcDir = args[0];
		String targetDir = args[1];
		String targetGroup = args[2];
		
		String[] distCpArgs = new String[5];
		distCpArgs[0] = "-delete";
		distCpArgs[1] = "-overwrite";
		distCpArgs[2] = "-p";
		distCpArgs[3] = srcDir;
		distCpArgs[4] = targetDir;
		
		logger.info("Calling DistCp");

		JobConf job = new JobConf(DistCp.class);
		DistCp distcp = new DistCp(job);
		try {
			int res = ToolRunner.run(distcp, distCpArgs );
			changePervs(fs, targetGroup, fs.getFileStatus(new Path(targetDir)));
		} catch (Exception e) {
			logger.error(e);
		}
		logger.info("Finished DistCp");
	}

  private static void changePervs(FileSystem fs, String targetGroup, FileStatus fileStatus) throws IOException {
    logger.info("Update File Privs: " + fileStatus.getPath());
    fs.setOwner(fileStatus.getPath(), null, targetGroup);	
    fs.setPermission(fileStatus.getPath(), new FsPermission(Short.parseShort("750", 8)));
    if (fileStatus.isDirectory()) {
      for (FileStatus subFileStatus: fs.listStatus(fileStatus.getPath())) {
        changePervs(fs, targetGroup, subFileStatus);
      }
    }
  }
}
