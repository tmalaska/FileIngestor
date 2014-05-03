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

import com.cloudera.sa.fileingestor.action.common.PrivsCommon;

/**
 * HDFS Copy Utility - Copy from Staging to Destination Directory
 *
 */

public class CopyFromStaging {

	public static Logger logger = Logger.getLogger(CopyFromStaging.class);
	
	public static void main( String[] args ) throws IOException
	{

	  if (args.length == 0) {
      System.out.println("copyFromStaging {srcDir} {targetDir} {targetGroup} {targetPrem}");
      return;
    }
	  
		Logger logger = Logger.getLogger(CopyFromStaging.class);
		logger.info( "Starting CopyFromStaging." );
		
		FileSystem fs = FileSystem.get(new Configuration());

		String srcDir = args[0];
		String targetDir = args[1];
		String targetGroup = args[2];
		String targetPerm = args[3];
		
		String[] distCpArgs = new String[3];
		distCpArgs[0] = "-update";		
		distCpArgs[1] = srcDir;
		distCpArgs[2] = targetDir;
		
		logger.info("Calling DistCp");

		JobConf job = new JobConf(DistCp.class);
		DistCp distcp = new DistCp(job);
		try {
			int res = ToolRunner.run(distcp, distCpArgs );
			PrivsCommon.changePervs(fs, targetGroup, targetPerm, fs.getFileStatus(new Path(targetDir)));			
		} catch (Exception e) {
			logger.error(e);
		}
		logger.info("Finished DistCp");
	}
	
}
