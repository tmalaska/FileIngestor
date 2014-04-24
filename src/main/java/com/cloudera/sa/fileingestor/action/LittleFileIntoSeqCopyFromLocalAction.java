package com.cloudera.sa.fileingestor.action;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;

import com.cloudera.sa.fileingestor.model.DstPojo;
import com.cloudera.sa.fileingestor.model.IngestionPlanPojo;


public class LittleFileIntoSeqCopyFromLocalAction extends AbstractIngestToHDFSAction{

  FileSystem fs;
  LittleFileIntoSeqCopyFromLocalAction thisObj;
  IngestionPlanPojo planPojo;

  public LittleFileIntoSeqCopyFromLocalAction(IngestionPlanPojo planPojo) {
    super(planPojo);
    thisObj = this;
    this.planPojo = planPojo;
  }

  @Override
  protected void ingestDataToHdfsDir() throws IOException {
    
    
    
    Configuration config = new Configuration();
    fs = FileSystem.get(new Configuration());
    
    Path dstSeqFilePath = new Path(distination.getPath() + "/" + planPojo.getJobId() + ".seq");
    
    logger.info("Creating Seq file to store small files at path " + dstSeqFilePath);
    
    SequenceFile.Writer out = 
        SequenceFile.createWriter(fs, config, dstSeqFilePath,
                                  Text.class,
                                  Text.class,
                                  SequenceFile.CompressionType.BLOCK, 
                                  new SnappyCodec(), 
                                  null);
    
    File processingDirFile = new File(processingDir);
    Text key = new Text();
    Text value = new Text();
    
    logger.info("Files: " + FileUtils.listFiles(processingDirFile, null, true).size());
    
    for (File file: FileUtils.listFiles(processingDirFile, null, true)) {
      
      String filePath = file.getPath().substring(processingDirFile.getPath().length());
      
      if (file.length() < 1024 * 1024 * 5) {
        byte[] byteArray = new byte[(int)file.length()];
        BufferedInputStream input = new BufferedInputStream(new FileInputStream(file));
        
        int byteRead = input.read(byteArray);
        
        key.set(filePath);
        
        byte[] readByteArray = new byte[byteRead];
        System.arraycopy(byteArray, 0, readByteArray, 0, byteRead);
        value.set(readByteArray);
        
        out.append(key, value);
        
        logger.info("Write Seq Record: " + key);
        
        input.close();
        
      } else {
        logger.error("file: " + filePath + " was to large at " + file.length() + " bytes");  
      }
    }
    out.close();
    
    copiedFiles.add(fs.getFileStatus(dstSeqFilePath));

    fs.close();
  }
    
}
