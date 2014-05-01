package com.cloudera.sa.fileingestor.action;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;

import com.cloudera.sa.fileingestor.model.IngestionPlanPojo;

public class LittleFileIntoAvroCopyFromLocalAction extends AbstractIngestToHDFSAction {

  FileSystem fs;
  LittleFileIntoAvroCopyFromLocalAction thisObj;
  IngestionPlanPojo planPojo;

  public LittleFileIntoAvroCopyFromLocalAction(IngestionPlanPojo planPojo) {
    super(planPojo);
    thisObj = this;
    this.planPojo = planPojo;
  }

  @Override
  protected void ingestDataToHdfsDir() throws IOException {

    fs = FileSystem.get(new Configuration());

    File processingDirFile = new File(processingDir);
    Text key = new Text();

    logger.info("Files: " + processingDirFile.listFiles().length);

    Path dstAvroFilePath = new Path(destination.getPath() + "/" + planPojo.getJobId() + ".avro");

    logger.info("Creating Avro file to store small files at path " + dstAvroFilePath);

    OutputStream os = fs.create(dstAvroFilePath);

    final String FIELD_FILENAME = "filename";
    final String FIELD_CONTENTS = "contents";
    final String SCHEMA_JSON = "{\"type\": \"record\", \"name\": \"SmallFilesAvro\", " + "\"fields\": [" + "{\"name\":\"" + FIELD_FILENAME
        + "\", \"type\":\"string\"}," + "{\"name\":\"" + FIELD_CONTENTS + "\", \"type\":\"bytes\"}]}";
    final Schema SCHEMA = Schema.parse(SCHEMA_JSON);

    DataFileWriter<Object> writer = new DataFileWriter<Object>(new GenericDatumWriter<Object>()).setSyncInterval(100);
    writer.setCodec(CodecFactory.snappyCodec());
    writer.create(SCHEMA, os);

    for (File file : FileUtils.listFiles(processingDirFile, null, true)) {

      String filePath = file.getPath().substring(processingDirFile.getPath().length());

      if (file.length() < 1024 * 1024 * 5) {
        byte content[] = FileUtils.readFileToByteArray(file);
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put(FIELD_FILENAME, filePath  + "~" + file.length());
        record.put(FIELD_CONTENTS, ByteBuffer.wrap(content));
        writer.append(record);

        key.set(filePath  + "~" + file.length());
        logger.info("Write Avro Record: " + key);

      } else {
        logger.error("file: " + filePath + " was too large at " + file.length() + " bytes");
      }
    }
    IOUtils.cleanup(null, writer);
    copiedFiles.add(fs.getFileStatus(dstAvroFilePath));
    IOUtils.cleanup(null, os);
  }

}
