Hadoop FileIngestor
-----------------------------

A simple program that puts files from a local directory into HDFS and provide utilities to list and retrieve them. The program  takes a configuration file as argument which sets up various environment configuration options described below. 
It also includes utilities to enable data copying in HDFS between functional IDs (FIDs). This is accomplished using a FID membership to a shared group. It also allows setting permission to copied data.

Usage:

### Run the JAR file as a Hadoop program with config file argument:

* sudo -u hdfs hadoop jar FileIngestor.jar ingest FileIngestorSmallFilesSeq.config

Parameters:
* ingest {configFileLocation}

In this example, the program file FileIngestor.jar is run with configuration file FileIngestorSmallFilesSeq.config.

### To list Wequence container file contents, run:

* sudo -u hdfs hadoop jar FileIngestor.jar listFilesInSeq /user/hdfs/staging/avro/folder1/1398648788787.seq

Parameters:
* listFilesInSeq {pathToSequenceFile}

In this example, Contents (filenames) of avro container file in HDFS called  1398648788787.avro are displayed.

### To extract a file from HDFS Sequence container file, run:

* sudo -u hdfs hadoop jar FileIngestor.jar getSmallFileInSeqByKey /user/hdfs/staging/seq/folder1/1398648788787.seq '/Activity.CSV' /localhomedir/Activity.CSV

Parameters:
* getSmallFileInSeqByKey {pathToSequenceFile} {nameOfFileToGet} {outputFilePath}

### To extract all files from HDFS Sequence container file, run:

sudo -u hdfs hadoop jar FileIngestor.jar explodSmallFileSeqToLocal /user/hdfs/staging/seq/folder1/1398648788787.seq localhomedir

Parameters:
* explodSmallFileSeqToLocal {pathToSequenceFile} {outputFilePath}

### To list avro container file contents, run:

* sudo -u hdfs hadoop jar FileIngestor.jar listFilesInAvro /user/hdfs/staging/seq/folder1/1398648788787.avro

Parameters:
* listFilesInAvro {pathToAvroFile}

In this example, Contents (filenames) of avro container file in HDFS called  1398648788787.avro are displayed.

### To extract a file from HDFS avro container file, run:

* sudo -u hdfs hadoop jar FileIngestor.jar getSmallFileInAvroByKey /user/hdfs/staging/avro/folder1/1398648788787.avro '/Activity.CSV' /localhomedir/Activity.CSV

Parameters:
* getSmallFileInSeqByKey {pathToAvroFile} {nameOfFileToGet} {outputFilePath}

In this example, file '/Activity.CSV~1389' which is in avro container file in HDFS called  1398648788787.avro, is extracted to local (not HDFS) directory called /localhomedir.

### To extract all files from HDFS Avro container file, run:

sudo -u hdfs hadoop jar FileIngestor.jar explodSmallFileAvroToLocal /user/hdfs/staging/avro/folder1/1398648788787.seq localhomedir

Parameters:
* explodSmallFileAvroToLocal {pathToSequenceFile} {outputFilePath}


### To copy data into staging directory in HDFS:

* sudo -u dataproducer hadoop jar HdfsCopy.jar copyToStaging /hdfscopy/source /hdfscopy/destination datastaging

Parameters:
* copyFromStaging {srcDir} {targetDir} {targetGroup} {targetPrem}

In this example, the id name dataproducer will copy contents in source directory into the destination. Then it will change the group owner of the destination contents (files and directories) to datastaging with appropridate permissions.

### To copy data from a staging directory in HDFS:

* sudo -u dataconsumer hadoop jar HdfsCopy.jar copyFromStaging /hdfscopy/destination /user/dataconsumer/destination marketing 750

Parameters:
copyToStaging {srcDir} {targetDir} {targetGroup}

In this example, the FID name dataconsumer will copy contents from source staging directory into the target directory. Then it will change the group owner of the destination contents (files and directories) to marketing with read  (750 in octal notation) permissions.


Building the program JAR file from source:

After cloning it to your local computer, use Maven to compile the JAR file by going to the directory where pom.xml file is located and running:

mvn package

This will create the jar file (FileIngestor.jar) in the target subdirectory.


Configuration files:

There are 2 types of config files used by the program: one for small files and another for big files (larger than x MB). The difference is small files are aggregated into a single container file (Sequence File or Avro) while big files are not.
