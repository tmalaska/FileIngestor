Hadoop FileIngestor
-----------------------------

A simple program that puts files from a local directory into HDFS and provide utilities to list and retrieve them. The program  takes a configuration file as argument which sets up various environment configuration options described below.

Usage:

Run the JAR file as a Hadoop program with config file argument:

sudo -u hdfs hadoop jar FileIngestor.jar Ingest FileIngestorSmallFilesSeq.config

In this example, the program file FileIngestor.jar is run with configuration file FileIngestorSmallFilesSeq.config.

To list container file contents, run:

sudo -u hdfs hadoop jar FileIngestor.jar listFilesInAvro /user/hdfs/staging/avro/folder1/1398648788787.avro

In this example, Contents (filenames) of avro container file in HDFS called  1398648788787.avro are displayed.

To extract a file from HDFS container file, run:

sudo -u hdfs hadoop jar FileIngestor.jar getSmallFileInAvroByKey /user/hdfs/staging/avro/folder1/1398648788787.avro '/Activity.CSV' /localhomedir/Activity.CSV

In this example, file '/Activity.CSV' which is in avro container file in HDFS called  1398648788787.avro, is extracted to local (not HDFS) directory called /localhomedir.


Building the program JAR file from source:

After cloning it to your local computer, use Maven to compile the JAR file by going to the directory where pom.xml file is located and running:

mvn package

This will create the jar file (FileIngestor.jar) in the target subdirectory.


Configuration files:

There are 2 types of config files used by the program: one for small files and another for big files (larger than x MB). The difference is small files are aggregated into a single container file (Sequence File or Avro) while big files are not.
