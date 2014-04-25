Hadoop FileIngestor
-----------------------------

A simple program that puts files from a local directory into HDFS. The program  takes a configuration file as argument which sets up various environment configuration options described below.

Usage:

Run the JAR file as a Hadoop program with config file argument:

sudo -u hdfs hadoop jar fileingestor.jar FileIngestorSmallFilesSeq.config

In this example, the program file is called   fileingestor.jar. The configuration file argument is FileIngestorSmallFilesSeq.config.


Building the program JAR file from source:

After cloning it to your local computer, use Maven to compile the JAR file by going to the directory where pom.xml file is located and running:

mvn package

This will create the jar file (fileingestor.jar) in the target subdirectory.


Configuration files:

There are 2 types of config files used by the program: one for small files and another for big files (larger than x MB). The difference is small files are aggregated into a single container file (Sequence File or Avro) while big files are not.
