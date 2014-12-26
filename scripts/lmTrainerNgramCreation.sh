#!/bin/bash

# This script is written to execute the speechtools library class LMTrainer
# The LMTrain is inteded to initiate a Hadoop MapReduce job to perform
# n-gram language model training on data in HDFS
# ${1} - input data path (file or directory) relative to the HDFS
# ${2} - output data path (file or directory) relative to the HDFS for ngram count intermediate document

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$(hadoop classpath)

export LIBJARS1=/home/kyle/Software/lucene-4.10.2/analysis/common/lucene-analyzers-common-4.10.2.jar,/home/kyle/Software/lucene-4.10.2/core/lucene-core-4.10.2.jar

# Run hadoop job to create intermediate ngram document file via mapreduce job
hadoop --config $HADOOP_CONF_DIR jar /home/kyle/workspace/speechtools/target/speechtools-0.0.1-SNAPSHOT.jar ripley.speechtools.client.LMTrainer -libjars $LIBJARS1 ${1} ${2}
