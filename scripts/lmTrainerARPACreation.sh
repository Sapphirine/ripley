#!/bin/bash

# This script is written to execute the speechtools library class LMTrainer
# The LMTrain is inteded to initiate a Hadoop MapReduce job to perform
# n-gram language model training on data in HDFS
# ${1} - input data path (currently only a file) of intermediate, ordered ngram count file relative to the HDFS
# ${2} - output data path (file) relative to the HDFS for ARPA format ngram language model document

export LIBJARS2=/home/hduser/software/berkeleylm-1.1.5/jar/berkeleylm.jar

# Run hadoop tool to go from the intermediate ngram counts document to an ARPA format language model document
#hadoop --config $HADOOP_CONF_DIR jar /home/kyle/workspace/speechtools/target/speechtools-0.0.1-SNAPSHOT.jar ripley.speechtools.LMCompiler.KneserNeyLMCompiler -libjars $LIBJARS2 ${2}/part-r-00000 ${3}

# As an alternative to the LIBJARS methods, with the correct Maven pom file setup the following command may be utilized.
hadoop --config $HADOOP_CONF_DIR jar /home/kyle/workspace/speechtools/target/speechtools-0.0.1-SNAPSHOT-jar-with-dependencies.jar ripley.speechtools.LMCompiler.KneserNeyLMCompiler ${1} ${2}
