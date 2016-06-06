#!/usr/bin/env bash
cd src
hadoop fs -rm -r /user/$HADOOP_USERNAME/output
hadoop com.sun.tools.javac.Main WordCount.java
jar cf wc.jar WordCount*.class
hadoop jar wc.jar WordCount -Dwordcount.case.sensitive=false /user/$HADOOP_USERNAME/input /user/$HADOOP_USERNAME/output -skip /user/$HADOOP_USERNAME/patterns.txt