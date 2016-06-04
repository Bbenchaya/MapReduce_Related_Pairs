#!/usr/bin/env bash
hadoop fs -rm -r /user/$HADOOP_USERNAME/wordcount/output
hadoop com.sun.tools.javac.Main WordCount.java
jar cf wc.jar WordCount*.class
hadoop jar wc.jar WordCount /user/$HADOOP_USERNAME/wordcount/input /user/$HADOOP_USERNAME/wordcount/output
