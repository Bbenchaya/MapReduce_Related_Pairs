#!/usr/bin/env bash
cd src
hadoop fs -rm -r /user/$HADOOP_USERNAME/output
hadoop com.sun.tools.javac.Main *.java
jar cf p1.jar *.class Manager.class
hadoop jar p1.jar Manager -Dphase1.case.sensitive=false /Users/bbenchaya/Documents/Programming/DSP/mapreduce-relatedpairs/input/ /Users/bbenchaya/Documents/Programming/DSP/mapreduce-relatedpairs/output