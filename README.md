# Extract related pairs of words with Map-Reduce

This app was written as part of a Distributed Systems Programming course taken in BGU.
It uses an Amazon EMR cluster to process huge amounts of data (~26GB) of [Google Books Ngrams]
(https://aws.amazon.com/datasets/google-books-ngrams/), which are saved on a public bucket in S3. The app is scalable,
and can process evry input set in said link.

Each record in the input is of the following form:

    <text>  <year> <no. of appearances> <other stuff>

The text part is just 5 units of text from some publication. All none alphabetic characters are discarded, and then
split into words. These words are grouped into pairs, and then 4 Map-Reduce tasks are run in sequence in order to assign
each pair of words a [PMI](https://en.wikipedia.org/wiki/Pointwise_mutual_information) value.

After the output of the tasks is stored in S3, only the output relevant to the years 2000-2009 is downloaded locally in
a different program (CalculateF), which calculates the [F-Measure](https://en.wikipedia.org/wiki/F1_score) of that decade.
The F-Measure is calculated with two test sets:

    /resource/wordsim-pos.txt
    /resource/wordsim-neg.txt

The final F-Measure score is printed on screen.

For further information, please consult the [assignment description](https://www.cs.bgu.ac.il/~dsp162/Assignments/Assignment_2).

## System configuration

1. Using Hadoop 2.7.2
2. Java 1.7.79
3. OS X 10.10.5 Yosemite
4. Replace the JAR `/path/to/hadoop/share/hadoop/tools/lib/httpclient-4.2.5.jar` with version 4.3.3 - version 4.2.5 causes a runtime exception.
5. Required definitions in `~/.bash_profile`:


    export JAVA_HOME=$(/usr/libexec/java_home)
    export HADOOP_HOME=</path/to/hadoop>
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
    export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
    export HADOOP_OPTS=-Djava.library.path=$HADOOP_HOME/lib/native
    export PATH=$JAVA_HOME/bin:$PATH
    export PATH=$HADOOP_HOME/bin:$PATH
    export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
    export HADOOP_USERNAME=<your username>

## How to run this on your computer

TODO!!!


# License

The MIT License (MIT)

Copyright (c) 2016 Asaf Chelouche, Ben Ben-Chaya

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.