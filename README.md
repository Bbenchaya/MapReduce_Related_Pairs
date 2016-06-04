# Extract related pairs of words with Map-Reduce

For further information, please consult the [assignment description](https://www.cs.bgu.ac.il/~dsp162/Assignments/Assignment_2).

Using Hadoop 2.7.2 and Java 1.8, on OS X 10.10 Yosemite.

## Setup
Required definitions in `~/.bash_profile`:

    export JAVA_HOME=$(/usr/libexec/java_home)
    export HADOOP_HOME=/Users/asafchelouche/programming/hadoop-2.7.2
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
    export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
    export HADOOP_OPTS=-Djava.library.path=$HADOOP_HOME/lib/native
    export PATH=$JAVA_HOME/bin:$PATH
    export PATH=$HADOOP_HOME/bin:$PATH
    export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
    export HADOOP_USERNAME=<your username>

## Running this
`cd` into `<project directory>/src` and then run:

`$ ./run.sh`

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