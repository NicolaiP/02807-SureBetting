# 02807-SureBetting
Computational Tools For Data Science Project by Joakim Edin, Aske Bluhme Klok, Nicolai Pedersen


Requirements:
Python 3 (Python 3.7)
Java SDK 8 (java version "1.8.0_161")
Spark 2 (Apache Spark 2.3.2)

[In brackets is the specific version used for developing the software]

Linux install:

-install java and set up path
1) sudo apt install openjdk-8-jre-headless (unless you have already installed java)
    -check with [java -version] if you have java installed
2) locate java file path by running "readlink -f $(which java)"
    -This returned the path "/usr/lib/jvm/java-8-openjdk-amd64/jre/bin" for med
3) add the path to the jre folder to ~/.bashrc as JAVA_HOME
    - for me [export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre] 
4) add bin folder to path in ~/.bashrc
- for me [export PATH=$PATH:$JAVA_HOME/bin]

-install Apache Spark and set up path
1) download Apache Spark 2.3.2 using [wget http://dk.mirrors.quenda.co/apache/spark/spark-2.3.2/spark-2.3.2-bin-hadoop2.7.tgz]
    - visit https://spark.apache.org/downloads.html for other mirrors and different versions
2)
