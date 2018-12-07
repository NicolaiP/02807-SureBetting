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
3) add the path to the jre folder to ~/.bashrc (you can use [nano ~/.bashrc] to edit the file) as JAVA_HOME
    - for me [export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre] 

-install Apache Spark and set up path
1) download Apache Spark 2.3.2 using [wget http://dk.mirrors.quenda.co/apache/spark/spark-2.3.2/spark-2.3.2-bin-hadoop2.7.tgz]
    - visit https://spark.apache.org/downloads.html for other mirrors and different versions
2) make a directory for Apache Spark
    - I created the directory [sudo mkdir /opt/apache-spark]
3) unzip the downloaded file into the directory
    - for me [tar xvzf spark-2.3.2-bin-hadoop2.7.tgz -C /opt/apache-spark]
4) add the path to the unzipped folder to ~/.bashrc as SPARK_HOME
    - for me [export SPARK_HOME=/opt/apache-spark/spark-2.3.2-bin-hadoop2.7]
5) add java/bin and spark/bin path in ~/.bashrc
    - for me [export PATH=$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin]
6) add python version to be used by pyspark to ~/.bashrc [export PYSPARK_PYTHON=python3] 
7) apply changes in ~/.bashrc by runiing the command [source ~/.bashrc]
