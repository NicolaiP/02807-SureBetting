a=$SPARK_HOME"/conf/;"$SPARK_HOME"/jars/*"
java -cp $a -Xmx1g org.apache.spark.deploy.SparkSubmit $1

read