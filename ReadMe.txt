README FILE

Hardware Requirements-

Operating System- ubuntu
Hard Drive Minimum- Minimum 32 GB; Recommended 64 GB or more
RAM	Minimum- Recommended 4 GB or more
Processor- Intel i3 or i5

Software Requirements-

• Apache Zookeeper
• Kafka
• Spark
• Hadoop
• Python Libraries

Pre-Requisites

Hadoop Installation
• Setup Spark Cluster (Spark Installation)
• Install Kafka & Zookeeper (Kakfa Installation) (Zookeeper Installation)
• Python 3

How to execute the project-

Step 1:   Start Zookeeper

bin/zookeeper-server-start.sh config/zookeeper.properties

Step 2:  Start Apache Kafka

JMX_PORT=8004 bin/kafka-server-start.sh config/server.properties

Step 3:   Create topic for kafka (do it only only once)

bin/kafka-topics.sh --create --zookeeper --partitions 1 --topic whether localhost:9092-- replication-factor 1

bin/kafka-topics.sh --create --zookeeper --partitions 1 --topic testing localhost:9092-- replication-factor 1

Step 4:  start all nodes of spark
Sbin/start-all.sh

Step 5:   Execute streaming code
Downloads/project/kafkac.p

Step 6:   Job submission on spark for prediction
/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0- 10_2.12:3.1.2 ./hd/kafkaconsumer.py
