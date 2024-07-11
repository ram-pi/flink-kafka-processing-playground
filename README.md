# Flink Processing Kafka Data

## Pre-requisites

### Basic Kafka Cluster with docker-compose

Install docker and docker-compose on your machine and run the following command to start a basic Kafka cluster.

```bash
docker-compose up -d
```

### Flink Cluster with sdkman

Install sdkman on your machine and run the following command to install Flink.

```bash
sdk install flink 1.18.0
start-cluster
taskmanager.sh start
```

```bash
#wget https://dlcdn.apache.org/flink/flink-1.19.0/flink-1.19.0-bin-scala_2.12.tgz
wget https://dlcdn.apache.org/flink/flink-1.19.1/flink-1.19.1-bin-scala_2.12.tgz
#tar xf flink-1.19.0-bin-scala_2.12.tgz
tar -xvf flink-1.19.1-bin-scala_2.12.tgz
cd flink-1.19.1
# Start Flink locally
./bin/start-cluster.sh
./bin/taskmanager.sh start
```

### Create resources

Create a topic in Kafka with the following command.

```bash
kafka-topics --bootstrap-server localhost:9091 --create --topic input --partitions 1
kafka-topics --bootstrap-server localhost:9091 --create --topic output-1 --partitions 4
kafka-topics --bootstrap-server localhost:9091 --create --topic output-2 --partitions 4
kafka-topics --bootstrap-server localhost:9091 --create --topic output-3 --partitions 4
kafka-topics --bootstrap-server localhost:9091 --list
```

### Generate data (with JR)

Install jr on your machine and run the following command to generate data.

```bash
jr template run --embedded '{{random_string 3 10}}' -k '{{integer 1 5}}' -d 30m -f 1s -n 1 --kcat | kafka-console-producer --bootstrap-server localhost:9091 --topic input --property parse.key=true --property key.separator=,
```

#### Optional - Check if messages are produced

```bash
# kcat -b localhost:9091 -G test '^output.*'
kafka-console-consumer --bootstrap-server localhost:9091 --topic output-1 \
  --property --property print.partition=true --property print.offset=true --property print.timestamp=true
kafka-console-consumer --bootstrap-server localhost:9091 --topic output-2 \
  --property --property print.partition=true --property print.offset=true --property print.timestamp=true
```

## Submit the job

You can submit the job with the following command.

```bash
./flink-1.19.1/bin/flink run -c com.github.rampi.MyJob target/MyJob.jar
```

### Increase parallelism

You can increase the parallelism of the job with the following command.

```bash
./flink-1.19.1/bin/flink run -p 4 -c com.github.rampi.MyJob target/MyJob.jar
```

## Check the logs

You can check the logs of Flink with the following command.

```bash
 tail -100f flink-1.19.1/log/*.log
```

It's also possible to do the same with the embedded web interface of Flink.

## Add Task Managers

You can add more Task Managers to the cluster with the following command.

```bash
./flink-1.19.1/bin/taskmanager.sh start
```

## Teardown

You can teardown the cluster with the following command.

```bash
./flink-1.19.1/bin/stop-cluster
./flink-1.19.1/bin/taskmanager.sh stop
rm -fr flink-1.19.1
docker-compose down -v
```