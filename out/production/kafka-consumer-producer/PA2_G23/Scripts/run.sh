#!/bin/bash

kafka_home="kafka"
broker_home="properties/servers"
properties_home="properties"
num_brokers=6

$kafka_home/bin/zookeeper-server-start.sh $properties_home/zookeeper.properties >logs/zookeeper.out 2>&1 &

for i in {0..5}
do
  sleep 100
    $kafka_home/bin/kafka-server-start.sh $broker_home/server$i.properties >logs/server$i.out 2>&1 &
    printf "Started kafka broker: $i\n"
done

sleep 100
$kafka_home/bin/kafka-topics.sh --create --topic Sensor --replication-factor 3 --partitions 6 --config min.insync.replicas=2 --bootstrap-server localhost:9092 2>&1 &
printf "Created Sensor topic\n"