#!/bin/bash

kafka_home="kafka"
broker_home="properties/servers"
properties_home="properties"
num_brokers=6


$kafka_home/bin/kafka-server-stop.sh  >logs/servers_ending.out 2>&1 &
sleep 100
$kafka_home/bin/zookeeper-server-stop.sh  >logs/zookeeper_ending.out 2>&1 &
printf "Ended Kafka Service gracefully\n"