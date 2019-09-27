#!/usr/bin/env bash

# I was able to send to a pipe prior to creating it: why?
$HOME/kafka_2.12-2.3.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic scrapypipe