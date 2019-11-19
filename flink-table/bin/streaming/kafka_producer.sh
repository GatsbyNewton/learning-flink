#!/usr/bin/env bash

sh data_generator.sh | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic user-behavior