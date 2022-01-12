#!/bin/bash

docker-compose -f ./docker-compose.yml up -d

# Verify Confluent Control Center has started within MAX_WAIT seconds
MAX_WAIT=480
CUR_WAIT=0
echo "Waiting up to $MAX_WAIT seconds for Confluent Control Center control-center to start"
docker container logs control-center > /tmp/out.txt 2>&1
while [[ ! $(cat /tmp/out.txt) =~ "Started NetworkTrafficServerConnector" ]]; do
  sleep 10
  docker container logs control-center > /tmp/out.txt 2>&1
  CUR_WAIT=$(( CUR_WAIT+10 ))
  if [[ "$CUR_WAIT" -gt "$MAX_WAIT" ]]; then
    echo -e "\nERROR: The logs in control-center container do not show 'Started NetworkTrafficServerConnector' after $MAX_WAIT seconds. Please troubleshoot with 'docker container ps' and 'docker container logs'.\n"
    exit 1
  fi
done
echo "Control Center has started!"

sleep 10 

# Pre-checks
echo "Verify no records checkpointed for the 2 partitions of the products topic"
docker exec -it sample-gktable_streams_1 cat /tmp/kafka-streams/com.examples.danielpetisme.SampleGKTable/global/.checkpoint
echo "Verify the no record has been restored"
docker logs sample-gktable_streams_1 | grep "Global State Batch restoration ended" | grep "totalRestored=0"
echo "Verify product-events paritions is distributed on only 1 instance"
docker exec -it broker kafka-consumer-groups --bootstrap-server broker:9092 --describe --group com.examples.danielpetisme.SampleGKTable


# Loading data
echo "Sending Products"
docker exec -i broker kafka-console-producer --broker-list broker:9092 --topic products --property "parse.key=true" --property "key.separator=," <<EOF
1,101
2,102
3,103
4,104
5,105
6,106
7,107
8,108
9,109
10,110
EOF

# docker exec broker kafka-console-consumer --bootstrap-server broker:9092 --topic products --from-beginning --property "print.key=true" --property "key.separator=," --max-messages 100

echo "Sending Product Events"
docker exec -i broker kafka-console-producer --broker-list broker:9092 --topic product-events --property "parse.key=true" --property "key.separator=," << EOF
1,1-A
2,2-B
3,3-C
4,4-D
5,5-E
6,6-F
7,7-G
8,8-H
9,9-I
10,10-J
EOF

# docker exec broker kafka-console-consumer --bootstrap-server broker:9092 --topic product-events --from-beginning --property "print.key=true" --property "key.separator=," --max-messages 10

echo "Verify the application output"
docker exec broker kafka-console-consumer --bootstrap-server broker:9092 --topic enriched-product-events --from-beginning --property "print.key=true" --property "key.separator=," --max-messages 10

# Test Global State store persistence
echo "Wait for commit interval ms, default 30s"
sleep 40
echo "Verify 10 records checkpointed for the 2 partitions of the products topic"
docker exec -it sample-gktable_streams_1 cat /tmp/kafka-streams/com.examples.danielpetisme.SampleGKTable/global/.checkpoint
echo "Stopping the application and resuming processing"
docker stop sample-gktable_streams_1
sleep 5
echo "Starting the application and resuming processing"
docker start sample-gktable_streams_1
sleep 5
echo "Verify the checkpoint file is there and up to date"
docker exec -it sample-gktable_streams_1 ls /tmp/kafka-streams/com.examples.danielpetisme.SampleGKTable/global/.checkpoint
echo "Verify the no record has been restored"
docker logs sample-gktable_streams_1 | grep "Global State Batch restoration ended" | grep "totalRestored=0"

# Testing Global State is duplication while product-events is distributed on 2 instances
echo "Starting a new instance"
docker-compose scale streams=2
sleep 5
echo "Verify the checkpoint file is not there"
docker exec -it sample-gktable_streams_2 ls /tmp/kafka-streams/com.examples.danielpetisme.SampleGKTable/global/.checkpoint
echo "Verify 10 records are restored across 2 partitions"
docker logs sample-gktable_streams_2 | grep "Global State Batch restoration ended"
echo "Wait for rebalancing + commit interval ms, default 30s"
sleep 40
echo "Verify 10 records checkpointed for the 2 partitions of the products topic"
docker exec -it sample-gktable_streams_1 cat /tmp/kafka-streams/com.examples.danielpetisme.SampleGKTable/global/.checkpoint
echo "Verify product-events paritions are distributed across the 2 instances"
docker exec -it broker kafka-consumer-groups --bootstrap-server broker:9092 --describe --group com.examples.danielpetisme.SampleGKTable

# Global State is available on all the instances
docker exec -i broker kafka-console-producer --broker-list broker:9092 --topic product-events --property "parse.key=true" --property "key.separator=," << EOF
1,1-AA
2,2-BB
3,3-CC
4,4-DD
5,5-EE
6,6-FF
7,7-GG
8,8-HH
9,9-II
10,10-JJ
EOF

sleep 10

echo "Verify the application output when distributed 2 instances"
docker exec broker kafka-console-consumer --bootstrap-server broker:9092 --topic enriched-product-events --from-beginning --property "print.key=true" --property "key.separator=," --max-messages 20
