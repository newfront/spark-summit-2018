#!/bin/bash

# Example ./generate_proto.sh ../src/main/resources/protobuf/ ../src/main/scala/ ../src/main/resources/protobuf/metrics.proto

which protoc
EXISTS=$?

if [ ${EXISTS} -eq 1 ]; then
  echo "You need to install the protoc library from google. Version 2.6.5..."
  exit $EXISTS
fi

echo "$EXISTS was the result of test for protoc"
if [ "$#" -eq "3" ]; then
  echo "PROTOBUF_SOURCE_DIR=$1 JAVA_OUT=$2 PROTO_FILE=$3"
  protoc -I=$1 --java_out=$2 $3
else
  echo "usage: ./generate_proto.sh /path/to/proto/dir /path/to/java/out /path/to/proto/file"
fi

exit 0
