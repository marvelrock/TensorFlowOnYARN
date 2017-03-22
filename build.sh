#!/usr/bin/env bash

PATCH=../tensorflow-1.0.0.patch
cd tensorflow

git apply ${PATCH}

if [ "$1" == "1" ];
  then echo '\n' | ./configure;
  else ./configure;
fi

bazel build -c opt //tensorflow/core/distributed_runtime/rpc:libbridge.so
git apply --reverse ${PATCH}

cd ..
cp tensorflow/bazel-bin/tensorflow/core/distributed_runtime/rpc/libbridge.so tensorflow-yarn-app/samples/between-graph

mvn install



