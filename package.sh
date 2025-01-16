#!/bin/bash

set -e

export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64/

./build/sbt package
cd python
python3 setup.py sdist

