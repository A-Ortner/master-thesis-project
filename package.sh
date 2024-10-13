#!/bin/bash

set -e

export JAVA_HOME=/usr/lib/jvm/default-java/

./build/sbt package
cd python
python3 setup.py sdist
