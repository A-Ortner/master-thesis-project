# Apache Spark SQL with optimizations for acyclic aggregate queries

## Building

Run `./package.sh`. This will compile the scala sources using the sbt binary provided here and generate the PySpark
python package.

## Overview of the changes

The logical rewriting is implemented in `RewriteJoinsAsSemijoins.scala`. 