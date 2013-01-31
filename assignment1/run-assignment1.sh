#!/bin/sh

etc/hadoop-cluster.sh edu.umd.cloud9.example.simple.DemoWordCountModified \
  -input bible+shakes.nopunc.gz -output cimbriano -numReducers 5