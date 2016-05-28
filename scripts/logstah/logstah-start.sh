#!/usr/bin/env bash

current_dir=$(pwd)
echo $current_dir
file_dir=$(dirname "${BASH_SOURCE[0]}")
cd $file_dir
logstash -f logstash.conf

