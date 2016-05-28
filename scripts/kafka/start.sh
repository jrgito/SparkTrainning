#!/usr/bin/env bash
file_dir=$(dirname "${BASH_SOURCE[0]}")
kafka-server-start.sh "$file_dir/kafka.conf"