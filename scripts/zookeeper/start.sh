#!/usr/bin/env bash
SOURCE=${BASH_SOURCE[0]}
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )/zookeeper.conf"
zkServer start-foreground $DIR


#zookeeper-server-start.sh /usr/local/Cellar/kafka/0.8.2.1/libexec/config/zookeeper.properties