#!/bin/sh

#enable debug loggin
export H2_560_DEBUG=on

./build.sh jar && java -Djava.util.logging.config.file=/home/ubuntu/h2/logging.properties -jar bin/h2-1.4.196.jar  -webAllowOthers
