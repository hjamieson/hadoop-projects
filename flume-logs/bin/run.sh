#!/bin/bash
# runs the flume agent

nohup flume-ng agent -Xmx256m -f logtrap.conf -n a1 --classpath dbahadoop-flume-0.1.jar 2>&1 &
