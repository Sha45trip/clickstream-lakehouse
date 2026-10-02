#!/usr/bin/env bash
set -euo pipefail
role="${1:?usage: entrypoint.sh <namenode|datanode|resourcemanager|nodemanager|client>}"
shift || true

case "$role" in
  namenode)
    if [ ! -f /data/hdfs/name/current/VERSION ]; then
      echo ">> Formatting NameNode (first start)"
      hdfs namenode -format -force -nonInteractive
    fi
    exec hdfs namenode ;;
  datanode)         exec hdfs datanode ;;
  resourcemanager)  exec yarn resourcemanager ;;
  nodemanager)      exec yarn nodemanager ;;
  client)           exec sleep infinity ;;
  *)                exec "$role" "$@" ;;
esac
