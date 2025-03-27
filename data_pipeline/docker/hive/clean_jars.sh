#!/bin/bash
echo "Cleaning up conflicting JARs..."

find /opt/hive/lib/ -name "slf4j-log4j12-*.jar" -exec rm -v {} \;
find /opt/hive/lib/ -name "log4j-slf4j-impl-*.jar" -exec rm -v {} \;
find /opt/hive/lib/ -name "guava-*.jar" -exec rm -v {} \;
find /opt/hadoop/share/hadoop/common/lib/ -name "slf4j-log4j12-*.jar" -exec rm -v {} \;
find /opt/hadoop/share/hadoop/common/lib/ -name "log4j-slf4j-impl-*.jar" -exec rm -v {} \;

echo "Jar cleanup complete!"
