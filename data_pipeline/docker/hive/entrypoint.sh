#!/bin/bash
set -e

echo "Running jar cleanup..."
/opt/hive/clean_jars.sh

echo "Generating core-site.xml from template..."
envsubst < /opt/hive/conf/core-site.xml.template > /opt/hadoop/etc/hadoop/core-site.xml
echo "core-site.xml generated successfully."

echo "Checking Hive Metastore Schema..."
if schematool -dbType mysql -info > /dev/null 2>&1; then
    echo "Schema already initialized. Skipping initialization."
else
    echo "Initializing Hive Metastore Schema..."
    schematool -dbType mysql -initSchema
fi

echo "Starting Hive Metastore..."
exec /opt/hive/bin/hive --service metastore
