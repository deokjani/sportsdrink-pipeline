#!/bin/bash

# 볼륨 경로에 권한 부여
chown -R 1000:1000 /var/trino || true
chmod -R 777 /var/trino || true

# 트리노 런처 실행
exec /usr/lib/trino/bin/launcher run --etc-dir /etc/trino
