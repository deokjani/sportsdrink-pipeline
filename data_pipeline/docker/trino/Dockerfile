FROM trinodb/trino:412

USER root
RUN apt-get update && apt-get install -y curl bash iputils-ping netcat
COPY trino-entrypoint.sh /usr/local/bin/trino-entrypoint.sh
RUN chmod +x /usr/local/bin/trino-entrypoint.sh

USER trino
ENTRYPOINT ["/usr/local/bin/trino-entrypoint.sh"]
