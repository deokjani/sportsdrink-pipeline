FROM openjdk:8-jdk

# 필수 패키지 설치
RUN apt-get update && \
    apt-get install -y wget curl unzip python3-pip gettext && \
    apt-get clean

# AWS CLI 설치
RUN pip3 install awscli

# Hadoop 3.3.4 복사 & 설치
COPY hadoop-3.3.4.tar.gz /opt/
RUN tar -xzf /opt/hadoop-3.3.4.tar.gz -C /opt/ && \
    mv /opt/hadoop-3.3.4 /opt/hadoop && \
    rm -f /opt/hadoop-3.3.4.tar.gz

ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$HADOOP_HOME/bin:$PATH

# Hive 3.1.3 복사 & 설치
COPY apache-hive-3.1.3-bin.tar.gz /opt/
RUN tar -xzf /opt/apache-hive-3.1.3-bin.tar.gz -C /opt/ && \
    mv /opt/apache-hive-3.1.3-bin /opt/hive && \
    rm -f /opt/apache-hive-3.1.3-bin.tar.gz

ENV HIVE_HOME=/opt/hive
ENV PATH=$HIVE_HOME/bin:$PATH

# 최신 JAR 복사
COPY hadoop-aws-3.3.4.jar /opt/hive/lib/
COPY aws-java-sdk-bundle-1.12.262.jar /opt/hive/lib/
COPY mysql-connector-j-8.0.33.jar /opt/hive/lib/

# 설정 파일 및 스크립트 복사
COPY hive-site.xml /opt/hive/conf/hive-site.xml
COPY clean_jars.sh /opt/hive/clean_jars.sh
RUN chmod +x /opt/hive/clean_jars.sh

COPY core-site.xml.template /opt/hive/conf/core-site.xml.template
COPY entrypoint.sh /opt/hive/entrypoint.sh
RUN chmod +x /opt/hive/entrypoint.sh

ENTRYPOINT ["/opt/hive/entrypoint.sh"]
