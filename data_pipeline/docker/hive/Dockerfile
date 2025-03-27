FROM ubuntu:20.04

ENV HIVE_VERSION=3.1.3
ENV HADOOP_VERSION=3.3.4
ENV DEBIAN_FRONTEND=noninteractive

# 필수 패키지 설치
RUN apt-get update && apt-get install -y \
    curl wget tar openjdk-11-jdk gnupg lsb-release netcat \
    && apt-get clean

# JAVA 환경변수
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Hive 다운로드 및 설치
RUN curl -O https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz \
    && tar -xzf apache-hive-${HIVE_VERSION}-bin.tar.gz -C /opt/ \
    && rm apache-hive-${HIVE_VERSION}-bin.tar.gz \
    && ln -s /opt/apache-hive-${HIVE_VERSION}-bin /opt/hive

# Hadoop 다운로드
RUN curl -O https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt/ \
    && rm hadoop-${HADOOP_VERSION}.tar.gz \
    && ln -s /opt/hadoop-${HADOOP_VERSION} /opt/hadoop

# AWS SDK 다운로드
RUN curl -o /opt/hive/lib/aws-java-sdk-bundle.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# 로컬 jar 및 설정 파일들 복사
COPY mysql-connector-j-8.0.33.jar /opt/hive/lib/
COPY hadoop-aws-3.3.4.jar /opt/hadoop/share/hadoop/common/lib/
COPY hive-site.xml /opt/hive/conf/
COPY core-site.xml.template /opt/hive/conf/
COPY clean_jars.sh /opt/hive/
COPY entrypoint.sh /opt/hive/

# 실행권한 부여
RUN chmod +x /opt/hive/clean_jars.sh /opt/hive/entrypoint.sh

# Entrypoint
ENTRYPOINT ["/opt/hive/entrypoint.sh"]
