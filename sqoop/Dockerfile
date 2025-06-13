FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

# Instalar Java y herramientas necesarias
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk wget curl unzip && \
    mkdir -p /usr/share/java && \
    wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/5.1.49/mysql-connector-java-5.1.49.jar \
    -O /usr/share/java/mysql-connector-java.jar && \
    apt-get clean

# Instalar Hadoop 2.7.4
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-2.7.4/hadoop-2.7.4.tar.gz && \
    tar -xzf hadoop-2.7.4.tar.gz -C /opt && \
    ln -s /opt/hadoop-2.7.4 /opt/hadoop && \
    rm hadoop-2.7.4.tar.gz

# Instalar Sqoop 1.4.7 desde URL válida (no redirección)
RUN wget https://archive.apache.org/dist/sqoop/1.4.7/sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz && \
    tar -xzf sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz -C /opt && \
    ln -s /opt/sqoop-1.4.7.bin__hadoop-2.6.0 /opt/sqoop && \
    rm sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz

# Variables de entorno
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop
ENV SQOOP_HOME=/opt/sqoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$SQOOP_HOME/bin
ENV HADOOP_COMMON_HOME=$HADOOP_HOME
ENV HADOOP_MAPRED_HOME=$HADOOP_HOME
ENV HADOOP_HDFS_HOME=$HADOOP_HOME
ENV HADOOP_YARN_HOME=$HADOOP_HOME
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

# Enlazar el conector JDBC de MySQL a Sqoop
RUN ln -s /usr/share/java/mysql-connector-java.jar $SQOOP_HOME/lib/mysql-connector-java.jar

CMD ["bash"]