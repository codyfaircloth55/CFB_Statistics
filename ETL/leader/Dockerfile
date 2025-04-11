FROM apache/spark:latest

USER root

RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/partial/ && \
    apt-get autoclean && \
    apt-get update && \
    apt-get install -y openssh-server && \
    mkdir /var/run/sshd && \
    echo 'root:root' | chpasswd && \
    echo "PermitRootLogin yes" >> /etc/ssh/sshd_config && \
    ssh-keygen -A && \
    wget -P /opt/spark/jars \
    -q https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/9.2.0/mysql-connector-j-9.2.0.jar

EXPOSE 22 8080 7077

CMD service ssh start && /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master -h spark-leader