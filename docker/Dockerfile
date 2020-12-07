FROM alpine/git as clone
WORKDIR /app
RUN git clone https://github.com/AndriiBraslavskyi/replication.git

FROM maven:3.6.0-jdk-8-slim AS build
WORKDIR /app
COPY --from=clone /app/replication /app
RUN mvn -Dmaven.test.skip=true clean package

FROM openjdk:8u252-jre-slim
WORKDIR /app
RUN mkdir conf
COPY --from=build /app/target/replication-0.0.1-SNAPSHOT.jar /app

# Default values
ARG SERVER_RETRY=99999
ARG SERVER_TIMEOUT=5000
ARG CLIENT_HOSTS=localhost:8070

# Values from docker compose file
ENV SERVER_RETRY $SERVER_RETRY
ENV SERVER_TIMEOUT $SERVER_TIMEOUT
ENV CLIENTS_HOSTS $CLIENTS_HOSTS

EXPOSE 8080

CMD ["java", "-jar", "replication-0.0.1-SNAPSHOT.jar", "--replicas.retry-number=${SERVER_RETRY}", "--replicas.timeout=${SERVER_TIMEOUT}", "--replicas.hosts=${CLIENTS_HOSTS}"]