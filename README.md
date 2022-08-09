# Event Processing Project SS 2022

## Organization

- **Student**: Tung Nguyen
- **Matriculation**: 03764572
- **Major**: Computer Science 
- **Course**: Event Processing
- **Term**: Summer Term 2022
 
---
## Technologies

Kafka, KSQL, Java, Maven, Docker

---
## Requirements & Installation

- **Java**: https://www.oracle.com/java/technologies/downloads/
- **Maven**: https://maven.apache.org/
- **Docker**: https://www.docker.com/
- **Kafka, KSQL**: Wrapped inside [Confluent setup](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html)

---
## Setup & Deployment:

- Run `docker-compose up -d`. Make sure that Docker is up to run

- Create topic `meetup-events` with 2 partitions

- Create stream `MEETUP_EVENTS_STREAM` by running:

````$xslt
CREATE STREAM MEETUP_EVENTS_STREAM (
  key VARCHAR KEY,
  utc_offset BIGINT, 
  venue STRUCT<country VARCHAR, city VARCHAR>
) WITH (KAFKA_TOPIC='meetup-events', VALUE_FORMAT='JSON');
````

- Create stream `GERMANY_MEETUP_EVENTS_STREAM` by:
````$xslt
CREATE STREAM GERMANY_MEETUP_EVENTS_STREAM
AS SELECT * FROM MEETUP_EVENTS_STREAM WHERE venue->country = 'de'
EMIT CHANGES;
````

- Create stream `GERMANY_MUNICH_MEETUP_EVENTS_STREAM` by:
````$xslt
CREATE STREAM GERMANY_MUNICH_MEETUP_EVENTS_STREAM
AS SELECT * FROM GERMANY_MEETUP_EVENTS_STREAM 
WHERE venue->city LIKE '%Munich%' or venue->city LIKE '%München%'
EMIT CHANGES;
````

- Create stream `MUNICH_MEETUP_EVENTS_STREAM` by:
````$xslt
CREATE STREAM MUNICH_MEETUP_EVENTS_STREAM
AS SELECT * FROM MEETUP_EVENTS_STREAM 
WHERE venue->city LIKE '%Munich%' or venue->city LIKE '%München%'
EMIT CHANGES;
````

- Run `mvn install & mvn clean compile exec:java`

---
## Clean-up

- Docker stop: `docker-compose stop`
- Docker container clean: `docker system prune -a --volumes --filter "label=io.confluent.docker"`

---
## Additional Sources

Java Kafka API: https://kafka.apache.org/21/javadoc/?org/apache/kafka/