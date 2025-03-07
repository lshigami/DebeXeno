# DebeXeno

[![Java Version](https://img.shields.io/badge/Java-17-blue.svg)](https://www.oracle.com/java/technologies/javase/jdk17-archive-downloads.html)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.4.3-brightgreen.svg)](https://spring.io/projects/spring-boot)

DebeXeno is a robust Change Data Capture (CDC) solution inspired by Debezium, designed to capture and stream changes from PostgreSQL databases to Apache Kafka in real-time. It enables event-driven architectures and real-time data integration by converting database changes into event streams.
[![Uploading image.png…]()](https://imgur.com/a/yJMqIHY)


## Features
- **PostgreSQL CDC**: Capture INSERT, UPDATE, and DELETE operations from PostgreSQL databases
- **Real-time Kafka Streaming**: Stream captured changes to Kafka topics with minimal latency
- **Schema Management**: Handle schema changes gracefully with built-in schema versioning
- **Error Handling**: Robust error handling with Dead Letter Queue (DLQ) support
- High availability and fault tolerance

## Prerequisites

- Java 17 or higher
- PostgreSQL database
- Apache Kafka
- Maven

## Tech Stack

- **Spring Boot** (v3.4.3) - Application framework
- **PostgreSQL** (v42.7.3) - Source database
- **Apache Kafka** (v3.4.0) - Event streaming platform
- **Apache Curator** (v5.6.0) - ZooKeeper client framework

## Getting Started

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/DebeXeno.git
cd DebeXeno
```

2. Build the project:
```bash
mvn clean install
```

3. Configure your application.properties:
```properties
# PostgreSQL Configuration
spring.datasource.url=jdbc:postgresql://localhost:5432/your_database
spring.datasource.username=your_username
spring.datasource.password=your_password

# Kafka Configuration
kafka.bootstrap.servers=localhost:9092
```

4. Run the application:
```bash
mvn spring-boot:run
```

## Configuration

DebeXeno can be configured through the `application.properties` file. Key configuration options include:

- Database connection settings
- Kafka broker configuration
- CDC capture settings


## Architecture

DebeXeno follows a modular architecture:

1. **Capture Service**: Monitors PostgreSQL changes using logical replication
2. **Event Processing**: Transforms database changes into standardized events
3. **Kafka Producer**: Streams events to configured Kafka topics
