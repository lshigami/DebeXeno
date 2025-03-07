# DebeXeno

[![Java Version](https://img.shields.io/badge/Java-17-blue.svg)](https://www.oracle.com/java/technologies/javase/jdk17-archive-downloads.html)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.4.3-brightgreen.svg)](https://spring.io/projects/spring-boot)

DebeXeno is a robust Change Data Capture (CDC) solution inspired by Debezium, designed to capture and stream changes from PostgreSQL databases to Apache Kafka in real-time. It enables event-driven architectures and real-time data integration by converting database changes into event streams.
![Architecture](https://i.imgur.com/RD0Xvx0.png)



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

### PostgreSQL Configuration for CDC  

DebeXeno utilizes PostgreSQL's **logical replication** to capture database changes in real-time. To enable this, you need to configure your PostgreSQL instance with the following settings:  

#### 1. Enable Logical Replication  
Edit your `postgresql.conf` file to set `wal_level` to `logical`:  
```properties
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
```
Restart PostgreSQL after applying the changes:  
```bash
sudo systemctl restart postgresql
```
**Purpose:**  
This enables logical decoding, allowing PostgreSQL to track and stream changes to external consumers like DebeXeno.  

#### 2. Create a Replication Slot  
Connect to PostgreSQL and create a **replication slot** using the `wal2json` logical decoding plugin:  
```sql
SELECT * FROM pg_create_logical_replication_slot('debeXeno_slot', 'wal2json');
```
**Purpose:**  
A replication slot retains WAL logs for CDC tools to consume without losing data. `wal2json` converts changes into JSON format for easy processing.  

#### 3. Set REPLICA IDENTITY FULL  
To ensure **UPDATE** and **DELETE** operations contain full row data, set `REPLICA IDENTITY` for the required tables:  
```sql
ALTER TABLE your_table REPLICA IDENTITY FULL;
```
**Purpose:**  
By default, PostgreSQL only logs the primary key when rows are updated or deleted. `REPLICA IDENTITY FULL` ensures the entire row data is available in change events.  

## Architecture

DebeXeno follows a modular architecture:

1. **Capture Service**: Monitors PostgreSQL changes using logical replication
2. **Event Processing**: Transforms database changes into standardized events
3. **Kafka Producer**: Streams events to configured Kafka topics
