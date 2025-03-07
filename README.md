# DebeXeno

[![Java Version](https://img.shields.io/badge/Java-17-blue.svg)](https://www.oracle.com/java/technologies/javase/jdk17-archive-downloads.html)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.4.3-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

DebeXeno is a robust Change Data Capture (CDC) solution inspired by Debezium, designed to capture and stream changes from PostgreSQL databases to Apache Kafka in real-time. It enables event-driven architectures and real-time data integration by converting database changes into event streams.

## Features

- Real-time PostgreSQL change capture
- Reliable event streaming to Apache Kafka
- Spring Boot integration
- Prometheus metrics support
- Grafana dashboards for monitoring
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
- **Project Lombok** - Boilerplate code reduction
- **Prometheus & Grafana** - Monitoring and visualization

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
- Monitoring endpoints

## Monitoring

DebeXeno includes built-in monitoring capabilities:

- Prometheus metrics for performance monitoring
- Pre-configured Grafana dashboards
- Health check endpoints

## Architecture

DebeXeno follows a modular architecture:

1. **Capture Service**: Monitors PostgreSQL changes using logical replication
2. **Event Processing**: Transforms database changes into standardized events
3. **Kafka Producer**: Streams events to configured Kafka topics
4. **Metrics Service**: Collects and exposes operational metrics

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For support and questions, please open an issue in the GitHub repository.
