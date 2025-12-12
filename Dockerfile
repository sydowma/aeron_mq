# Build stage
FROM azul/zulu-openjdk:25 AS builder

# Install Maven
RUN apt-get update && apt-get install -y maven && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy pom files first for dependency caching
COPY pom.xml .
COPY aeron-mq-protocol/pom.xml aeron-mq-protocol/
COPY aeron-mq-broker/pom.xml aeron-mq-broker/
COPY aeron-mq-gateway/pom.xml aeron-mq-gateway/
COPY aeron-mq-server/pom.xml aeron-mq-server/

# Download dependencies (cached layer)
RUN mvn dependency:go-offline -B || true

# Copy source code
COPY aeron-mq-protocol/src aeron-mq-protocol/src
COPY aeron-mq-broker/src aeron-mq-broker/src
COPY aeron-mq-gateway/src aeron-mq-gateway/src
COPY aeron-mq-server/src aeron-mq-server/src

# Build the application
RUN mvn package -DskipTests -B

# Runtime stage - reuse JDK
FROM azul/zulu-openjdk:25

# Install necessary tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    dumb-init \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the built JAR
COPY --from=builder /app/aeron-mq-server/target/aeron-mq-server-1.0.0-SNAPSHOT.jar /app/aeron-mq.jar

# Create data directories
RUN mkdir -p /app/data /app/logs

# Environment variables with defaults
ENV NODE_ID=0 \
    HOST=0.0.0.0 \
    KAFKA_PORT=9092 \
    DATA_DIR=/app/data \
    AERON_DIR=/dev/shm/aeron-mq \
    AUTO_CREATE_TOPICS=true \
    JAVA_OPTS="-Xms512m -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=10"

# Expose Kafka protocol port
EXPOSE 9092

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD nc -z localhost 9092 || exit 1

# Use dumb-init as PID 1 for proper signal handling
ENTRYPOINT ["/usr/bin/dumb-init", "--"]

# Start the server
CMD ["sh", "-c", "java $JAVA_OPTS --enable-preview -jar /app/aeron-mq.jar"]
