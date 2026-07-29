# ====== 1) Build stage ======
FROM eclipse-temurin:25-jdk AS build

WORKDIR /workspace

# Copy Maven Wrapper and POM first for dependency caching
COPY mvnw .
COPY .mvn .mvn
COPY pom.xml .

RUN chmod +x mvnw

# Download dependencies
RUN ./mvnw -B -U -DskipTests dependency:go-offline

# Copy sources and build
COPY src ./src

RUN ./mvnw -B -DskipTests clean package

# Copy the executable Spring Boot JAR to a fixed filename
RUN JAR_FILE="$(find target \
      -maxdepth 1 \
      -type f \
      -name 'rdnoc-alarm-app-*.jar' \
      ! -name '*.original' \
      | head -n 1)" \
    && test -n "$JAR_FILE" \
    && cp "$JAR_FILE" /workspace/app.jar


# ====== 2) Runtime stage ======
FROM eclipse-temurin:25-jre

USER root

# Install curl for the health check
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root application user and config directory
RUN groupadd --system app \
    && useradd --system --create-home --gid app app \
    && mkdir -p /app/config \
    && chown -R app:app /app

WORKDIR /app

COPY --from=build --chown=app:app /workspace/app.jar /app/app.jar

USER app

ENV JAVA_OPTS="-Xms256m -Xmx512m"
ENV SPRING_PROFILES_ACTIVE="default"

EXPOSE 8080

HEALTHCHECK --interval=30s \
            --timeout=3s \
            --start-period=20s \
            --retries=3 \
  CMD curl -fsS http://localhost:8080/actuator/health \
      | grep -q '"status":"UP"' \
      || exit 1

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar --spring.config.additional-location=optional:file:/app/config/"]