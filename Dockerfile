# ====== 1) Build stage ======
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /workspace

# copy only the pom first (layer caching)
COPY pom.xml .
RUN mvn -q -e -U -DskipTests dependency:go-offline

# now copy sources and build
COPY src ./src
RUN mvn -q -DskipTests clean package

# ====== 2) Runtime stage ======
# Use a small JRE image; Temurin is stable
FROM eclipse-temurin:21-jre

# Install curl just for the healthcheck (then drop to non-root)
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

# Create app user (non-root)
RUN groupadd --system app && useradd --system --create-home --gid app app
USER app

WORKDIR /app

# copy boot fat jar from the build stage
COPY --from=build /workspace/target/rdnoc-alarm-app-0.0.1-SNAPSHOT.jar /app/app.jar

# Optional: Drop in a default config dir (you can mount your own at runtime)
# COPY src/main/resources/application.yml /app/config/application.yml

# JVM + Spring defaults are overridable via env
ENV JAVA_OPTS="-Xms256m -Xmx512m"
ENV SPRING_PROFILES_ACTIVE=default
# If you use a separate management port, expose it too
EXPOSE 8080

# Basic healthcheck against actuator (remove if not using actuator)
HEALTHCHECK --interval=30s --timeout=3s --start-period=20s \
  CMD wget -qO- http://localhost:8080/actuator/health | grep '"status":"UP"' || exit 1

# Let Spring read external config from /app/config (first in search path)
ENTRYPOINT [ "sh", "-c", "java $JAVA_OPTS -jar /app/app.jar --spring.config.additional-location=file:/app/config/" ]
