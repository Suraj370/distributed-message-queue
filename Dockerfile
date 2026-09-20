# Multi-stage build for the distributed-message-queue broker.
#
# Broker identity, cluster membership, and Raft/transport timing are NEVER baked into this image -
# they come entirely from Spring configuration (environment variables, e.g. BROKER_ID,
# CLUSTER_BROKERS_0_ID, ...) supplied at container run time. The same image runs as any broker in
# the cluster; only its runtime configuration differs. See application.yml for the full set of
# recognized properties.

# ---- Build stage -----------------------------------------------------------------------------
# Uses the official Gradle image (Gradle pre-installed) rather than this project's own wrapper, so
# the build never depends on services.gradle.org being reachable at image-build time - only on
# Docker Hub, which any Docker build already depends on for its base images anyway.
FROM gradle:9.7.1-jdk25 AS build
WORKDIR /workspace

# Cache dependency resolution separately from source changes.
COPY settings.gradle.kts build.gradle.kts ./
COPY src src
RUN gradle bootJar --no-daemon -x test

# ---- Runtime stage ----------------------------------------------------------------------------
FROM eclipse-temurin:25-jre AS runtime
WORKDIR /app

# Runs as a non-root user. /data is this image's conventional writable data root - set
# BROKER_DATA_DIRECTORY to a path under it (e.g. /data/broker-1), or mount a volume over /data
# entirely, if data should survive a container restart.
RUN useradd --create-home --shell /usr/sbin/nologin broker \
    && mkdir -p /data \
    && chown broker:broker /data
USER broker

COPY --from=build /workspace/build/libs/*.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
