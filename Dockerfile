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

# Cache dependency resolution separately from source changes. Only src/main is needed to produce
# the runtime jar - src/test, src/jmh and src/dockerTest are irrelevant to bootJar and would only
# invalidate this layer's cache on every unrelated test/benchmark change.
COPY settings.gradle.kts build.gradle.kts ./
COPY src/main src/main
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

# application.yml's own default (a relative "data/broker-1") resolves under /app, which the
# non-root broker user has no write access to - only /data was chowned above. This default keeps
# a plain `docker run` (no explicit BROKER_DATA_DIRECTORY) working out of the box; per-broker
# compose setups still override it to their own subdirectory under /data.
ENV BROKER_DATA_DIRECTORY=/data

COPY --from=build /workspace/build/libs/*.jar app.jar

EXPOSE 8080

# Dependency-free HTTP health check (this base image has no curl/wget): a raw HTTP/1.1 request over
# bash's /dev/tcp, checking Actuator's aggregate health status rather than just "is the port open".
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=5 \
  CMD bash -c 'exec 3<>/dev/tcp/localhost/8080 && \
    printf "GET /actuator/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n" >&3 && \
    grep -q "\"status\":\"UP\"" <&3' || exit 1

# -XX:+ExitOnOutOfMemoryError: crash cleanly on OOM instead of limping along in a broken state, so
# Docker/an orchestrator can actually restart the container - container memory limits (if any) are
# left to be set at deploy time, never assumed here.
ENTRYPOINT ["java", "-XX:+ExitOnOutOfMemoryError", "-jar", "/app/app.jar"]
