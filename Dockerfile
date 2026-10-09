# Stage 1: build the executable jar inside the image (no local Maven required).
FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /workspace
COPY pom.xml mvnw ./
COPY .mvn/ .mvn/
RUN ./mvnw dependency:go-offline -q --no-transfer-progress
COPY src/ src/
RUN ./mvnw package -DskipTests -q --no-transfer-progress

# Stage 2: explode the jar into Spring Boot layers for better Docker caching.
FROM build AS layers
WORKDIR /layers
RUN cp /workspace/target/*-exec.jar application.jar \
 && java -Djarmode=layertools -jar application.jar extract

# Stage 3: minimal runtime image, non-root.
# The "RUN true" lines work around moby/moby#37965 (legacy builder on Docker 18.09-20.10 can
# fail with "layer does not exist" on consecutive COPY --from); harmless under BuildKit.
FROM eclipse-temurin:21-jre-noble
RUN useradd --system --uid 10001 --gid root --home-dir /app --shell /usr/sbin/nologin app
WORKDIR /app
COPY --from=layers /layers/dependencies/ ./
RUN true
COPY --from=layers /layers/snapshot-dependencies/ ./
RUN true
COPY --from=layers /layers/spring-boot-loader/ ./
RUN true
COPY --from=layers /layers/application/ ./
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0"
USER 10001
EXPOSE 8083
ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
