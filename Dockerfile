FROM gcr.io/distroless/java:8
WORKDIR /opt
COPY target/dynamodb-import-export-tool-1.0.1.jar app.jar
ENTRYPOINT ["/usr/bin/java", "-jar","app.jar"]
