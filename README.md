# Kafbat OTLP Trace Serde

A custom [Kafbat UI](https://github.com/kafbat/kafka-ui) serde for viewing Kafka record values encoded as the OTLP protobuf message `opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest`.

The serde:

- renders the protobuf as JSON in Kafbat;
- decompresses values when `content_encoding`, `content-encoding`, or `Content-Encoding` is `gzip`;
- falls back to gzip magic-byte detection when the header is absent;
- matches the ingestion consumer's 8 MiB decoded-request limit; and
- only supports value deserialization, not record production.

## Build

[mise](https://mise.jdx.dev/) pins the Java and Gradle versions used by Kafbat v1.5.0:

```sh
mise install
mise run check
```

The self-contained plugin is written to `build/libs/kafbat-otlp-trace-serde-0.1.1.jar`. It bundles the OTLP and protobuf runtime dependencies, but deliberately excludes Kafbat's serde API because Kafbat supplies that interface to the plugin classloader.

## Distribution

Releases are published as minimal OCI images containing only the plugin JAR. This lets another image copy the JAR without vendoring it in source control:

```dockerfile
FROM packages.buildkite.com/buildkite/kafbat-otlp-trace-serde/kafbat-otlp-trace-serde:0.1.1 AS otlp-trace-serde
FROM kafbat/kafka-ui:v1.5.0
COPY --from=otlp-trace-serde /kafbat-otlp-trace-serde.jar /opt/kafbat/serdes/kafbat-otlp-trace-serde.jar
```

Consumers should pin the artifact image by digest.

## Kafbat configuration

Mount the JAR into the Kafbat container and add the serde to each applicable cluster:

```yaml
kafka:
  clusters:
    - name: example
      serde:
        - name: OTLP Trace
          className: com.buildkite.kafbat.serde.OtlpTraceSerde
          filePath: /opt/kafbat/serdes/kafbat-otlp-trace-serde.jar
          topicValuesPattern: ta-otlp-traces-inline
```

The `topicValuesPattern` is a regular expression. Omit it to make the serde manually selectable for every topic value.

## License

Apache License 2.0.
