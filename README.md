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

The self-contained plugin is written to `build/libs/kafbat-otlp-trace-serde-0.1.0.jar`. It bundles the OTLP and protobuf runtime dependencies, but deliberately excludes Kafbat's serde API because Kafbat supplies that interface to the plugin classloader.

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
