package com.buildkite.kafbat.serde;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.kafbat.ui.serde.api.DeserializeResult;
import io.kafbat.ui.serde.api.PropertyResolver;
import io.kafbat.ui.serde.api.RecordHeader;
import io.kafbat.ui.serde.api.RecordHeaders;
import io.kafbat.ui.serde.api.SchemaDescription;
import io.kafbat.ui.serde.api.Serde;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.zip.GZIPInputStream;

/** Deserializes raw or gzip-compressed OTLP trace export requests. */
public final class OtlpTraceSerde implements Serde {
  private static final int MAX_DECODED_BYTES = 64 * 1024 * 1024;
  private static final int COPY_BUFFER_BYTES = 8192;
  private static final JsonFormat.Printer JSON_PRINTER = JsonFormat.printer();

  @Override
  public void configure(
      PropertyResolver serdeProperties,
      PropertyResolver kafkaClusterProperties,
      PropertyResolver globalProperties
  ) {
    // This serde has no configuration.
  }

  @Override
  public Optional<String> getDescription() {
    return Optional.of(
        "Decodes `opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest` values. "
            + "Gzip is selected by a Content-Encoding header or by gzip magic bytes."
    );
  }

  @Override
  public Optional<SchemaDescription> getSchema(String topic, Target type) {
    return Optional.empty();
  }

  @Override
  public boolean canDeserialize(String topic, Target type) {
    return type == Target.VALUE;
  }

  @Override
  public boolean canSerialize(String topic, Target type) {
    return false;
  }

  @Override
  public Serializer serializer(String topic, Target type) {
    throw new UnsupportedOperationException("OtlpTraceSerde does not support serialization");
  }

  @Override
  public Deserializer deserializer(String topic, Target type) {
    if (!canDeserialize(topic, type)) {
      throw new IllegalArgumentException("OtlpTraceSerde only deserializes record values");
    }
    return OtlpTraceSerde::deserialize;
  }

  private static DeserializeResult deserialize(RecordHeaders headers, byte[] data) {
    if (data == null) {
      return new DeserializeResult(null, DeserializeResult.Type.JSON, Map.of());
    }

    DecodedPayload decoded = decodePayload(headers, data);
    try {
      ExportTraceServiceRequest request = ExportTraceServiceRequest.parseFrom(decoded.bytes());
      String json = JSON_PRINTER.print(request);
      return new DeserializeResult(
          json,
          DeserializeResult.Type.JSON,
          Map.of(
              "messageType", ExportTraceServiceRequest.getDescriptor().getFullName(),
              "compression", decoded.compression(),
              "encodedBytes", data.length,
              "decodedBytes", decoded.bytes().length
          )
      );
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("Value is not an OTLP ExportTraceServiceRequest", e);
    }
  }

  private static DecodedPayload decodePayload(RecordHeaders headers, byte[] data) {
    Optional<String> contentEncoding = contentEncoding(headers);
    if (contentEncoding.isPresent()
        && !contentEncoding.get().equals("gzip")
        && !contentEncoding.get().equals("identity")) {
      throw new IllegalArgumentException(
          "Unsupported Content-Encoding: " + contentEncoding.get()
      );
    }

    boolean gzip = contentEncoding.filter("gzip"::equals).isPresent() || hasGzipMagic(data);
    if (!gzip && data.length > MAX_DECODED_BYTES) {
      throw decodedSizeExceeded();
    }
    byte[] decoded = gzip ? gunzip(data) : data;
    if (decoded.length > MAX_DECODED_BYTES) {
      throw decodedSizeExceeded();
    }
    return new DecodedPayload(decoded, gzip ? "gzip" : "identity");
  }

  private static Optional<String> contentEncoding(RecordHeaders headers) {
    if (headers == null) {
      return Optional.empty();
    }

    String value = null;
    for (RecordHeader header : headers) {
      if (header.key() != null
          && header.key().replace('_', '-').equalsIgnoreCase("content-encoding")) {
        if (header.value() == null) {
          throw new IllegalArgumentException("Content-Encoding header has no value");
        }
        value = new String(header.value(), StandardCharsets.UTF_8)
            .trim()
            .toLowerCase(Locale.ROOT);
      }
    }
    return Optional.ofNullable(value);
  }

  private static boolean hasGzipMagic(byte[] data) {
    return data.length >= 2
        && (data[0] & 0xff) == 0x1f
        && (data[1] & 0xff) == 0x8b;
  }

  private static byte[] gunzip(byte[] data) {
    try (
        GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(data));
        ByteArrayOutputStream output = new ByteArrayOutputStream(
            Math.min(Math.max(data.length * 2, COPY_BUFFER_BYTES), MAX_DECODED_BYTES)
        )
    ) {
      byte[] buffer = new byte[COPY_BUFFER_BYTES];
      int total = 0;
      int read;
      while ((read = gzip.read(buffer)) != -1) {
        total += read;
        if (total > MAX_DECODED_BYTES) {
          throw decodedSizeExceeded();
        }
        output.write(buffer, 0, read);
      }
      return output.toByteArray();
    } catch (IOException e) {
      throw new IllegalArgumentException("Value is not valid gzip data", e);
    }
  }

  private static IllegalArgumentException decodedSizeExceeded() {
    return new IllegalArgumentException(
        "Decoded OTLP value exceeds the 64 MiB safety limit"
    );
  }

  private record DecodedPayload(byte[] bytes, String compression) {
  }
}
