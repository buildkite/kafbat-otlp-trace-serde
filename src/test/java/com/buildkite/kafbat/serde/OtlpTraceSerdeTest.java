package com.buildkite.kafbat.serde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.util.JsonFormat;
import io.kafbat.ui.serde.api.DeserializeResult;
import io.kafbat.ui.serde.api.RecordHeader;
import io.kafbat.ui.serde.api.RecordHeaders;
import io.kafbat.ui.serde.api.Serde;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class OtlpTraceSerdeTest {
  private static final RecordHeaders NO_HEADERS = () -> List.<RecordHeader>of().iterator();
  private static final ExportTraceServiceRequest REQUEST = ExportTraceServiceRequest.newBuilder()
      .addResourceSpans(ResourceSpans.newBuilder().setSchemaUrl("https://opentelemetry.io/schemas/1.38.0"))
      .build();

  private final OtlpTraceSerde serde = new OtlpTraceSerde();

  @Test
  void deserializesRawExportTraceServiceRequestAsJson() throws Exception {
    DeserializeResult result = deserialize(NO_HEADERS, REQUEST.toByteArray());

    assertEquals(DeserializeResult.Type.JSON, result.getType());
    assertEquals(REQUEST, parseJson(result.getResult()));
    assertEquals(
        Map.of(
            "messageType", "opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest",
            "compression", "identity",
            "encodedBytes", REQUEST.getSerializedSize(),
            "decodedBytes", REQUEST.getSerializedSize()
        ),
        result.getAdditionalProperties()
    );
  }

  @Test
  void deserializesGzipSelectedByHeader() throws Exception {
    byte[] compressed = gzip(REQUEST.toByteArray());

    DeserializeResult result = deserialize(headers("Content-Encoding", "GZIP"), compressed);

    assertEquals(REQUEST, parseJson(result.getResult()));
    assertEquals("gzip", result.getAdditionalProperties().get("compression"));
    assertEquals(compressed.length, result.getAdditionalProperties().get("encodedBytes"));
    assertEquals(REQUEST.getSerializedSize(), result.getAdditionalProperties().get("decodedBytes"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"content_encoding", "content-encoding", "Content-Encoding"})
  void recognizesSupportedContentEncodingHeaderNames(String headerName) throws Exception {
    DeserializeResult result = deserialize(headers(headerName, "gzip"), gzip(REQUEST.toByteArray()));

    assertEquals(REQUEST, parseJson(result.getResult()));
    assertEquals("gzip", result.getAdditionalProperties().get("compression"));
  }

  @Test
  void detectsGzipFromMagicBytesWithoutAHeader() throws Exception {
    DeserializeResult result = deserialize(NO_HEADERS, gzip(REQUEST.toByteArray()));

    assertEquals(REQUEST, parseJson(result.getResult()));
    assertEquals("gzip", result.getAdditionalProperties().get("compression"));
  }

  @Test
  void rejectsUnsupportedContentEncoding() {
    IllegalArgumentException error = assertThrows(
        IllegalArgumentException.class,
        () -> deserialize(headers("content_encoding", "br"), REQUEST.toByteArray())
    );

    assertEquals("Unsupported Content-Encoding: br", error.getMessage());
  }

  @Test
  void rejectsMalformedProtobuf() {
    IllegalArgumentException error = assertThrows(
        IllegalArgumentException.class,
        () -> deserialize(NO_HEADERS, new byte[] {(byte) 0xff})
    );

    assertEquals("Value is not an OTLP ExportTraceServiceRequest", error.getMessage());
  }

  @Test
  void rejectsRawValuesLargerThanTheIngestionLimit() {
    IllegalArgumentException error = assertThrows(
        IllegalArgumentException.class,
        () -> deserialize(NO_HEADERS, new byte[8 * 1024 * 1024 + 1])
    );

    assertEquals("Decoded OTLP value exceeds the 8 MiB safety limit", error.getMessage());
  }

  @Test
  void rejectsGzipValuesThatExpandBeyondTheIngestionLimit() throws Exception {
    byte[] compressed = gzip(new byte[8 * 1024 * 1024 + 1]);

    IllegalArgumentException error = assertThrows(
        IllegalArgumentException.class,
        () -> deserialize(headers("Content-Encoding", "gzip"), compressed)
    );

    assertEquals("Decoded OTLP value exceeds the 8 MiB safety limit", error.getMessage());
  }

  @Test
  void onlySupportsValueDeserialization() {
    assertTrue(serde.canDeserialize("topic", Serde.Target.VALUE));
    assertFalse(serde.canDeserialize("topic", Serde.Target.KEY));
    assertFalse(serde.canSerialize("topic", Serde.Target.VALUE));
    assertThrows(
        UnsupportedOperationException.class,
        () -> serde.serializer("topic", Serde.Target.VALUE)
    );
  }

  private DeserializeResult deserialize(RecordHeaders headers, byte[] data) {
    return serde.deserializer("topic", Serde.Target.VALUE).deserialize(headers, data);
  }

  private static ExportTraceServiceRequest parseJson(String json) throws Exception {
    ExportTraceServiceRequest.Builder builder = ExportTraceServiceRequest.newBuilder();
    JsonFormat.parser().merge(json, builder);
    return builder.build();
  }

  private static RecordHeaders headers(String key, String value) {
    RecordHeader header = new TestHeader(key, value.getBytes(StandardCharsets.UTF_8));
    return () -> List.of(header).iterator();
  }

  private static byte[] gzip(byte[] data) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(output)) {
      gzip.write(data);
    }
    return output.toByteArray();
  }

  private record TestHeader(String key, byte[] value) implements RecordHeader {
  }
}
