package platform.ingestion.managed;

import java.net.URI;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import platform.ingestion.bronze.BronzeKeys;
import platform.ingestion.bronze.S3BronzeStorage;
import platform.ingestion.rss.RawFeed;
import platform.ingestion.rss.RssSource;
import static platform.ingestion.managed.StrictFetchJson.*;

/** Read-only native validation before publishing OK, while the sink owns its empty prefix. */
final class StrictFetchCandidate {
    static RawFeed reopen(StrictFetchEvidence sink, byte[] command) throws Exception {
        need(Arrays.equals(command, sink.readCommand()));
        var c = parse(command, 65536);
        var d = parse(sink.readCandidate("descriptor.json"), 65536);
        fields(d, "journalVersion", "commandSha256", "runId", "attemptId", "ownerEpoch", "stage", "itemOrdinal", "checkpointId",
                "sourceId", "revision", "capturedConfigSha256", "endpointSha256", "runtimeSha256", "ingestionId", "ingestedAt",
                "httpStatus", "contentType", "declaredContentLength", "payloadByteLength", "payloadSha256", "expected");
        need(d.get("journalVersion").isInt() && d.get("journalVersion").intValue() == 1
                && text(d, "commandSha256").equals(sha(command)));
        for (String key : List.of("runId", "attemptId", "ownerEpoch", "stage", "itemOrdinal", "checkpointId", "sourceId",
                "revision", "capturedConfigSha256", "endpointSha256", "runtimeSha256")) need(d.get(key).equals(c.get(key)));
        byte[] payload = sink.readCandidate("payload.xml");
        need(d.get("payloadByteLength").isInt() && d.get("payloadByteLength").intValue() == payload.length
                && text(d, "payloadSha256").equals(sha(payload)));
        need(d.get("httpStatus").isInt() && d.get("httpStatus").intValue() >= 200 && d.get("httpStatus").intValue() < 300
                && (d.get("declaredContentLength").isNull() || d.get("declaredContentLength").isIntegralNumber()
                && d.get("declaredContentLength").canConvertToLong()));
        UUID ingestion = UUID.fromString(text(d, "ingestionId"));
        Instant instant = Instant.parse(text(d, "ingestedAt"));
        need(ingestion.toString().equals(text(d, "ingestionId")) && instant.toString().equals(text(d, "ingestedAt")));
        var feed = new RawFeed(ingestion, text(d, "sourceId"), instant, d.get("httpStatus").intValue(),
                d.get("contentType").isNull() ? Optional.empty() : Optional.of(text(d, "contentType")),
                d.get("declaredContentLength").isNull() ? OptionalLong.empty() : OptionalLong.of(d.get("declaredContentLength").longValue()), payload);
        byte[] metadata = sink.readCandidate("metadata.json");
        need(Arrays.equals(metadata, S3BronzeStorage.preparedMetadata(new RssSource(feed.sourceId(), URI.create(text(c, "feedUri")), true), feed)));
        var e = d.get("expected"); fields(e, "payloadKey", "metadataKey", "metadataByteLength", "metadataSha256");
        var keys = BronzeKeys.forFeed(feed);
        need(text(e, "payloadKey").equals(keys.payload()) && text(e, "metadataKey").equals(keys.metadata())
                && e.get("metadataByteLength").isInt() && e.get("metadataByteLength").intValue() == metadata.length
                && text(e, "metadataSha256").equals(sha(metadata)));
        sink.inspect();
        return feed;
    }
    private StrictFetchCandidate() { }
}
