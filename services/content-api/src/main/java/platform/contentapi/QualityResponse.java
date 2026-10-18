package platform.contentapi;

import java.util.List;

public record QualityResponse(String publication, List<Quality> data, Meta meta) {
    public record Quality(String sourceId, String observationCount, String qualityWarningCount,
                          double warningCodesPerObservation) { }
    public record Meta(int returned, int limit) { }
}
