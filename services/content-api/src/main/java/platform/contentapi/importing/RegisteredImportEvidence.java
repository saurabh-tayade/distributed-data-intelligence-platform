package platform.contentapi.importing;

import java.util.Arrays;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;

/** Database metadata validation only: no filesystem, network, native reader, process, or freshness claim. */
public final class RegisteredImportEvidence {
    private RegisteredImportEvidence() { }
    public record Qualified(ImportCatalog.Entry selection,JsonNode evidence) { }
    public static Qualified validate(byte[] bytes,String digest) {
        try {
        var n=ImportJson.parse(bytes,ImportJson.PROTOCOL_LIMIT);
        ImportJson.fields(n,"qualificationVersion","selection","verification");ImportJson.integer(n.get("qualificationVersion"),1,1);
        ImportFailure.require(Arrays.equals(bytes,SourceJson.canonical(n)) && SourceJson.sha(bytes).equals(digest),ImportFailure.Code.RESULT_INVALID);
        var selection=ImportCatalog.entry(n.get("selection"));ImportEvidence.validate(selection,n.get("verification"));
        return new Qualified(selection,n);
        } catch(RuntimeException ex) {throw new ImportFailure(ImportFailure.Code.RESULT_INVALID); }
    }
}
