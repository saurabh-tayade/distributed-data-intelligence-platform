package platform.contentapi.importing;

import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.nio.file.*;
import java.util.Arrays;
import tools.jackson.core.*;
import tools.jackson.core.json.JsonFactory;
import tools.jackson.databind.*;
import tools.jackson.databind.json.JsonMapper;
import platform.contentapi.management.SourceJson;
import static platform.contentapi.importing.ImportFailure.Code.*;

final class ImportJson {
    static final int PROTOCOL_LIMIT=512*1024;
    private static final JsonMapper JSON=JsonMapper.builder(JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(16).maxStringLength(65536).maxNumberLength(20).build()).build())
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).build();
    static JsonNode parse(byte[] bytes,int max) {
        try {
            ImportFailure.require(bytes!=null && bytes.length>0 && bytes.length<=max,INVALID_CATALOG);
            String s=StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
            ImportFailure.require(s.charAt(0)!='\ufeff',INVALID_CATALOG);
            var n=JSON.readTree(s);SourceJson.canonical(n);return n;
        } catch(RuntimeException | CharacterCodingException ex) {throw new ImportFailure(INVALID_CATALOG);}
    }
    static byte[] read(Path path,int limit) {
        ImportFailure.require(Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS),INVALID_CATALOG);
        try(var in=Files.newInputStream(path,LinkOption.NOFOLLOW_LINKS)) {
            byte[] raw=in.readNBytes(limit+1);ImportFailure.require(raw.length<=limit,INVALID_CATALOG);return raw;
        } catch(java.io.IOException | RuntimeException ex) {throw new ImportFailure(INVALID_CATALOG);}
    }
    static String text(JsonNode n) {try{return SourceJson.text(n);}catch(RuntimeException e){throw new ImportFailure(INVALID_CATALOG);}}
    static void fields(JsonNode n,String... fields) {try{SourceJson.fields(n,fields);}catch(RuntimeException e){throw new ImportFailure(INVALID_CATALOG);}}
    static int integer(JsonNode n,int low,int high) {
        ImportFailure.require(n!=null && n.isIntegralNumber() && n.canConvertToInt() && n.intValue()>=low && n.intValue()<=high,INVALID_CATALOG);return n.intValue();
    }
    static String match(JsonNode n,String regex) {String s=text(n);ImportFailure.require(s.matches(regex),INVALID_CATALOG);return s;}
    static String token(JsonNode n) {return match(n,"[A-Za-z0-9_-]{1,128}");}
    static String hash(JsonNode n) {return match(n,"[0-9a-f]{64}");}
    static java.util.UUID uuid(JsonNode n) {
        try {String s=text(n);var id=java.util.UUID.fromString(s);ImportFailure.require(id.toString().equals(s),INVALID_CATALOG);return id;}
        catch(RuntimeException ex){throw new ImportFailure(INVALID_CATALOG);}
    }
    static void canonical(byte[] raw,JsonNode n) {ImportFailure.require(Arrays.equals(raw,SourceJson.canonical(n)),INVALID_CATALOG);}
}
