package platform.contentapi.management;

import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.security.MessageDigest;
import java.util.*;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.core.StreamReadConstraints;
import tools.jackson.core.json.JsonFactory;
import tools.jackson.databind.*;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/** Small canonical byte encoder for bounded E5 trees: object/array/string/integer/boolean/null only. */
public final class SourceJson {
    private SourceJson() { }
    static final JsonMapper JSON=JsonMapper.builder(JsonFactory.builder()
            .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(16).maxStringLength(65536).maxNumberLength(20).build())
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build())
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).build();
    public static JsonNode parse(byte[] bytes) {
        try {
            SourceFailure.require(bytes.length<=65536 && bytes.length>0);
            String text=StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
            SourceFailure.require(text.charAt(0)!='\ufeff');
            JsonNode node=JSON.readTree(text); canonical(node); return node;
        } catch(SourceFailure ex) { throw ex; }
        catch(Exception ex) { throw new SourceFailure(SourceFailure.Code.INVALID_ARGUMENT); }
    }
    public static ObjectNode object() { return JSON.createObjectNode(); }
    public static void fields(JsonNode n,String... names) {
        SourceFailure.require(n!=null && n.isObject() && n.size()==names.length);
        for(String name:names) SourceFailure.require(n.has(name));
    }
    public static String text(JsonNode n) { SourceFailure.require(n!=null && n.isString()); return n.stringValue(); }
    static boolean bool(JsonNode n) { SourceFailure.require(n!=null && n.isBoolean()); return n.booleanValue(); }
    static int integer(JsonNode n) { SourceFailure.require(n!=null && n.isIntegralNumber() && n.canConvertToInt()); return n.intValue(); }
    static void scalar(String s) {
        for(int i=0;i<s.length();i++) {
            char c=s.charAt(i);
            if(Character.isHighSurrogate(c)) { SourceFailure.require(i+1<s.length() && Character.isLowSurrogate(s.charAt(++i))); }
            else SourceFailure.require(!Character.isLowSurrogate(c));
        }
    }
    public static byte[] canonical(JsonNode n) {
        StringBuilder s=new StringBuilder(); append(s,n,0); return s.toString().getBytes(StandardCharsets.UTF_8);
    }
    private static void append(StringBuilder s,JsonNode n,int depth) {
        SourceFailure.require(n!=null && depth<=16);
        if(n.isObject()) {
            s.append('{'); var keys=new ArrayList<>(n.propertyNames());
            keys.sort((a,b) -> Arrays.compareUnsigned(a.getBytes(StandardCharsets.UTF_8),b.getBytes(StandardCharsets.UTF_8)));
            boolean first=true;
            for(String key:keys) { if(!first)s.append(','); first=false; quote(s,key);s.append(':');append(s,n.get(key),depth+1); }
            s.append('}');
        } else if(n.isArray()) { s.append('[');boolean first=true;for(JsonNode v:n) {if(!first)s.append(',');first=false;append(s,v,depth+1);}s.append(']'); }
        else if(n.isString()) quote(s,n.stringValue());
        else if(n.isIntegralNumber()) s.append(n.bigIntegerValue());
        else if(n.isBoolean()) s.append(n.booleanValue());
        else if(n.isNull()) s.append("null");
        else throw new SourceFailure(SourceFailure.Code.INVALID_ARGUMENT);
    }
    private static void quote(StringBuilder s,String v) {
        scalar(v);s.append('"');
        for(int i=0;i<v.length();i++) {
            char c=v.charAt(i);
            switch(c) {
                case '"' -> s.append("\\\""); case '\\' -> s.append("\\\\");
                case '\b' -> s.append("\\b"); case '\f' -> s.append("\\f"); case '\n' -> s.append("\\n");
                case '\r' -> s.append("\\r"); case '\t' -> s.append("\\t");
                default -> { if(c<32)s.append(String.format(Locale.ROOT,"\\u%04x",(int)c));else s.append(c); }
            }
        }
        s.append('"');
    }
    public static String sha(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch(java.security.NoSuchAlgorithmException ex) { throw new IllegalStateException("SHA256_UNAVAILABLE"); }
    }
}
