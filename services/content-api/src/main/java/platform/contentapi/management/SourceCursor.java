package platform.contentapi.management;

import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import tools.jackson.databind.JsonNode;

/** Authenticated pagination state only; this is not operator authentication. */
public final class SourceCursor {
    private final byte[] key;
    SourceCursor(String configured) {
        if(configured.isEmpty()) {key=new byte[32];new SecureRandom().nextBytes(key);}
        else {
            try {key=Base64.getUrlDecoder().decode(configured);SourceFailure.require(key.length==32);}
            catch(RuntimeException ex) {throw new platform.contentapi.execution.ManagementFailure(platform.contentapi.execution.ManagementFailure.Code.MANAGEMENT_CONFIGURATION_INVALID);}
        }
    }
    String encode(JsonNode payload) {
        String token=Base64.getUrlEncoder().withoutPadding().encodeToString(SourceJson.canonical(payload));
        String result=token+"."+Base64.getUrlEncoder().withoutPadding().encodeToString(sign(token));
        SourceFailure.require(result.length()<=512);return result;
    }
    JsonNode decode(String cursor) {
        try {
            SourceFailure.require(cursor!=null && cursor.length()<=512 && cursor.matches("[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]{43}"));
            String[] parts=cursor.split("\\.");byte[] signature=Base64.getUrlDecoder().decode(parts[1]);
            SourceFailure.require(MessageDigest.isEqual(sign(parts[0]),signature));
            byte[] raw=Base64.getUrlDecoder().decode(parts[0]);JsonNode node=SourceJson.parse(raw);
            SourceFailure.require(encode(node).equals(cursor));return node;
        } catch(RuntimeException ex) {throw new SourceFailure(SourceFailure.Code.INVALID_ARGUMENT);}
    }
    private byte[] sign(String text) {
        try {Mac mac=Mac.getInstance("HmacSHA256");mac.init(new SecretKeySpec(key,"HmacSHA256"));return mac.doFinal(text.getBytes(StandardCharsets.US_ASCII));}
        catch(GeneralSecurityException ex) {throw new IllegalStateException("HMAC_UNAVAILABLE");}
    }
}
