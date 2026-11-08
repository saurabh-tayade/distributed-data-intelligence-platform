package platform.ingestion.managed;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.InetAddress;
import java.net.Inet6Address;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PublicFetchAddressPolicyTest {
    static InetAddress ip(long value) throws Exception {
        return InetAddress.getByAddress(new byte[]{(byte)(value >>> 24), (byte)(value >>> 16), (byte)(value >>> 8), (byte)value});
    }
    static long numeric(String text) {
        long value = 0;
        for (String octet : text.split("\\.")) value = (value << 8) | Integer.parseInt(octet);
        return value;
    }
    @Test void exactReviewedSnapshotAllPrefixesBoundariesAndNeighbors() throws Exception {
        Path snapshot = Path.of("../docs/architecture/0046-public-fetch-ipv4-deny-snapshot.json");
        byte[] bytes = Files.readAllBytes(snapshot);
        assertEquals(PublicFetchAddressPolicy.SNAPSHOT_SHA256, StrictFetchJson.sha(bytes));
        var root = new ObjectMapper().readTree(bytes);
        List<String> prefixes = new ArrayList<>();
        for (String section : List.of("specialPurposeRows", "additionalDeniedRanges"))
            for (var row : root.get(section)) for (var prefix : row.get("prefixes")) prefixes.add(prefix.textValue());
        assertEquals(27, prefixes.size());
        assertEquals(prefixes, PublicFetchAddressPolicy.DENIED);
        for (String prefix : prefixes) {
            String[] parts = prefix.split("/");
            long first = numeric(parts[0]);
            long last = first + (1L << (32 - Integer.parseInt(parts[1]))) - 1;
            for (long candidate : new long[]{first, last, Math.max(0, first - 1), Math.min(0xffffffffL, last + 1)}) {
                boolean denied = prefixes.stream().anyMatch(p -> {
                    String[] range = p.split("/");
                    long base = numeric(range[0]), end = base + (1L << (32 - Integer.parseInt(range[1]))) - 1;
                    return candidate >= base && candidate <= end;
                });
                assertEquals(!denied, PublicFetchAddressPolicy.permitted(ip(candidate)), prefix + ": " + candidate);
            }
        }
        for (String candidate : List.of("1.1.1.1", "8.8.8.8", "192.0.1.1", "192.0.3.1", "223.255.255.255"))
            assertTrue(PublicFetchAddressPolicy.permitted(ip(numeric(candidate))), candidate);
    }
    @Test void wholeSetIsBoundedClosedDeduplicatedAndSortedByUnsignedBytes() throws Exception {
        var a = ip(numeric("8.8.8.8")); var b = ip(numeric("1.1.1.1"));
        var closed = new AtomicBoolean();
        var selected = PublicFetchAddressPolicy.choose("pinned.test.invalid", Stream.of(a, b, b).onClose(() -> closed.set(true)));
        assertTrue(closed.get());
        assertArrayEquals(b.getAddress(), selected.getAddress());
        assertEquals("pinned.test.invalid", selected.getHostName());
        assertThrows(Exception.class, () -> PublicFetchAddressPolicy.choose("pinned.test.invalid", Stream.of(a, ip(numeric("127.0.0.1")))));
        assertThrows(Exception.class, () -> PublicFetchAddressPolicy.choose("pinned.test.invalid", Stream.empty()));
        assertThrows(Exception.class, () -> PublicFetchAddressPolicy.choose("pinned.test.invalid", Stream.of((InetAddress)null)));
        var count = new AtomicInteger(); var overflowClosed = new AtomicBoolean();
        assertThrows(Exception.class, () -> PublicFetchAddressPolicy.choose("pinned.test.invalid",
                Stream.generate(() -> { count.incrementAndGet(); return a; }).onClose(() -> overflowClosed.set(true))));
        assertEquals(65, count.get()); assertTrue(overflowClosed.get());
        assertEquals(a, PublicFetchAddressPolicy.choose("pinned.test.invalid", Stream.generate(() -> a).limit(64)));
    }
    @Test void allActualIpv6IncludingMappedTranslationAndScopesReject() throws Exception {
        for (byte[] bytes : List.of(new byte[16], new byte[]{0, 0x64, (byte)0xff, (byte)0x9b, 0,0,0,0,0,0,0,0,8,8,8,8},
                new byte[]{0,0,0,0,0,0,0,0,0,0,(byte)255,(byte)255,8,8,8,8})) {
            assertFalse(PublicFetchAddressPolicy.permitted(Inet6Address.getByAddress(null, bytes, 0)));
            assertFalse(PublicFetchAddressPolicy.permitted(Inet6Address.getByAddress(null, bytes, 3)));
        }
        // JDK normalization can lose mapped wire form; actual Inet4Address still gets full IPv4 checks.
        assertFalse(PublicFetchAddressPolicy.permitted(InetAddress.getByAddress(
                new byte[]{0,0,0,0,0,0,0,0,0,0,(byte)255,(byte)255,127,0,0,1})));
    }
}
