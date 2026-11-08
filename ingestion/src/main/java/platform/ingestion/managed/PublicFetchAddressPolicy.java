package platform.ingestion.managed;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.TreeSet;
import java.util.List;
import java.util.stream.Stream;

/** Fixed deny-all snapshot from ADR 0046; no live registry or caller-supplied table. */
final class PublicFetchAddressPolicy {
    static final String SNAPSHOT_SHA256 = "85959d534b478bf1c8194e99f49a2c860f9fe9864978ed7118496913767cefcd";
    static final int MAX_ADDRESSES = 64;
    // Keep every original prefix, including overlaps and globally reachable exceptions.
    static final List<String> DENIED = List.of(
        "0.0.0.0/8", "0.0.0.0/32", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8",
        "169.254.0.0/16", "172.16.0.0/12", "192.0.0.0/24", "192.0.0.0/29", "192.0.0.8/32",
        "192.0.0.9/32", "192.0.0.10/32", "192.0.0.170/32", "192.0.0.171/32", "192.0.2.0/24",
        "192.31.196.0/24", "192.52.193.0/24", "192.88.99.0/24", "192.88.99.2/32", "192.168.0.0/16",
        "192.175.48.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24",
        "240.0.0.0/4", "255.255.255.255/32", "224.0.0.0/4"
    );
    private static final long[][] PREFIXES = DENIED.stream().map(cidr -> {
        String[] parts = cidr.split("/"), octets = parts[0].split("\\.");
        long base = 0;
        for (String octet : octets) base = (base << 8) | Integer.parseInt(octet);
        long mask = (0xffffffffL << (32 - Integer.parseInt(parts[1]))) & 0xffffffffL;
        return new long[]{base, mask};
    }).toArray(long[][]::new);

    static boolean permitted(InetAddress address) {
        if (!(address instanceof Inet4Address)) return false;
        byte[] bytes = address.getAddress();
        if (bytes.length != 4) return false;
        long value = 0;
        for (byte b : bytes) value = (value << 8) | Byte.toUnsignedInt(b);
        for (long[] prefix : PREFIXES) if ((value & prefix[1]) == prefix[0]) return false;
        return true;
    }

    /** Consume and close at most 65 answers, validating the entire bounded set before choosing. */
    static InetAddress choose(String host, Stream<InetAddress> answers) throws UnknownHostException {
        if (answers == null) throw new UnknownHostException("PUBLIC_FETCH_ADDRESS_REJECTED");
        var unique = new TreeSet<byte[]>(Arrays::compareUnsigned);
        try (answers) {
            var iterator = answers.iterator();
            int count = 0;
            while (iterator.hasNext()) {
                InetAddress address = iterator.next();
                if (++count > MAX_ADDRESSES || !permitted(address))
                    throw new UnknownHostException("PUBLIC_FETCH_ADDRESS_REJECTED");
                unique.add(address.getAddress());
            }
        }
        if (unique.isEmpty()) throw new UnknownHostException("PUBLIC_FETCH_ADDRESS_REJECTED");
        return InetAddress.getByAddress(host, unique.first());
    }
    private PublicFetchAddressPolicy() { }
}
