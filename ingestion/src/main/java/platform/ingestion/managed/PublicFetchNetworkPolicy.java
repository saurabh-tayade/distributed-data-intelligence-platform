package platform.ingestion.managed;

import com.fasterxml.jackson.databind.node.ObjectNode;
import static platform.ingestion.managed.StrictFetchJson.object;

/** Exact immutable launch-v2 policy. Its digest is distinct from source/runtime evidence. */
public final class PublicFetchNetworkPolicy {
    public static final String ID = "ddip-public-feed-pinned-v1";
    public static final String PROVIDER = "platform.ingestion.managed.PublicFetchResolverProvider";
    public static final String ADDRESS_SHA256 = "85959d534b478bf1c8194e99f49a2c860f9fe9864978ed7118496913767cefcd";
    public static ObjectNode policy() {
        return object().put("policyVersion", 1).put("policyId", ID).put("resolverProvider", PROVIDER)
                .put("maxResolvedAddresses", 64).put("addressPolicyVersion", 1).put("addressPolicySha256", ADDRESS_SHA256)
                .put("addressFamily", "IPV4_ONLY").put("httpVersion", "HTTP_1_1").put("maxApplicationInvocations", 1)
                .put("transportRetryPolicy", "JDK_BOUNDED_PINNED_V1").put("redirects", false).put("proxies", false);
    }
    private PublicFetchNetworkPolicy() { }
}
