package platform.contentapi.execution;

import java.io.IOException;
import java.util.List;

/** Test-classpath-only access to the shared actual PostgreSQL commit-acknowledgment relay. */
public final class RecoveryAcknowledgmentTestSupport implements AutoCloseable {
    private final RecoveryAcknowledgmentProxy proxy;
    public RecoveryAcknowledgmentTestSupport(String url,String action)throws IOException {proxy=new RecoveryAcknowledgmentProxy(url,action);}
    public String url(){return proxy.url();}
    public boolean dropped(){return proxy.dropped.get();}
    public List<String> transactionSummary(){return proxy.transactionSummary();}
    @Override public void close(){proxy.close();}
}
