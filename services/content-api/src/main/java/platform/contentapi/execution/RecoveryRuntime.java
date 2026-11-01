package platform.contentapi.execution;

import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;

/** Pipeline-specific runtime qualification behind the unchanged C3c ownership protocol. */
final class RecoveryRuntime {
    final ReliabilityRuntime reliability;
    final RssContentRuntime content;
    private RecoveryRuntime(ReliabilityRuntime r,RssContentRuntime c){reliability=r;content=c;}
    static RecoveryRuntime load(JdbcTemplate j,ManagementTransactions tx,InstallationHostGuard g,WorkerRepository.Owner o,String id,JsonNode frozen){
        if("CONTENT_RSS_V1".equals(frozen.get("pipeline").stringValue()))return new RecoveryRuntime(null,RssContentRuntime.load(j,tx,g,o.runId(),frozen,id));
        return new RecoveryRuntime(ReliabilityRuntime.load(j,tx,g,o,id,frozen),null);
    }
    void check(){if(reliability!=null)reliability.check();else content.check();}
    static JsonNode contentJson(RssContentRuntime content){int version=content.config.get("contentRuntimeVersion").intValue();return SourceJson.object().put("contentRecoveryRuntimeVersion",version).put("digest",content.digest).put("bridgeSha256",SourceJson.sha(PreflightFiles.read(java.nio.file.Path.of(content.gold.silver.config.get("processingRoot").stringValue()).resolve(version==2?"bounded_content_producer.py":"content_recovery_verifier.py"),1024*1024,false)));}
    JsonNode json(){if(reliability!=null)return reliability.json();return contentJson(content);}
}
