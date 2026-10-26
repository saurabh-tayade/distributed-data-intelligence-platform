package platform.contentapi.execution;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;

/** Wire-level fixture checks supplement the packaged cases that prove actual PostgreSQL commits. */
class RecoveryAcknowledgmentProxyTest {
    @ParameterizedTest @ValueSource(strings={"RECOVERY_HANDOFF","RECOVERY_FINALIZED"})
    void forwardsUnrelatedFramesAndDropsOnlyTheSelectedCommit(String action)throws Exception {
        try(var backend=new ServerSocket(0,16,InetAddress.getLoopbackAddress());
            var proxy=new RecoveryAcknowledgmentProxy("jdbc:postgresql://127.0.0.1:"+backend.getLocalPort()+"/fixture",action);
            var client=new Socket("127.0.0.1",URI.create(proxy.url().substring(5)).getPort())) {
            backend.setSoTimeout(3000);client.setSoTimeout(3000);
            var sent=new DataOutputStream(new BufferedOutputStream(client.getOutputStream()));var received=new DataInputStream(client.getInputStream());
            sent.writeInt(8);sent.writeInt(80877103);sent.flush();
            try(var server=backend.accept()) {
                server.setSoTimeout(3000);var forwarded=new DataInputStream(server.getInputStream());var replies=new DataOutputStream(new BufferedOutputStream(server.getOutputStream()));
                assertThat(forwarded.readInt()).isEqualTo(8);assertThat(forwarded.readInt()).isEqualTo(80877103);replies.writeByte('N');replies.flush();assertThat(received.read()).isEqualTo('N');
                sent.writeInt(12);sent.writeLong(196608L);sent.flush();assertThat(forwarded.readInt()).isEqualTo(12);assertThat(forwarded.readLong()).isEqualTo(196608L);
                for(String sql:List.of("SELECT ? FROM ddip_management.audit_events","INSERT INTO ddip_management.worker_instances VALUES (?)")) {
                    exchange(sent,forwarded,sql,action);complete(replies,received,"COMMIT");assertThat(proxy.dropped).isFalse();
                }
                exchange(sent,forwarded,"INSERT INTO ddip_management.audit_events VALUES (?)","RECOVERY_SLOT_RELEASED");complete(replies,received,"COMMIT");assertThat(proxy.dropped).isFalse();
                exchange(sent,forwarded,"INSERT INTO ddip_management.audit_events VALUES (?)",action);
                complete(replies,received,"INSERT 0 1");assertThat(proxy.dropped).isFalse();
                frame(replies,'C',cstring("COMMIT"));assertThat(received.read()).isEqualTo(-1);assertThat(proxy.dropped).isTrue();
            }
        }
    }
    private static void exchange(DataOutputStream sent,DataInputStream forwarded,String sql,String action)throws Exception {
        var parse=new ByteArrayOutputStream();var p=new DataOutputStream(parse);p.write(cstring("fixture"));p.write(cstring(sql));p.writeShort(0);
        var bind=new ByteArrayOutputStream();var b=new DataOutputStream(bind);b.writeByte(0);b.write(cstring("fixture"));b.writeShort(0);b.writeShort(1);byte[] value=action.getBytes(StandardCharsets.UTF_8);b.writeInt(value.length);b.write(value);b.writeShort(0);
        frame(sent,'P',parse.toByteArray());frame(sent,'B',bind.toByteArray());assertFrame(forwarded,'P',parse.toByteArray());assertFrame(forwarded,'B',bind.toByteArray());
    }
    private static byte[] cstring(String text){return (text+'\0').getBytes(StandardCharsets.UTF_8);}
    private static void frame(DataOutputStream sent,int type,byte[] body)throws IOException {sent.writeByte(type);sent.writeInt(body.length+4);sent.write(body);sent.flush();}
    private static void assertFrame(DataInputStream received,int type,byte[] body)throws IOException {assertThat(received.read()).isEqualTo(type);assertThat(received.readInt()).isEqualTo(body.length+4);assertThat(received.readNBytes(body.length)).containsExactly(body);}
    private static void complete(DataOutputStream replies,DataInputStream received,String tag)throws IOException {byte[] body=cstring(tag);frame(replies,'C',body);assertFrame(received,'C',body);}
}
