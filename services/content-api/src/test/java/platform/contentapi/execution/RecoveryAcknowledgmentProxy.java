package platform.contentapi.execution;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Test-only PostgreSQL wire relay: drop one actual committed transaction's acknowledgment. */
final class RecoveryAcknowledgmentProxy implements AutoCloseable {
    private final ServerSocket listener;private final URI upstream;private final String action;
    private final List<Socket> sockets=new CopyOnWriteArrayList<>();final AtomicBoolean dropped=new AtomicBoolean();
    private final List<String> transactions=new CopyOnWriteArrayList<>();
    RecoveryAcknowledgmentProxy(String jdbcUrl,String action)throws IOException {
        upstream=URI.create(jdbcUrl.substring(5));this.action=action;listener=new ServerSocket(0,16,InetAddress.getLoopbackAddress());
        Thread.ofPlatform().daemon().start(()->{while(!listener.isClosed())try{Socket client=listener.accept(),server=new Socket(upstream.getHost(),upstream.getPort());sockets.add(client);sockets.add(server);Thread.ofPlatform().daemon().start(()->relay(client,server));}catch(IOException ignored){}});
    }
    String url(){return "jdbc:postgresql://127.0.0.1:"+listener.getLocalPort()+upstream.getPath();}
    private void relay(Socket client,Socket server) {
        try {
            client.setTcpNoDelay(true);server.setTcpNoDelay(true);
            // Flush whole protocol frames. Unbuffered type/length/body writes trigger Linux
            // Nagle/delayed-ACK stalls and can exhaust the application's short SQL transaction budget.
            var input=new DataInputStream(client.getInputStream());var output=new DataOutputStream(new BufferedOutputStream(server.getOutputStream()));
            // Negotiate the driver's SSL/GSS request before normal typed messages. Disposable server has no TLS.
            while(true){int length=input.readInt();byte[] body=input.readNBytes(length-4);if(body.length!=length-4)throw new EOFException();output.writeInt(length);output.write(body);output.flush();if(length!=8)break;int response=server.getInputStream().read();if(response!='N')throw new IOException("Unexpected encryption negotiation");client.getOutputStream().write(response);client.getOutputStream().flush();}
            var armed=new AtomicBoolean();var statements=new HashMap<String,String>();var lastStatement=new java.util.concurrent.atomic.AtomicReference<>("NONE");
            Thread.ofPlatform().daemon().start(()->{
                try {var incoming=new DataInputStream(server.getInputStream());var outgoing=new DataOutputStream(new BufferedOutputStream(client.getOutputStream()));long begun=0;while(true){int type=incoming.read();if(type<0)break;int length=incoming.readInt();byte[] body=incoming.readNBytes(length-4);if(body.length!=length-4)break;
                    if(type=='C'){String tag=new String(body,StandardCharsets.UTF_8);if(tag.equals("BEGIN\0"))begun=System.nanoTime();if(tag.equals("COMMIT\0") || tag.equals("ROLLBACK\0")){transactions.add(tag.substring(0,tag.length()-1)+" last="+lastStatement.get()+" millis="+(begun==0?0:TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-begun)));if(transactions.size()>128)transactions.removeFirst();}}
                    if(type=='C' && new String(body,StandardCharsets.UTF_8).equals("COMMIT\0") && armed.get() && dropped.compareAndSet(false,true)){client.close();server.close();return;}
                    outgoing.writeByte(type);outgoing.writeInt(length);outgoing.write(body);outgoing.flush();}
                }catch(IOException ignored){}finally{close(client);close(server);}
            });
            while(true){int type=input.read();if(type<0)break;int length=input.readInt();if(length<4 || length>32*1024*1024)throw new IOException("Oversized message");byte[] body=input.readNBytes(length-4);if(body.length!=length-4)break;
                if(type=='P'){int end=zero(body,0);statements.put(new String(body,0,end,StandardCharsets.UTF_8),new String(body,end+1,zero(body,end+1)-end-1,StandardCharsets.UTF_8));}
                if(type=='B'){int portal=zero(body,0),end=zero(body,portal+1);String statement=new String(body,portal+1,end-portal-1,StandardCharsets.UTF_8);String sql=statements.getOrDefault(statement,"");if(!Set.of("BEGIN","COMMIT","ROLLBACK").contains(sql))lastStatement.set(statementKind(sql));if(sql.startsWith("INSERT INTO ddip_management.audit_events") && new String(body,StandardCharsets.UTF_8).contains(action))armed.set(true);}
                output.writeByte(type);output.writeInt(length);output.write(body);output.flush();
            }
        }catch(IOException ignored){}finally{close(client);close(server);}
    }
    private static int zero(byte[] bytes,int start)throws IOException {for(int i=start;i<bytes.length;i++)if(bytes[i]==0)return i;throw new IOException("Missing terminator");}
    private static String statementKind(String sql){for(String table:List.of("artifacts","artifact_dependencies","stage_artifacts","attempts","runs","controls","worker_instances","coordinator","audit_events"))for(String verb:List.of("INSERT INTO","UPDATE"))if(sql.startsWith(verb+" ddip_management."+table+" ") || sql.startsWith(verb+" ddip_management."+table+"("))return verb.replace(' ','_')+"_"+table;return "READ_OR_OTHER";}
    List<String> transactionSummary(){var snapshot=List.copyOf(transactions);return snapshot.subList(Math.max(0,snapshot.size()-8),snapshot.size());}
    private static void close(Socket socket){try{socket.close();}catch(IOException ignored){}}
    public void close(){try{listener.close();}catch(IOException ignored){}sockets.forEach(RecoveryAcknowledgmentProxy::close);}
}
