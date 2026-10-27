package platform.contentapi.management;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Test-only transparent HTTP/1 forwarding to real SeaweedFS; signed Host is preserved. */
final class BronzeFaultProxy implements AutoCloseable {
    final AtomicInteger puts=new AtomicInteger(),gets=new AtomicInteger(),deletes=new AtomicInteger();
    volatile String fault="none";final ServerSocket server;final URI backend;final ExecutorService threads=Executors.newVirtualThreadPerTaskExecutor();
    BronzeFaultProxy(String endpoint)throws IOException {backend=URI.create(endpoint);server=new ServerSocket(0,50,InetAddress.getByName("127.0.0.1"));threads.submit(()->{while(!server.isClosed())try{Socket client=server.accept();threads.submit(()->forward(client));}catch(IOException ignored){ }});}
    String endpoint(){return "http://127.0.0.1:"+server.getLocalPort();}
    private void forward(Socket client){try(client){client.setSoTimeout(15000);var input=client.getInputStream();byte[] headers=header(input);String text=new String(headers,StandardCharsets.ISO_8859_1);String[] lines=text.split("\r\n");String method=lines[0].split(" ")[0];boolean put=method.equals("PUT"),get=method.equals("GET");int p=put?puts.incrementAndGet():puts.get();if(get)gets.incrementAndGet();if(method.equals("DELETE"))deletes.incrementAndGet();
        int length=0;for(String line:lines)if(line.toLowerCase(Locale.ROOT).startsWith("content-length:"))length=Integer.parseInt(line.substring(line.indexOf(':')+1).trim());if(length<0 || length>6*1024*1024)throw new IOException();byte[] body=input.readNBytes(length);if(body.length!=length)throw new IOException();
        if(get && Set.of("generic-404","no-bucket","auth","io").contains(fault)){if(fault.equals("io"))return;respond(client,fault.equals("auth")?403:404,switch(fault){case "generic-404"->"NotFound";case "no-bucket"->"NoSuchBucket";default->"AccessDenied";});return;}
        if((put && fault.equals("before-put")) || (put && p%2==0 && fault.equals("between-puts")) || (get && p>=2 && fault.equals("after-puts"))){respond(client,503,"ServiceUnavailable");return;}
        try(var upstream=new Socket()){upstream.connect(new InetSocketAddress(backend.getHost(),backend.getPort()),5000);upstream.setSoTimeout(15000);var out=upstream.getOutputStream();for(String line:lines)if(!line.isEmpty() && !line.toLowerCase(Locale.ROOT).startsWith("connection:")){out.write(line.getBytes(StandardCharsets.ISO_8859_1));out.write("\r\n".getBytes(StandardCharsets.ISO_8859_1));}out.write("Connection: close\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1));out.write(body);out.flush();byte[] response=upstream.getInputStream().readNBytes(8*1024*1024);if(put && p%2==0 && fault.equals("metadata-ack"))return;client.getOutputStream().write(response);client.getOutputStream().flush();}
    }catch(IOException | RuntimeException ignored){ }}
    private static byte[] header(InputStream input)throws IOException {var b=new ByteArrayOutputStream();int end=0,v;while((v=input.read())!=-1){b.write(v);end=(end<<8)|v;if(b.size()>65536)throw new IOException();if(end==0x0d0a0d0a)return b.toByteArray();}throw new EOFException();}
    private static void respond(Socket client,int status,String code)throws IOException {byte[] body=("<Error><Code>"+code+"</Code><Message>Synthetic failure</Message></Error>").getBytes(StandardCharsets.UTF_8);client.getOutputStream().write(("HTTP/1.1 "+status+" Error\r\nContent-Type: application/xml\r\nContent-Length: "+body.length+"\r\nConnection: close\r\n\r\n").getBytes(StandardCharsets.ISO_8859_1));client.getOutputStream().write(body);}
    public void close(){try{server.close();}catch(IOException ignored){ }threads.shutdownNow();}
}
