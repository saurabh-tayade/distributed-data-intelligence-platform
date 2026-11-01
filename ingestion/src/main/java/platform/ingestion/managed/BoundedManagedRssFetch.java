package platform.ingestion.managed;

import java.nio.file.Path;

/** Fixed versioned entry point; the native fetch and journal contract are unchanged. */
public final class BoundedManagedRssFetch {
    public static void main(String[] args){int status=2;try{if(args.length==1)try(var sink=ReservedEvidence.open(Path.of(args[0]))){sink.checkActive();status=ManagedRssFetch.execute(Path.of(args[0]));}}catch(Exception ignored){}System.exit(status);}
    private BoundedManagedRssFetch(){}
}
