package platform.ingestion.managed;

import java.nio.file.Path;

/** Fixed versioned entry point; Bronze GET/PUT and receipt contracts are unchanged. */
public final class BoundedManagedBronze {
    public static void main(String[] args){int status=2;try{if(args.length==1)try(var sink=ReservedEvidence.open(Path.of(args[0]))){sink.checkActive();status=ManagedBronze.execute(Path.of(args[0]));}}catch(Exception ignored){}System.exit(status);}
    private BoundedManagedBronze(){}
}
