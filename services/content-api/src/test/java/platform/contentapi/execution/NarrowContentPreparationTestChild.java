package platform.contentapi.execution;

import java.nio.file.Files;
import java.nio.file.Path;
import platform.contentapi.management.SourceJson;
import static platform.contentapi.execution.NarrowContentPreparationFixtures.*;

/** Test-only hard-exit process. Its argument/output files are not admission receipts. */
public final class NarrowContentPreparationTestChild {
    public static void main(String[] args)throws Exception{
        var host=OwnershipFixtures.readBinding(Path.of(args[0]));
        var spec=specification(host,SourceJson.parse(Files.readAllBytes(Path.of(args[1]))));
        System.out.println("PID "+ProcessHandle.current().pid());
        try(var guard=InstallationHostGuard.acquire(host)){
            try{
                if(args[2].equals("INSPECT")){
                    var expected=expected(host,SourceJson.parse(Files.readAllBytes(Path.of(args[3]))));
                    var totals=new NarrowContentEvidenceContext(expected).inspect(guard,FROZEN);
                    System.out.println("INSPECTED "+totals.reservedBytes()+" "+totals.reservedEntries());
                }else{
                    var prepared=NarrowContentPreparation.prepare(guard,spec,(boundary,path)->{
                        if(boundary.name().equals(args[2])){
                            System.out.println("HALTED "+boundary.name());System.out.flush();
                            // No finally/close runs. This proves JVM interruption, not power-loss persistence.
                            Runtime.getRuntime().halt(43);
                        }
                    });
                    Files.write(Path.of(args[3]),expectedBytes(prepared.expected()));
                    System.out.println("PREPARED");
                }
            }catch(PreflightFailure failure){System.out.println("REJECTED "+failure.code());}
        }
    }
    private NarrowContentPreparationTestChild(){}
}
