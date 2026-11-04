package platform.contentapi.execution;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.UUID;
import platform.contentapi.management.SourceJson;
import static platform.contentapi.execution.PreflightFailure.need;
import static platform.contentapi.execution.PreflightFailure.Code.*;

/**
 * Inactive filesystem preparation, never SQL ownership or permission to execute. The caller must
 * first acknowledge durable PREPARING for this exact attempt and independently validate the spec.
 * No existing attempt root is adopted, repaired or retried. Interrupted evidence is retained.
 */
final class NarrowContentPreparation {
    record Specification(String policy,UUID run,UUID attempt,String source,
                         InstallationHostGuard.Binding host,NarrowContentEvidenceContext.FrozenIdentities frozen) {
        Specification {
            need(NarrowContentProfile.POLICY.equals(policy) && run!=null && attempt!=null && host!=null && frozen!=null
                    && source!=null && source.matches("[a-z0-9][a-z0-9_-]{0,127}"),INPUT_INVALID);
        }
        @Override public String toString(){return "NarrowContentPreparationSpecification[redacted]";}
    }
    /** Preparation data only. A durable READY record and current owner checks remain mandatory. */
    record Prepared(NarrowContentEvidenceContext.Expected expected,
                    NarrowContentEvidenceContext.FrozenIdentities independentlySupplied) {
        Prepared {need(expected!=null && independentlySupplied!=null && expected.frozen().equals(independentlySupplied),INPUT_INVALID);}
        @Override public String toString(){return "NarrowContentPrepared[redacted]";}
    }
    enum Boundary {
        ANCESTOR_CREATED, ANCESTOR_PARENT_FORCED, ROOT_CREATED, ROOT_PARENT_FORCED,
        LOCK_CREATED, LOCK_FORCED, LOCK_DIRECTORY_FORCED,
        INITIAL_PREFIX_CREATED, INITIAL_PREFIX_WRITTEN, INITIAL_PREFIX_FORCED, INITIAL_RENAMED, INITIAL_DIRECTORY_FORCED,
        FIXED_PREFIX_CREATED, FIXED_PREFIX_WRITTEN, FIXED_PREFIX_FORCED, FIXED_RENAMED, FIXED_DIRECTORY_FORCED,
        ANCESTOR_FORCED, BEFORE_INSPECTION, INSPECTED
    }
    interface Fault {void at(Boundary boundary,Path path);}
    private static final Fault NO_FAULT=(boundary,path)->{};

    static Prepared prepare(InstallationHostGuard guard,Specification specification){return prepare(guard,specification,NO_FAULT);}
    static Prepared prepare(InstallationHostGuard guard,Specification specification,Fault fault){
        need(specification!=null && guard!=null && fault!=null,INPUT_INVALID);
        need(specification.host().equals(guard.binding()),RUNTIME_MISMATCH);
        try{
            guard.checkHeld(specification.host().installationId());
            Path root=specification.host().root().resolve("runs/"+specification.run()+"/attempts/"+specification.attempt());
            // This check is followed by CREATE_NEW-equivalent mkdir for the root, never an exists/adopt flow.
            need(!Files.exists(root,LinkOption.NOFOLLOW_LINKS),INPUT_CHANGED);
            Path path=specification.host().root();
            for(Path segment:specification.host().root().relativize(root)){
                path=path.resolve(segment);boolean leaf=path.equals(root);
                try{
                    Files.createDirectory(path,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
                    fault.at(leaf?Boundary.ROOT_CREATED:Boundary.ANCESTOR_CREATED,path);
                    PreparedCommand.forceDirectory(path.getParent());
                    fault.at(leaf?Boundary.ROOT_PARENT_FORCED:Boundary.ANCESTOR_PARENT_FORCED,path.getParent());
                }catch(FileAlreadyExistsException ex){need(!leaf,INPUT_CHANGED);}
                PreflightFiles.privateEntry(path,true);
            }
            var rootIdentity=InstallationHostGuard.identity(root);
            Path lock=root.resolve(ContentEvidenceBudget.LOCK);
            try(var channel=FileChannel.open(lock,Set.of(StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS),
                    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))){
                fault.at(Boundary.LOCK_CREATED,lock);channel.force(true);fault.at(Boundary.LOCK_FORCED,lock);
            }
            var lockIdentity=InstallationHostGuard.identity(lock);
            PreparedCommand.forceDirectory(root);fault.at(Boundary.LOCK_DIRECTORY_FORCED,root);
            ContentEvidenceBudget.initializeFresh(guard,specification.run(),specification.attempt(),rootIdentity,lockIdentity,
                    boundary->fault.at(boundary(boundary,false),root));
            var plan=ContentOperationPlan.original(root,specification.run(),specification.attempt(),specification.source());
            ContentEvidenceBudget.reservePlan(plan,boundary->fault.at(boundary(boundary,true),root));
            // Force all ancestor entries even when a directory predated this preparation call.
            for(Path current=root;current.startsWith(specification.host().root());current=current.getParent()){
                PreflightFiles.privateEntry(current,true);PreparedCommand.forceDirectory(current);
                fault.at(Boundary.ANCESTOR_FORCED,current);
                if(current.equals(specification.host().root()))break;
            }
            // Compute the expected canonical ledger from the plan and freshly captured identity.
            // Its digest is not learned from whatever bytes happen to be present on disk.
            var ledger=SourceJson.object().put("version",1).put("treeKind","ATTEMPT")
                    .put("runId",specification.run().toString()).put("attemptId",specification.attempt().toString())
                    .put("rootDevice",Long.toString(rootIdentity.device())).put("rootInode",Long.toString(rootIdentity.inode()));
            plan.apply(ledger);
            var expected=new NarrowContentEvidenceContext.Expected(specification.policy(),specification.run(),specification.attempt(),
                    NarrowContentEvidenceContext.PlanKind.ORIGINAL,specification.source(),specification.host(),rootIdentity,lockIdentity,
                    specification.frozen(),SourceJson.sha(SourceJson.canonical(ledger)));
            fault.at(Boundary.BEFORE_INSPECTION,root);
            new NarrowContentEvidenceContext(expected).inspect(guard,specification.frozen());
            fault.at(Boundary.INSPECTED,root);
            return new Prepared(expected,specification.frozen());
        }catch(PreflightFailure ex){throw ex;}
        catch(FileAlreadyExistsException ex){throw new PreflightFailure(INPUT_CHANGED);}
        catch(IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
        catch(RuntimeException ex){throw new PreflightFailure(INPUT_CHANGED);}
    }
    private static Boundary boundary(ContentEvidenceBudget.StoreBoundary boundary,boolean fixed){
        return switch(boundary){
            case PREFIX_CREATED->fixed?Boundary.FIXED_PREFIX_CREATED:Boundary.INITIAL_PREFIX_CREATED;
            case PREFIX_WRITTEN->fixed?Boundary.FIXED_PREFIX_WRITTEN:Boundary.INITIAL_PREFIX_WRITTEN;
            case PREFIX_FORCED->fixed?Boundary.FIXED_PREFIX_FORCED:Boundary.INITIAL_PREFIX_FORCED;
            case RENAMED->fixed?Boundary.FIXED_RENAMED:Boundary.INITIAL_RENAMED;
            case DIRECTORY_FORCED->fixed?Boundary.FIXED_DIRECTORY_FORCED:Boundary.INITIAL_DIRECTORY_FORCED;
        };
    }
    private NarrowContentPreparation(){}
}
