package platform.contentapi.management;

import java.util.UUID;
import static platform.contentapi.management.SourceFailure.require;

/** C3a admits one fixed control. The conditional run version is part of command identity. */
public final class ControlCommands {
    private ControlCommands(){ }
    public record Command(UUID runId,UUID attemptId,long expectedVersion,String key,byte[] normalized) {
        public Command {normalized=normalized.clone();}
        @Override public byte[] normalized(){return normalized.clone();}
        @Override public String toString(){return "ReconcileRequest[redacted]";}
    }
    public static Command parse(String run,byte[] bytes,String key,String ifMatch) {
        UUID id=UUID.fromString(SourceCommands.uuid(run));require(key!=null && key.matches("[A-Za-z0-9_-]{1,128}"));
        var body=SourceJson.parse(bytes);SourceJson.fields(body,"action","attemptId","quiescenceConfirmed");
        require(SourceJson.text(body.get("action")).equals("RECONCILE") && SourceJson.bool(body.get("quiescenceConfirmed")));
        UUID attempt=UUID.fromString(SourceCommands.uuid(SourceJson.text(body.get("attemptId"))));
        if(ifMatch==null)throw new SourceFailure(SourceFailure.Code.PRECONDITION_REQUIRED);
        require(ifMatch.matches("\"(0|[1-9][0-9]{0,18})\""));long version;
        try{version=Long.parseLong(ifMatch.substring(1,ifMatch.length()-1));}catch(NumberFormatException ex){throw new SourceFailure(SourceFailure.Code.INVALID_ARGUMENT);}
        var normalized=SourceJson.object().put("action","RECONCILE").put("attemptId",attempt.toString()).put("expectedVersion",Long.toString(version)).put("quiescenceConfirmed",true).put("runId",id.toString());
        return new Command(id,attempt,version,key,SourceJson.canonical(normalized));
    }
}
