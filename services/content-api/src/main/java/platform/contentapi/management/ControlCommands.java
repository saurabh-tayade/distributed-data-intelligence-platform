package platform.contentapi.management;

import java.util.UUID;
import static platform.contentapi.management.SourceFailure.require;

/** Explicit controls share one fixed conditional envelope. The conditional run version is part of command identity. */
public final class ControlCommands {
    private ControlCommands(){ }
    public record Command(UUID runId,UUID attemptId,long expectedVersion,String key,byte[] normalized) {
        public Command {normalized=normalized.clone();}
        @Override public byte[] normalized(){return normalized.clone();}
        public String action(){return SourceJson.parse(normalized).get("action").stringValue();}
        @Override public String toString(){return "ControlRequest[redacted]";}
    }
    public static Command parse(String run,byte[] bytes,String key,String ifMatch) {
        UUID id=UUID.fromString(SourceCommands.uuid(run));require(key!=null && key.matches("[A-Za-z0-9_-]{1,128}"));
        var body=SourceJson.parse(bytes);SourceJson.fields(body,"action","attemptId","quiescenceConfirmed");
        String action=SourceJson.text(body.get("action"));require(action.equals("RECONCILE") || action.equals("RESUME"));UUID attempt=null;
        if(action.equals("RECONCILE")){require(SourceJson.bool(body.get("quiescenceConfirmed")));attempt=UUID.fromString(SourceCommands.uuid(SourceJson.text(body.get("attemptId"))));}
        else require(body.get("attemptId").isNull() && body.get("quiescenceConfirmed").isNull());
        if(ifMatch==null)throw new SourceFailure(SourceFailure.Code.PRECONDITION_REQUIRED);
        require(ifMatch.matches("\"(0|[1-9][0-9]{0,18})\""));long version;
        try{version=Long.parseLong(ifMatch.substring(1,ifMatch.length()-1));}catch(NumberFormatException ex){throw new SourceFailure(SourceFailure.Code.INVALID_ARGUMENT);}
        var normalized=SourceJson.object().put("action",action).put("expectedVersion",Long.toString(version)).put("runId",id.toString());
        if(attempt==null)normalized.putNull("attemptId").putNull("quiescenceConfirmed");else normalized.put("attemptId",attempt.toString()).put("quiescenceConfirmed",true);
        return new Command(id,attempt,version,key,SourceJson.canonical(normalized));
    }
}
