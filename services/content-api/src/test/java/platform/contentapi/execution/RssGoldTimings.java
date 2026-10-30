package platform.contentapi.execution;

import java.util.*;

/** Test-only monotonic timings. Fixed labels, no payloads, credentials, paths or runtime changes. */
public final class RssGoldTimings {
    private final Map<String,Long> elapsed=new TreeMap<>(),starts=new HashMap<>();
    private final Map<String,Integer> calls=new TreeMap<>(),children=new TreeMap<>();
    private long began;
    public void reset(){elapsed.clear();starts.clear();calls.clear();children.clear();began=System.nanoTime();}
    public final class Phase implements AutoCloseable {
        private final String name;private final long start=System.nanoTime();
        Phase(String name){this.name=name;}
        public void close(){elapsed.merge(name,System.nanoTime()-start,Long::sum);}
    }
    public Phase phase(String name){return new Phase(name);}
    public void boundary(String phase,String point){
        if(point.equals("BEFORE_AUTHORIZATION")){int call=calls.merge(phase,1,Integer::sum);starts.put(phase+"/child"+call,System.nanoTime());}
        if(point.equals("CHILD_STARTED"))children.merge(phase,1,Integer::sum);
        if(point.equals("AFTER_CHILD")){String key=phase+"/child"+calls.get(phase);Long start=starts.remove(key);if(start!=null)elapsed.merge(key,System.nanoTime()-start,Long::sum);}
    }
    public void emit(String test){
        var millis=new TreeMap<String,Long>();elapsed.forEach((k,v)->millis.put(k,v/1_000_000));
        System.out.println("D4_TIMING test="+test+" elapsed_ms="+((System.nanoTime()-began)/1_000_000)+" phases_ms="+millis+" child_starts="+children+" incomplete_child_intervals="+starts.size());
    }
}
