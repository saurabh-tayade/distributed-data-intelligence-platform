package platform.contentapi.execution;

import java.nio.channels.FileChannel;
import java.nio.file.*;

/** Test-only external writer-lock holder; never invoked by the assessment. */
public final class NarrowContentAssessmentLockChild {
    public static void main(String[] args)throws Exception{
        try(var channel=FileChannel.open(Path.of(args[0]),StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS)){
            if(args.length==2){try(var lock=channel.tryLock()){System.out.println(lock==null?"ASSESSMENT_LOCK_RETAINED":"ASSESSMENT_LOCK_LOST");}return;}
            try(var lock=channel.lock()){System.out.println("ASSESSMENT_LOCK_HELD");System.out.flush();System.in.read();}
        }
    }
    private NarrowContentAssessmentLockChild(){}
}
