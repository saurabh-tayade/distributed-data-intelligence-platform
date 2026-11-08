package platform.contentapi.management;

import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import platform.contentapi.execution.*;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.management.SourceFixtures.*;

/** Reuses original live FETCH/Bronze loopback fixtures and installs real native runtimes. */
abstract class NarrowContentPipelineFixtures extends NarrowContentSilverReservationFixtures {
    static final String PIPELINE_PROFILE="strict-content-pipeline-test";
    Path processing,python;boolean emptyFeed;
    @Override void profile(String id,boolean content,String source){
        if(!content){super.profile(id,false,source);return;}
        try{
            processing=Path.of("../../processing").toRealPath();
            python=Path.of(Objects.requireNonNull(System.getenv("DDIP_TEST_PYTHON"),"DDIP_TEST_PYTHON")).toAbsolutePath();
            var manifest=RssFetchTestSupport.manifest(true);var entry=(ObjectNode)manifest.get("classpath").get(0);
            installedFetchJar=Files.copy(Path.of(entry.get("path").stringValue()),Files.createTempDirectory(temp,"strict-pipeline-runtime-").resolve("installed.jar")).toRealPath();entry.put("path",installedFetchJar.toString());
            NarrowContentPipelineTestSupport.registerChain(jdbc,id,RunFixtures.profile(true,source),CONTENT,manifest,processing,python);
        }catch(Exception ex){throw new IllegalStateException(ex);}
    }
    @Override void artifact(String id,ObjectNode evidence){
        if(id.equals(PUB))try{
            var p=new ProcessBuilder(System.getenv("DDIP_TEST_PYTHON"),"-I","-B",Path.of("../../processing/tests/managed_content_fixture.py").toRealPath().toString(),"reference",roots.get(REFS).toString(),REFS).redirectError(ProcessBuilder.Redirect.INHERIT).start();
            byte[] bytes=p.getInputStream().readAllBytes();assertThat(p.waitFor(30,java.util.concurrent.TimeUnit.SECONDS)).isTrue();assertThat(p.exitValue()).isZero();evidence=(ObjectNode)SourceJson.parse(bytes,512*1024);
        }catch(Exception ex){throw new IllegalStateException(ex);}
        super.artifact(id,evidence);
    }
    @BeforeEach void pipelineProfile()throws Exception{
        emptyFeed=false;
        // Probe import closure from actual installed source, never a hand-maintained test duplicate.
        var process=new ProcessBuilder(python.toString(),"-I","-B","-c","import sys,json;sys.path.insert(0,sys.argv[1]);from strict_native_content import MODULES;print(json.dumps(list(MODULES)))",processing.toString()).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        byte[] result=process.getInputStream().readAllBytes();assertThat(process.waitFor(30,java.util.concurrent.TimeUnit.SECONDS)).isTrue();assertThat(process.exitValue()).isZero();
        var config=SourceJson.object().put("strictContentPipelineVersion",1).put("runProfileId",CONTENT_PROFILE).put("runProfileSha256",digest(CONTENT_PROFILE))
                .put("contentRuntimeId","content").put("contentRuntimeSha256",digest("content")).put("controlBindingId",binding.id())
                .put("strictRuntimeSha256",NarrowContentPipelineTestSupport.protocolDigest()).put("python",python.toString()).put("processingRoot",processing.toString()).put("javaHome",System.getProperty("java.home"))
                .put("fetchLaunchProfileId",LAUNCH_PROFILE).put("bronzeLaunchProfileId",BRONZE_PROFILE).put("bronzeVerificationProfileId",VERIFY_PROFILE);
        config.set("versions",NarrowContentPipelineTestSupport.versions());var modules=config.putObject("modules");
        for(var name:SourceJson.parse(result)){String file=name.stringValue();modules.put(file,SourceJson.sha(Files.readAllBytes(processing.resolve(file))));}
        byte[] bytes=SourceJson.canonical(config);jdbc.update("INSERT INTO ddip_management.content_pipeline_profiles(profile_id,configuration_bytes,configuration_sha256) VALUES (?,?,?)",PIPELINE_PROFILE,bytes,SourceJson.sha(bytes));
    }
    @Override byte[] responsePayload(){if(emptyFeed)return "<rss version=\"2.0\"><channel/></rss>".getBytes(java.nio.charset.StandardCharsets.UTF_8);return ("<feed xmlns=\"http://www.w3.org/2005/Atom\"><title>test</title><entry><id>urn:strict:one</id><title>Strict pipeline story</title><link href=\"https://example.test/article\"/><updated>2026-10-30T00:00:00Z</updated><summary>Local fixture</summary></entry></feed>").getBytes(java.nio.charset.StandardCharsets.UTF_8);}
}
