package platform.contentapi;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;
import static platform.contentapi.TestSupport.*;
import static platform.contentapi.ApiFailure.Code.*;

class BoundedProcessRunnerTest {
    @TempDir Path temporary;
    private final Path working = Path.of(".").toAbsolutePath();

    @Test
    void successNonzeroAndMinimalEnvironment() {
        var runner = new BoundedProcessRunner(properties());
        assertEquals("ok", new String(runner.run(FakeChild.command("success", temporary.resolve("ok")), working).stdout()));
        assertEquals(7, runner.run(FakeChild.command("nonzero", temporary.resolve("exit")), working).exitCode());
        var environment = java.util.Set.of(new String(runner.run(FakeChild.command("environment", temporary.resolve("env")), working).stdout()).split(","));
        assertTrue(environment.containsAll(java.util.Set.of("LANG", "PATH", "TZ")));
        assertTrue(java.util.Set.of("LANG", "PATH", "TZ", "__CF_USER_TEXT_ENCODING").containsAll(environment));
    }

    @Test
    void drainsBothPipesWithoutDeadlock() {
        var runner = new BoundedProcessRunner(properties(Duration.ofSeconds(10), 100000, 100000));
        assertEquals(50000, runner.run(FakeChild.command("dual", temporary.resolve("dual")), working).stdout().length);
    }

    @ParameterizedTest
    @ValueSource(strings = {"stdout", "stderr", "sleep", "child"})
    void failuresAreBoundedAndReleaseProcessesAndCapacity(String mode) throws Exception {
        var runner = new BoundedProcessRunner(properties(Duration.ofSeconds(2), 1024, 1024));
        Path pidFile = temporary.resolve(mode);
        long start = System.nanoTime();
        var failure = assertThrows(ApiFailure.class, () -> runner.run(FakeChild.command(mode, pidFile), working));
        assertEquals(mode.equals("stdout") || mode.equals("stderr") ? ANALYTICS_RESPONSE_INVALID : ANALYTICS_TIMEOUT, failure.code());
        assertTrue(Duration.ofNanos(System.nanoTime() - start).toSeconds() < 7);
        assertDead(pidFile);
        if (mode.equals("child")) assertDead(Path.of(pidFile + ".child"));
        assertEquals(0, runner.run(FakeChild.command("success", temporary.resolve("reuse")), working).exitCode());
    }

    @Test
    void alreadyInterruptedRequestNeverStartsChildAndReleasesPermit() {
        var runner = new BoundedProcessRunner(properties());
        Path pidFile = temporary.resolve("not-started");
        Thread.currentThread().interrupt();
        try {
            assertEquals(ANALYTICS_EXECUTION_FAILED, assertThrows(ApiFailure.class,
                    () -> runner.run(FakeChild.command("success", pidFile), working)).code());
            assertTrue(Thread.currentThread().isInterrupted());
            assertFalse(Files.exists(pidFile));
        } finally {
            Thread.interrupted();
        }
        assertEquals(0, runner.run(FakeChild.command("success", temporary.resolve("reuse")), working).exitCode());
    }

    @Test
    void startupFailureReleasesCapacity() {
        var runner = new BoundedProcessRunner(properties());
        assertEquals(ANALYTICS_RUNTIME_UNAVAILABLE, assertThrows(ApiFailure.class,
                () -> runner.run(List.of(temporary.resolve("no-executable").toString()), working)).code());
        assertEquals(0, runner.run(FakeChild.command("success", temporary.resolve("reuse")), working).exitCode());
    }

    @Test
    void capacityRejectsImmediatelyAndInterruptionCleansUp() throws Exception {
        var runner = new BoundedProcessRunner(properties(Duration.ofSeconds(20), 1024, 1024));
        Path pidFile = temporary.resolve("blocking");
        AtomicReference<ApiFailure> outcome = new AtomicReference<>();
        AtomicReference<Boolean> interrupted = new AtomicReference<>(false);
        Thread thread = new Thread(() -> {
            try { runner.run(FakeChild.command("sleep", pidFile), working); }
            catch (ApiFailure failure) { outcome.set(failure); interrupted.set(Thread.currentThread().isInterrupted()); }
        });
        thread.start();
        try {
            awaitFile(pidFile);
            long start = System.nanoTime();
            assertEquals(ANALYTICS_CAPACITY_EXCEEDED, assertThrows(ApiFailure.class,
                    () -> runner.run(FakeChild.command("success", temporary.resolve("rejected")), working)).code());
            assertTrue(Duration.ofNanos(System.nanoTime() - start).toMillis() < 500);
            assertFalse(Files.exists(temporary.resolve("rejected")));
        } finally {
            thread.interrupt();
            thread.join(5000);
        }
        assertFalse(thread.isAlive());
        assertEquals(ANALYTICS_EXECUTION_FAILED, outcome.get().code());
        assertTrue(interrupted.get());
        assertDead(pidFile);
        assertEquals(0, runner.run(FakeChild.command("success", temporary.resolve("reuse")), working).exitCode());
    }

    static void awaitFile(Path path) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while ((!Files.exists(path) || Files.size(path) == 0) && System.nanoTime() < deadline) Thread.sleep(10);
        assertTrue(Files.exists(path) && Files.size(path) > 0, "Child did not start");
    }
    static void assertDead(Path path) throws Exception {
        awaitFile(path);
        long pid = Long.parseLong(Files.readString(path));
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false) && System.nanoTime() < deadline) Thread.sleep(10);
        assertFalse(ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false), "Child still running");
    }
}
