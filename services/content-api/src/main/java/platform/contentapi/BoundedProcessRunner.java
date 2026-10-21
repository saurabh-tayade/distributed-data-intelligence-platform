package platform.contentapi;

import platform.contentapi.execution.AnalyticsRole;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.springframework.stereotype.Component;
import static platform.contentapi.ApiFailure.Code.*;

/** Owns admission, two stream readers, the child and observed descendants for one call. */
@AnalyticsRole
@Component
public class BoundedProcessRunner {
    public record Result(int exitCode, byte[] stdout) { }
    private final AnalyticsProperties properties;
    private final Semaphore permits;

    public BoundedProcessRunner(AnalyticsProperties properties) {
        this.properties = properties;
        permits = new Semaphore(properties.maxProcesses());
    }

    public Result run(List<String> command, Path directory) {
        if (!permits.tryAcquire()) throw new ApiFailure(ANALYTICS_CAPACITY_EXCEEDED);
        Process process = null;
        Drain stdout = null;
        Drain stderr = null;
        Set<ProcessHandle> descendants = new HashSet<>();
        try {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
            long deadline = System.nanoTime() + properties.timeout().toNanos();
            var builder = new ProcessBuilder(command).directory(directory.toFile());
            builder.environment().clear();
            builder.environment().putAll(java.util.Map.of("PATH", "/usr/bin:/bin", "TZ", "UTC", "LANG", "C.UTF-8"));
            process = builder.start();
            process.getOutputStream().close(); // The CLI must never wait for request input.
            stdout = new Drain(process.getInputStream(), properties.maxStdoutBytes(), true);
            stderr = new Drain(process.getErrorStream(), properties.maxStderrBytes(), false);
            stdout.start();
            stderr.start();
            while (true) {
                observeDescendants(process, descendants);
                if (stdout.failure != null) throw stdout.failure;
                if (stderr.failure != null) throw stderr.failure;
                if (!process.isAlive() && stdout.finished && stderr.finished) {
                    if (stdout.failure != null) throw stdout.failure;
                    if (stderr.failure != null) throw stderr.failure;
                    return new Result(process.exitValue(), stdout.bytes.toByteArray());
                }
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) throw new ApiFailure(ANALYTICS_TIMEOUT);
                // Also checks reader failure and descendants while the child is alive. No request queue.
                TimeUnit.NANOSECONDS.sleep(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(10)));
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
            throw new ApiFailure(ANALYTICS_EXECUTION_FAILED);
        } catch (IOException | SecurityException ignored) {
            throw new ApiFailure(ANALYTICS_RUNTIME_UNAVAILABLE);
        } finally {
            try {
                if (process != null) cleanup(process, descendants, stdout, stderr);
            } finally {
                permits.release();
            }
        }
    }

    private static void cleanup(Process process, Set<ProcessHandle> descendants, Drain stdout, Drain stderr) {
        // Clear interruption temporarily so cleanup can reap the process; restore it for the caller.
        boolean interrupted = Thread.interrupted();
        try {
            observeDescendants(process, descendants);
            descendants.forEach(handle -> {
                try { if (handle.isAlive()) handle.destroyForcibly(); }
                catch (RuntimeException ignored) { /* OS-dependent best effort; still reap the direct child. */ }
            });
            if (process.isAlive()) process.destroyForcibly();
            try {
                process.waitFor(1, TimeUnit.SECONDS);
                // Killing descendants first releases inherited stdout/stderr descriptors.
                for (Drain drain : new Drain[]{stdout, stderr}) {
                    if (drain != null && drain.thread != null) drain.thread.join(Duration.ofSeconds(1));
                }
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
            close(process.getInputStream());
            close(process.getErrorStream());
            try { process.getOutputStream().close(); } catch (IOException ignored) { }
        } finally {
            if (stdout != null && stdout.thread != null) stdout.thread.interrupt();
            if (stderr != null && stderr.thread != null) stderr.thread.interrupt();
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    private static void observeDescendants(Process process, Set<ProcessHandle> descendants) {
        try {
            process.descendants().forEach(descendants::add);
        } catch (RuntimeException ignored) {
            // Some OS sandboxes deny process enumeration. Always still terminate/reap the direct child.
        }
    }

    private static void close(InputStream stream) {
        try { stream.close(); } catch (IOException ignored) { }
    }

    private static final class Drain implements Runnable {
        private final InputStream stream;
        private final int maximum;
        private final boolean retain;
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private volatile boolean finished;
        private volatile ApiFailure failure;
        private Thread thread;

        Drain(InputStream stream, int maximum, boolean retain) {
            this.stream = stream;
            this.maximum = maximum;
            this.retain = retain;
        }
        void start() { thread = Thread.ofVirtual().name("analytics-stream").start(this); }

        @Override
        public void run() {
            try (stream) {
                byte[] buffer = new byte[8192];
                int total = 0;
                int count;
                while ((count = stream.read(buffer)) != -1) {
                    if (count > maximum - total) {
                        failure = new ApiFailure(ANALYTICS_RESPONSE_INVALID);
                        return;
                    }
                    total += count;
                    if (retain) bytes.write(buffer, 0, count);
                }
            } catch (IOException ignored) {
                failure = new ApiFailure(ANALYTICS_EXECUTION_FAILED);
            } finally {
                finished = true;
            }
        }
    }
}
