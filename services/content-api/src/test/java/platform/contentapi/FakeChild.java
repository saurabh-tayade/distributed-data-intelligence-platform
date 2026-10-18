package platform.contentapi;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Deterministic JVM child: unit tests need neither Python nor a Gold publication. */
public final class FakeChild {
    static List<String> command(String mode, Path marker) {
        return List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-cp",
                Path.of("target/test-classes").toAbsolutePath().toString(), FakeChild.class.getName(), mode, marker.toString());
    }
    public static void main(String[] args) throws Exception {
        String mode = args[0];
        Path marker = Path.of(args[1]);
        Files.writeString(marker, Long.toString(ProcessHandle.current().pid()));
        switch (mode) {
            case "success" -> System.out.print("ok");
            case "nonzero" -> { System.out.print("partial"); System.exit(7); }
            case "stdout" -> { System.out.write(new byte[200000]); System.out.flush(); Thread.sleep(30000); }
            case "stderr" -> { System.err.write(new byte[200000]); System.err.flush(); Thread.sleep(30000); }
            case "dual" -> {
                Thread err = new Thread(() -> { for (int i = 0; i < 50000; i++) System.err.print('e'); });
                err.start();
                for (int i = 0; i < 50000; i++) System.out.print('o');
                err.join();
            }
            case "environment" -> System.out.print(String.join(",", new java.util.TreeSet<>(System.getenv().keySet())));
            case "child" -> {
                new ProcessBuilder(command("sleep", Path.of(marker + ".child"))).inheritIO().start();
                Thread.sleep(30000);
            }
            case "sleep" -> { System.out.print("partial"); System.out.flush(); Thread.sleep(30000); }
            default -> throw new IllegalArgumentException();
        }
    }
}
