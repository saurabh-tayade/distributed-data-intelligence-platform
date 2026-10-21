package platform.contentapi.importing;
public final class ImportProcessChild {
    public static void main(String[] args) throws Exception {
        switch(args[0]) {
            case "good" -> {System.in.readAllBytes();System.out.print("{}");}
            case "large" -> {System.out.write(new byte[600000]);Thread.sleep(5000);}
            case "error" -> {System.err.write(new byte[70000]);Thread.sleep(5000);}
            case "joined" -> {Process p=new ProcessBuilder(java.nio.file.Path.of(System.getProperty("java.home"),"bin/java").toString(),"-version").redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.DISCARD).start();p.waitFor();System.out.print("{}");}
            case "wait" -> Thread.sleep(30000);
            case "fork" -> {new ProcessBuilder(java.nio.file.Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",System.getProperty("java.class.path"),ImportProcessChild.class.getName(),"wait").start();Thread.sleep(30000);}
            default -> throw new IllegalArgumentException();
        }
    }
}
