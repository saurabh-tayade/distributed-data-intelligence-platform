package platform.contentapi.management;

/** Test-only process identity fixture. Never part of the production JAR or an execution capability. */
public final class ControlTestProcess {
    public static void main(String[] args)throws Exception {System.out.println("READY");System.out.flush();System.in.read();}
}
