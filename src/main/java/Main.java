import manager.MigrationManager;
import manager.MultiThreadedMigrationManager;
import manager.SingleThreadedMigrationManager;

import java.io.Console;

public class Main {
    private static final String DEFAULT_URL = "http://localhost:8080";
    private static final String SOURCE_STORAGE_URL_SUFFIX = "/oldStorage";
    private static final String TARGET_STORAGE_URL_SUFFIX = "/newStorage";

    public static void main(String[] args) {
        Console console = System.console();
        console.printf("Please enter source storage url. Default: %s.\n", DEFAULT_URL);
        console.flush();
        String readSourceStorageUrl = console.readLine();
        console.printf("Please enter target storage url. Default: %s.\n", DEFAULT_URL);
        console.flush();
        String readTargetStorageUrl = console.readLine();
        console.printf("Overwrite files that are already in target storage? (y, n) Default: n.\n");
        console.flush();
        String readOverwrite = console.readLine();
        console.readLine("Multithreaded? (y, n) Default: y.");
        console.flush();
        String readMultithreaded = console.readLine();
        String sourceStorageUrl = (readSourceStorageUrl.isEmpty() ? DEFAULT_URL : readSourceStorageUrl)
                + SOURCE_STORAGE_URL_SUFFIX;
        String targetStorageUrl = (readTargetStorageUrl.isEmpty() ? DEFAULT_URL : readTargetStorageUrl)
                + TARGET_STORAGE_URL_SUFFIX;
        boolean overwrite = !readOverwrite.isEmpty() && Character.toLowerCase(readOverwrite.charAt(0)) == 'y';
        boolean multithreaded = readMultithreaded.isEmpty() || Character.toLowerCase(readMultithreaded.charAt(0)) == 'y';
        MigrationManager migrationManager = multithreaded
                ? new MultiThreadedMigrationManager()
                : new SingleThreadedMigrationManager();
        String result = migrationManager.proceedMigration(sourceStorageUrl, targetStorageUrl, overwrite)
                ? "done"
                : "failed";
        console.printf("Migration %s.", result);
    }
}
