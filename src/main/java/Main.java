import manager.MigrationManager;
import manager.MultiThreadedMigrationManager;
import manager.SingleThreadedMigrationManager;

import java.io.PrintWriter;
import java.util.Scanner;

public class Main {
    private static final String DEFAULT_URL = "http://localhost:8080";
    private static final String SOURCE_STORAGE_URL_SUFFIX = "/oldStorage";
    private static final String TARGET_STORAGE_URL_SUFFIX = "/newStorage";

    public static void main(String[] args) {
        Scanner input = new Scanner(System.in);
        PrintWriter output = new PrintWriter(System.out);
        output.printf("Please enter source storage url. Default: %s.\n", DEFAULT_URL);
        output.flush();
        String readSourceStorageUrl = input.nextLine();
        output.printf("Please enter target storage url. Default: %s.\n", DEFAULT_URL);
        output.flush();
        String readTargetStorageUrl = input.nextLine();
        output.println("Overwrite files that are already in target storage? (y, n) Default: n.");
        output.flush();
        String readOverwrite = input.nextLine();
        output.println("Multithreaded? (y, n) Default: n.");
        String readMultithreaded = input.nextLine();
        output.flush();
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
        output.printf("Migration %s.", result);
        output.close();
        input.close();
    }
}
