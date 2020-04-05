import manager.MigrationManager;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws Exception {
        MigrationManager migrationManager = new MigrationManager();
        migrationManager.proceedMigration("http://localhost:8080/oldStorage", "http://localhost:8080/newStorage", false);
    }

}
