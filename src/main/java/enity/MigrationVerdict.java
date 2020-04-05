package enity;

public class MigrationVerdict {
    private final boolean migrationDone;
    private final String message;

    public MigrationVerdict(boolean migrationDone, String message) {
        this.migrationDone = migrationDone;
        this.message = message;
    }

    public boolean isMigrationDone() {
        return migrationDone;
    }

    public String getMessage() {
        return message;
    }
}
