package enity;

import java.util.List;

public class MigrationReport {
    private final boolean migrationDone;
    private final List<String> messages;

    public MigrationReport(boolean migrationDone, List<String> message) {
        this.migrationDone = true;
        this.messages = message;
    }

    public boolean isMigrationDone() {
        return migrationDone;
    }

    public List<String> getMessages() {
        return messages;
    }
}
