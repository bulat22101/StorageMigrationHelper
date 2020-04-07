package manager;

import connector.FaultyStorageConnector;

import java.util.Collection;
import java.util.Set;

public class SingleThreadedMigrationManager extends MigrationManager {

    protected long deleteAllFiles(FaultyStorageConnector storage, Collection<String> files) {
        return files.stream()
                .map(filename -> deleteFile(storage, filename))
                .filter(Boolean::booleanValue)
                .count();
    }

    protected long copyAllFiles(FaultyStorageConnector sourceStorage, FaultyStorageConnector targetStorage,
                                Collection<String> filesToCopy, Set<String> filesToOverwrite) {
        return filesToCopy.stream()
                .map(
                        filename -> copyFile(
                                sourceStorage,
                                targetStorage,
                                filename,
                                filesToOverwrite.contains(filename)
                        )
                )
                .filter(Boolean::booleanValue)
                .count();
    }
}
