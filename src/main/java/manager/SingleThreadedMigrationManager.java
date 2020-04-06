package manager;

import connector.FaultyStorageConnector;

import java.util.Collection;
import java.util.Set;

public class SingleThreadedMigrationManager extends MigrationManager {

    protected boolean deleteAllFiles(FaultyStorageConnector storage, Collection<String> files) {
        boolean allFilesDeleted = true;
        for (String filename : files) {
            allFilesDeleted &= deleteFile(storage, filename);
        }
        return allFilesDeleted;
    }

    protected boolean copyAllFiles(FaultyStorageConnector sourceStorage, FaultyStorageConnector targetStorage,
                                   Collection<String> filesToCopy, Set<String> filesToOverwrite) {
        boolean allFilesCopied = true;
        for (String filename : filesToCopy) {
            allFilesCopied &= copyFile(sourceStorage, targetStorage, filename, filesToOverwrite.contains(filename));
        }
        return allFilesCopied;
    }
}
