package manager;

import connector.FaultyStorageConnector;
import util.retry.RetryService;

import java.util.*;

public abstract class MigrationManager {
    private RetryService retryService;

    public MigrationManager() {
        this.retryService = new RetryService();
    }

    public boolean proceedMigration(String sourceStorageUrl, String targetStorageUrl, boolean overwrite) {
        return proceedMigration(
                new FaultyStorageConnector(sourceStorageUrl),
                new FaultyStorageConnector(targetStorageUrl),
                overwrite
        );
    }

    public boolean proceedMigration(FaultyStorageConnector sourceStorage, FaultyStorageConnector targetStorage, boolean overwrite) {
        Optional<List<String>> sourceStorageFiles = getStorageFiles(sourceStorage);
        Optional<List<String>> targetStorageFiles = getStorageFiles(targetStorage);
        if (!sourceStorageFiles.isPresent() || !targetStorageFiles.isPresent()) {
            return false;
        }
        Set<String> filesToMove = new HashSet<>(sourceStorageFiles.get());
        Set<String> filesToOverwrite;
        if (overwrite) {
            filesToOverwrite = new HashSet<>(filesToMove);
            filesToOverwrite.retainAll(new HashSet<>(targetStorageFiles.get()));
        } else {
            filesToOverwrite = Collections.emptySet();
            filesToMove.removeAll(new HashSet<>(targetStorageFiles.get()));
        }
        return copyAllFiles(sourceStorage, targetStorage, filesToMove, filesToOverwrite)
                && checkFiles(sourceStorageFiles.get(), targetStorage)
                && deleteAllFiles(sourceStorage, sourceStorageFiles.get());
    }

    private boolean checkFiles(Collection<String> expectedFiles, FaultyStorageConnector storage) {
        Optional<Set<String>> storageFiles = getStorageFiles(storage).map(HashSet::new);
        return storageFiles.isPresent() && storageFiles.get().containsAll(expectedFiles);
    }

    abstract protected  boolean deleteAllFiles(FaultyStorageConnector storage, Collection<String> files);

    abstract protected boolean copyAllFiles(FaultyStorageConnector sourceStorage, FaultyStorageConnector targetStorage,
                                            Collection<String> filesToCopy, Set<String> filesToOverwrite);

    protected boolean copyFile(FaultyStorageConnector sourceStorage, FaultyStorageConnector targetStorage,
                               String filename, boolean overwrite) {
        Optional<byte[]> file = downloadFile(sourceStorage, filename);
        return file.isPresent()
                && (!overwrite || deleteFile(targetStorage, filename))
                && uploadFile(targetStorage, filename, file.get());
    }

    private Optional<List<String>> getStorageFiles(FaultyStorageConnector storage) {
        return retryService.retry(storage::getFileNamesList);
    }

    protected boolean deleteFile(FaultyStorageConnector storage, String filename) {
        return retryService.retry(() -> Optional.of(storage.deleteFile(filename)), value -> value).isPresent();
    }

    private Optional<byte[]> downloadFile(FaultyStorageConnector storage, String filename) {
        return retryService.retry(() -> storage.downloadFile(filename));
    }

    private boolean uploadFile(FaultyStorageConnector storage, String filename, byte[] file) {
        return retryService.retry(() -> Optional.of(storage.uploadFile(filename, file)), value -> value).isPresent();
    }
}
