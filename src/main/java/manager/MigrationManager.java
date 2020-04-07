package manager;

import connector.FaultyStorageConnector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import util.retry.RetryService;

import java.util.*;

public abstract class MigrationManager {
    protected static final Logger log = LogManager.getLogger();
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
        log.debug("Starting migration.");
        log.debug("Getting file lists from storages.");
        Optional<List<String>> sourceStorageFiles = getStorageFiles(sourceStorage);
        Optional<List<String>> targetStorageFiles = getStorageFiles(targetStorage);
        if (!sourceStorageFiles.isPresent() || !targetStorageFiles.isPresent()) {
            log.debug("Failed to get files lists. Stopping migration.");
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
        boolean verdict = proceedCopying(sourceStorage, targetStorage, filesToMove, filesToOverwrite)
                && checkFiles(sourceStorageFiles.get(), targetStorage)
                && proceedDeletion(sourceStorage, sourceStorageFiles.get());
        if (verdict) {
            log.debug("Migration successfully completed.");
        } else {
            log.debug("Migration failed.");
        }
        return verdict;
    }

    private boolean proceedCopying(FaultyStorageConnector sourceStorage, FaultyStorageConnector targetStorage,
                                   Collection<String> filesToCopy, Set<String> filesToOverwrite) {
        log.debug(
                "Starting copying {} files from storage({}) to storage({}).",
                filesToCopy.size(),
                sourceStorage.getBaseUrl(),
                targetStorage.getBaseUrl()
        );
        long copied = copyAllFiles(sourceStorage, targetStorage, filesToCopy, filesToOverwrite);
        log.debug("Successfully copied {} files of {}.", copied, filesToCopy.size());
        boolean verdict = copied == filesToCopy.size();
        if (verdict) {
            log.debug("All files copied.");
        } else {
            log.debug("Not all files copied. Skipping other steps.");
        }
        return verdict;
    }

    private boolean proceedDeletion(FaultyStorageConnector storage, Collection<String> filesToDelete) {
        log.debug(
                "Starting copying {} files from storage({}).",
                filesToDelete.size(),
                storage.getBaseUrl()
        );
        long deleted = deleteAllFiles(storage, filesToDelete);
        log.debug("Successfully deleted {} files of {}.", deleted, filesToDelete.size());
        boolean verdict = deleted == filesToDelete.size();
        if (verdict) {
            log.debug("All files deleted.");
        } else {
            log.debug("Not all files deleted.");
        }
        return verdict;
    }

    private boolean checkFiles(Collection<String> expectedFiles, FaultyStorageConnector storage) {
        log.debug("Start checking file lists.");
        Optional<Set<String>> storageFiles = getStorageFiles(storage).map(HashSet::new);
        boolean verdict = storageFiles.isPresent() && storageFiles.get().containsAll(expectedFiles);
        if (verdict) {
            log.debug("File lists check successfully passed.");
        } else {
            log.debug(
                    storageFiles.isPresent()
                            ? "Check proceeded, but not passed."
                            : "Failed to proceed check."
            );
        }
        return verdict;
    }

    abstract protected long deleteAllFiles(FaultyStorageConnector storage, Collection<String> files);

    abstract protected long copyAllFiles(FaultyStorageConnector sourceStorage, FaultyStorageConnector targetStorage,
                                         Collection<String> filesToCopy, Set<String> filesToOverwrite);

    protected boolean copyFile(FaultyStorageConnector sourceStorage, FaultyStorageConnector targetStorage,
                               String filename, boolean overwrite) {
        log.debug("Starting copy file {}.", filename);
        Optional<byte[]> file = downloadFile(sourceStorage, filename);
        boolean verdict = file.isPresent()
                && (!overwrite || deleteFile(targetStorage, filename))
                && uploadFile(targetStorage, filename, file.get());
        if (verdict) {
            log.debug("File {} was successfully copied.", filename);
        } else {
            log.error("Failed to copy file: {}.", filename);
        }
        return verdict;
    }

    private Optional<List<String>> getStorageFiles(FaultyStorageConnector storage) {
        return retryService.retry(storage::getFileNamesList);
    }

    protected boolean deleteFile(FaultyStorageConnector storage, String filename) {
        log.debug("Starting deleting file {}.", filename);
        boolean verdict = retryService
                .retry(() -> Optional.of(storage.deleteFile(filename)), value -> value)
                .isPresent();
        if (verdict) {
            log.debug("File {} was successfully deleted.", filename);
        } else {
            log.error("Failed to delete file: {}.", filename);
        }
        return verdict;
    }

    private Optional<byte[]> downloadFile(FaultyStorageConnector storage, String filename) {
        return retryService.retry(() -> storage.downloadFile(filename));
    }

    private boolean uploadFile(FaultyStorageConnector storage, String filename, byte[] file) {
        return retryService.retry(() -> Optional.of(storage.uploadFile(filename, file)), value -> value).isPresent();
    }
}
