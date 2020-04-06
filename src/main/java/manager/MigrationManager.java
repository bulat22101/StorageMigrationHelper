package manager;

import connector.FaultyStorageConnector;
import util.retry.RetryService;

import java.util.*;

public class MigrationManager {
    RetryService retryService;

    public MigrationManager() {
        this.retryService = new RetryService();
    }

    public boolean proceedMigration(String sourceStorageUrl, String targetStorageUrl, boolean overwrite) {
        FaultyStorageConnector sourceStorage = new FaultyStorageConnector(sourceStorageUrl);
        FaultyStorageConnector targetStorage = new FaultyStorageConnector(targetStorageUrl);
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

    private boolean checkFiles(Collection<String> expectedFiles, FaultyStorageConnector storage){
        Optional<Set<String>> storageFiles = getStorageFiles(storage).map(HashSet::new);
        return storageFiles.isPresent() && storageFiles.get().containsAll(expectedFiles);
    }

    private boolean deleteAllFiles(FaultyStorageConnector storage, Collection<String> files) {
        System.err.println("START DELETION.");
        boolean allFilesDeleted = true;
        for (String filename : files) {
            allFilesDeleted &= deleteFile(storage, filename);
        }
        return allFilesDeleted;
    }

    private boolean copyAllFiles(FaultyStorageConnector sourceStorage, FaultyStorageConnector targetStorage,
                                 Collection<String> filesToCopy, Set<String> filesToOverwrite) {
        System.err.println("START COPYING.");
        boolean allFilesCopied = true;
        for (String filename : filesToCopy) {
            allFilesCopied &= copyFile(sourceStorage, targetStorage, filename, filesToOverwrite.contains(filename));
        }
        return allFilesCopied;
    }

    private boolean copyFile(FaultyStorageConnector sourceStorage, FaultyStorageConnector targetStorage,
                             String filename, boolean overwrite) {
        System.err.println(filename);
        Optional<byte[]> file = downloadFile(sourceStorage, filename);
        return file.isPresent()
                && (!overwrite || deleteFile(targetStorage, filename))
                && uploadFile(targetStorage, filename, file.get());
    }

    private Optional<List<String>> getStorageFiles(FaultyStorageConnector storage) {
        return retryService.retry(storage::getFileNamesList);
    }

    private boolean deleteFile(FaultyStorageConnector storage, String filename) {
        System.err.println(filename);
        return retryService.retry(() -> Optional.of(storage.deleteFile(filename)), value -> value).isPresent();
    }

    private Optional<byte[]> downloadFile(FaultyStorageConnector storage, String filename) {
        return retryService.retry(() -> storage.downloadFile(filename));
    }

    private boolean uploadFile(FaultyStorageConnector storage, String filename, byte[] file) {
        return retryService.retry(() -> Optional.of(storage.uploadFile(filename, file)), value -> value).isPresent();
    }
}
