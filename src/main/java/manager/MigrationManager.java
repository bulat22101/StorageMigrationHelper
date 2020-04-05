package manager;

import connector.FaultyStorageConnector;
import enity.MigrationReport;
import enity.MigrationSets;
import exception.MigrationException;
import util.retry.RetryService;

import java.util.*;

public class MigrationManager {
    RetryService retryService;

    public MigrationManager() {
        this.retryService = new RetryService();
    }

    public MigrationReport proceedMigration(String sourceStorageUrl, String targetStorageUrl, boolean overwrite) {
        List<String> reportMessages = new ArrayList<>();
        FaultyStorageConnector sourceStorage = new FaultyStorageConnector(sourceStorageUrl);
        FaultyStorageConnector targetStorage = new FaultyStorageConnector(targetStorageUrl);
        Set<String> filesToMove;
        Set<String> filesToOverwrite;
        try {
            MigrationSets migrationSets = getMigrationSets(sourceStorage, targetStorage, overwrite);
            filesToMove = migrationSets.getFilesToMove();
            filesToOverwrite = migrationSets.getFilesToOverwrite();
            String message = String.format(
                    "Successfully get actual file lists in storages. Files to move: %d. Will be overwritten: %d.",
                    filesToMove.size(),
                    filesToOverwrite.size()
            );
            reportMessages.add(message);
        } catch (MigrationException e) {
            reportMessages.add("Error while getting file lists: " + e.getMessage());
            return new MigrationReport(false, reportMessages);
        }
        boolean allFilesCopied = true;
        for (String filename : filesToMove) {
            try {
                copyFile(sourceStorage, targetStorage, filename, filesToOverwrite.contains(filename));
            } catch (MigrationException e) {
                allFilesCopied = false;
                String message = String.format(
                        "Error while copying file %s: %s",
                        filename,
                        e.getMessage()
                );
                reportMessages.add(message);
            }
        }
        if (allFilesCopied) {
            reportMessages.add("All files were successfully copied");
        }
        boolean allFilesChecked;
        try {
            allFilesChecked = checkAllFilesMoved(sourceStorage, targetStorage);
        } catch (MigrationException e) {
            allFilesChecked = false;
            String message = ("Error while checking resulting file lists: " + e.getMessage());
            reportMessages.add(message);
        }
        if (allFilesChecked) {
            reportMessages.add("All files are in target storage file list.");
        } else {
            reportMessages.add("Some files are missing in target storage file list.");
        }
        boolean allFilesDeleted = true;
        if (allFilesChecked) {
            for (String filename : filesToMove) {
                try {
                    deleteFile(sourceStorage, filename);
                } catch (MigrationException e) {
                    allFilesDeleted = false;
                    String message = String.format(
                            "Error while deleting file %s: %s",
                            filename,
                            e.getMessage()
                    );
                    reportMessages.add(message);
                }
            }
        } else {
            allFilesDeleted = false;
        }
        if(allFilesDeleted){
            reportMessages.add("All files were successfully deleted from source storage");
        }
        return new MigrationReport(allFilesCopied && allFilesChecked && allFilesDeleted, reportMessages);
    }


    private boolean checkAllFilesMoved(FaultyStorageConnector sourceStorage, FaultyStorageConnector targetStorage)
            throws MigrationException {
        return getMigrationSets(sourceStorage, targetStorage, false).getFilesToMove().isEmpty();
    }

    private MigrationSets getMigrationSets(FaultyStorageConnector sourceStorage,
                                           FaultyStorageConnector targetStorage,
                                           boolean overwrite)
            throws MigrationException {
        Set<String> filesToMove = new HashSet<>(getStorageFiles(sourceStorage));
        Set<String> targetStorageFiles = new HashSet<>(getStorageFiles(targetStorage));
        Set<String> filesToOverwrite;
        if (!overwrite) {
            filesToMove.removeAll(targetStorageFiles);
            filesToOverwrite = Collections.emptySet();
        } else {
            filesToOverwrite = new HashSet<>(filesToMove);
            filesToOverwrite.retainAll(targetStorageFiles);
        }
        return new MigrationSets(filesToMove, filesToOverwrite);
    }

    private void copyFile(FaultyStorageConnector sourceStorage, FaultyStorageConnector targetStorage,
                          String filename, boolean overwrite) throws MigrationException {
        byte[] file = downloadFile(sourceStorage, filename);
        if (overwrite) {
            deleteFile(targetStorage, filename);
        }
        uploadFile(targetStorage, filename, file);
    }

    private List<String> getStorageFiles(FaultyStorageConnector storage) throws MigrationException {
        return retryService.retry(storage::getFileNamesList)
                .orElseThrow(() -> new MigrationException(
                        String.format("Can't reach storage: %s.", storage.getBaseUrl())
                ));
    }

    private void deleteFile(FaultyStorageConnector storage, String filename) throws MigrationException {
        retryService.retry(() -> Optional.of(storage.deleteFile(filename)), value -> value)
                .orElseThrow(
                        () -> new MigrationException(
                                String.format("Can't delete file %s in storage %s.", filename, storage.getBaseUrl())
                        )
                );
    }

    private byte[] downloadFile(FaultyStorageConnector storage, String filename) throws MigrationException {
        return retryService.retry(() -> storage.downloadFile(filename))
                .orElseThrow(
                        () -> new MigrationException(
                                String.format("Can't download file %s from storage %s.", filename, storage.getBaseUrl())
                        )
                );
    }

    private void uploadFile(FaultyStorageConnector storage, String filename, byte[] file) throws MigrationException {
        retryService.retry(() -> Optional.of(storage.uploadFile(filename, file)), value -> value)
                .orElseThrow(
                        () -> new MigrationException(
                                String.format("Can't upload file %s to storage %s.", filename, storage.getBaseUrl())
                        )
                );
    }
}
