package manager;

import connector.FaultyStorageConnector;
import enity.MigrationReport;
import exception.MigrationException;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import util.retry.RetryService;

import java.io.IOException;
import java.util.*;

public class MigrationManager {
    RetryService retryService;

    public MigrationManager() {
        this.retryService = new RetryService();
    }

    public MigrationReport proceedMigration(String sourceStorageUrl, String targetStorageUrl, boolean overwrite)
            throws MigrationException {
        FaultyStorageConnector sourceStorage = new FaultyStorageConnector(sourceStorageUrl);
        FaultyStorageConnector targetStorage = new FaultyStorageConnector(targetStorageUrl);
        Set<String> filesToMove = getFilesToMove(sourceStorage, targetStorage, overwrite);
        for (String filename : filesToMove) {
            migrateFile(sourceStorage, targetStorage, filename);
        }
        checkAllFilesMoved(sourceStorage, targetStorage);
        return new MigrationReport(true, Collections.singletonList("Migration was successfully completed."));
    }

    public void migrateFile(FaultyStorageConnector sourceStorage, FaultyStorageConnector targetStorage, String filename)
            throws MigrationException {
        CloseableHttpResponse downloadConnection = retryService.retry(() -> sourceStorage.startDownloading(filename))
                .orElseThrow(() -> new MigrationException(""));
        try {
            System.err.println("Started");
            byte[] file = IOUtils.toByteArray(downloadConnection.getEntity().getContent());
            System.err.println("Continued");
            retryService.retry(() -> Optional.of(targetStorage.uploadFile(filename, file)), value -> value).orElseThrow(() -> new MigrationException(""));
            downloadConnection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteFile(FaultyStorageConnector storage, String filename) throws MigrationException {
        retryService.retry(() -> Optional.of(storage.deleteFile(filename)), value -> value)
                .orElseThrow(
                        () -> new MigrationException(
                                String.format("Can't delete file %s in storage %s.", filename, storage.getBaseUrl())
                        )
                );
    }

    public Set<String> getFilesToOverwrite(Set<String> filesToMove, List<String> targetStorageFiles) {
        HashSet<String> result = new HashSet<>(filesToMove);
        result.retainAll(targetStorageFiles);
        return result;
    }


    public boolean checkAllFilesMoved(FaultyStorageConnector sourceStorage, FaultyStorageConnector targetStorage)
            throws MigrationException {
        return getFilesToMove(sourceStorage, targetStorage, false).isEmpty();
    }

    public Set<String> getFilesToMove(FaultyStorageConnector sourceStorage,
                                      FaultyStorageConnector targetStorage,
                                      boolean overwrite)
            throws MigrationException {
        List<String> sourceStorageFiles = getStorageFiles(sourceStorage);
        List<String> targetStorageFiles = getStorageFiles(targetStorage);
        HashSet<String> result = new HashSet<>(sourceStorageFiles);
        if (!overwrite) {
            result.removeAll(targetStorageFiles);
        }
        return result;
    }

    public List<String> getStorageFiles(FaultyStorageConnector storage) throws MigrationException {
        return retryService.retry(storage::getFileNamesList)
                .orElseThrow(() -> new MigrationException(
                        String.format("Can't reach storage: %s.", storage.getBaseUrl())
                ));
    }
}
