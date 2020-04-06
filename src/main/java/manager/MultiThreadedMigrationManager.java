package manager;

import connector.FaultyStorageConnector;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

public class MultiThreadedMigrationManager extends MigrationManager {
    private static final int N_THREADS = 10;
    private ExecutorService pool;

    public MultiThreadedMigrationManager() {
        super();
        pool = Executors.newFixedThreadPool(N_THREADS);
    }

    @Override
    protected boolean deleteAllFiles(FaultyStorageConnector storage, Collection<String> files) {
        return files.size() == doAndCount(files, fileName -> deleteFile(storage, fileName));
    }

    @Override
    protected boolean copyAllFiles(FaultyStorageConnector sourceStorage, FaultyStorageConnector targetStorage,
                                   Collection<String> filesToCopy, Set<String> filesToOverwrite) {
        long copied = doAndCount(
                filesToCopy,
                fileName -> copyFile(sourceStorage, targetStorage, fileName, filesToOverwrite.contains(fileName))
        );
        return copied == filesToCopy.size();
    }

    private long doAndCount(Collection<String> files, Function<String, Boolean> task) {
        return files.stream()
                .map(filename -> pool.submit(() -> task.apply(filename)))
                .map(future -> {
                            try {
                                return Optional.of(future.get());
                            } catch (Exception e) {
                                return Optional.of(false);
                            }
                        }
                )
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(Boolean::booleanValue)
                .count();
    }
}
