package enity;

import java.util.Collections;
import java.util.Set;

public class MigrationSets {
    private final Set<String> filesToMove;
    private final Set<String> filesToOverwrite;

    public MigrationSets(Set<String> filesToMove, Set<String> filesToOverwrite) {
        this.filesToMove = Collections.unmodifiableSet(filesToMove);
        this.filesToOverwrite = Collections.unmodifiableSet(filesToOverwrite);
    }

    public Set<String> getFilesToMove() {
        return filesToMove;
    }

    public Set<String> getFilesToOverwrite() {
        return filesToOverwrite;
    }
}
