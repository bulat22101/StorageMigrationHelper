package connector;

import java.util.List;
import java.util.Optional;

public interface HttpStorageConnector {

    String getBaseUrl();

    Optional<List<String>> getFileNamesList();

    boolean deleteFile(String filename);

    Optional<byte[]> downloadFile(String filename);

    boolean uploadFile(String filename, byte[] file);
}
