package manager;

import connector.HttpStorageConnector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.*;

public class MigrationManagerTest {
    private static final int SOURCE_STORAGE_SIZE = 5000;
    private HttpStorageConnector sourceStorage;
    private HttpStorageConnector targetStorage;
    private Map<String, File> sourceStorageMap;
    private Map<String, File> targetStorageMap;
    private Map<String, File> expectedStorageMap;

    @Before
    public void before() {
        sourceStorageMap = new HashMap<>();
        targetStorageMap = new HashMap<>();
        Random random = new Random(22041870L);
        for (int i = 0; i < SOURCE_STORAGE_SIZE; i++) {
            String filename = i + ".something";
            File file = generateRandomFile(random, 1, 10);
            sourceStorageMap.put(filename, file);
            if (i % 100 == 0) {
                targetStorageMap.put(filename, file);
            }
        }
        targetStorageMap.put("L22041980.txt", generateRandomFile(random, 10, 100));
        expectedStorageMap = new HashMap<>(sourceStorageMap);
        sourceStorage = Mockito.spy(new FakeStorage(sourceStorageMap));
        targetStorage = Mockito.spy(new FakeStorage(targetStorageMap));
    }

    @Test
    public void testSingleThreadOverwrite() {
        testOverwrite(new SingleThreadedMigrationManager());
    }

    @Test
    public void testMultiThreadOverwrite() {
        testOverwrite(new MultiThreadedMigrationManager());
    }

    @Test
    public void testSingleThreadNotOverwrite() {
        testNotOverwrite(new SingleThreadedMigrationManager());
    }

    @Test
    public void testMultiThreadNotOverwrite() {
        testNotOverwrite(new MultiThreadedMigrationManager());
    }

    private void testOverwrite(MigrationManager migrationManager) {
        test(migrationManager, true);
        Mockito.verify(sourceStorage, Mockito.times(SOURCE_STORAGE_SIZE)).downloadFile(Mockito.anyString());
        Mockito.verify(targetStorage, Mockito.times((SOURCE_STORAGE_SIZE + 99) / 100)).deleteFile(Mockito.anyString());
        Mockito.verify(targetStorage, Mockito.times(0)).downloadFile(Mockito.anyString());
        Mockito.verify(targetStorage, Mockito.times(SOURCE_STORAGE_SIZE)).uploadFile(Mockito.anyString(), Mockito.any());
        Mockito.verify(targetStorage, Mockito.times(2)).getFileNamesList();

    }

    private void testNotOverwrite(MigrationManager migrationManager) {
        test(migrationManager, false);
        Mockito.verify(sourceStorage, Mockito.times(SOURCE_STORAGE_SIZE - (SOURCE_STORAGE_SIZE + 99) / 100)).downloadFile(Mockito.anyString());
        Mockito.verify(targetStorage, Mockito.times(0)).deleteFile(Mockito.anyString());
        Mockito.verify(targetStorage, Mockito.times(0)).downloadFile(Mockito.anyString());
        Mockito.verify(targetStorage, Mockito.times(SOURCE_STORAGE_SIZE - (SOURCE_STORAGE_SIZE + 99) / 100)).uploadFile(Mockito.anyString(), Mockito.any());
        Mockito.verify(targetStorage, Mockito.times(2)).getFileNamesList();
    }

    private void test(MigrationManager migrationManager, boolean overwrite) {
        Assert.assertTrue(migrationManager.proceedMigration(sourceStorage, targetStorage, overwrite));
        Assert.assertTrue(sourceStorageMap.isEmpty());
        Assert.assertEquals(SOURCE_STORAGE_SIZE + 1, targetStorageMap.size());
        Assert.assertTrue(checkTargetMapFiles());
        Mockito.verify(sourceStorage, Mockito.times(SOURCE_STORAGE_SIZE)).deleteFile(Mockito.anyString());
        Mockito.verify(sourceStorage, Mockito.times(0)).uploadFile(Mockito.anyString(), Mockito.any());
        Mockito.verify(sourceStorage, Mockito.times(1)).getFileNamesList();
    }

    private boolean checkTargetMapFiles() {
        boolean result = true;
        for (Map.Entry<String, File> entry : expectedStorageMap.entrySet()) {
            result &= targetStorageMap.containsKey(entry.getKey())
                    && entry.getValue().equals(targetStorageMap.get(entry.getKey()));
        }
        return result;
    }

    private File generateRandomFile(Random random, int leftB, int rightB) {
        byte[] data = new byte[leftB + random.nextInt(rightB)];
        random.nextBytes(data);
        return new File(data);
    }

    private class FakeStorage implements HttpStorageConnector {
        Map<String, File> map;

        public FakeStorage(Map<String, File> map) {
            this.map = map;
        }

        @Override
        public String getBaseUrl() {
            return "fake-storage.url";
        }

        @Override
        public Optional<List<String>> getFileNamesList() {
            return Optional.of(new ArrayList<>(map.keySet()));
        }

        @Override
        public boolean deleteFile(String filename) {
            return map.remove(filename) != null;
        }

        @Override
        public Optional<byte[]> downloadFile(String filename) {
            return Optional.of(map.get(filename).getData());
        }

        @Override
        public boolean uploadFile(String filename, byte[] file) {
            return map.put(filename, new File(file)) == null;
        }
    }

    private class File {
        private final byte[] data;

        public File(byte[] data) {
            this.data = data;
        }

        public byte[] getData() {
            return data;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            File file = (File) o;
            return Arrays.equals(data, file.data);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(data);
        }
    }
}
