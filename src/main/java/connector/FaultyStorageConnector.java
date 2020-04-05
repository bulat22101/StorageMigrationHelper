package connector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.params.HttpParams;

import java.io.InputStream;
import java.util.List;
import java.util.Optional;

public class FaultyStorageConnector {
    private CloseableHttpClient httpClient;
    private String baseUrl;
    private ObjectMapper objectMapper;

    public FaultyStorageConnector(String baseUrl) {
        this.baseUrl = baseUrl;
        this.objectMapper = new ObjectMapper();
        int timeout = 20;
        RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(timeout * 1000)
                .setConnectionRequestTimeout(timeout * 1000)
                .setSocketTimeout(timeout * 1000).build();
        this.httpClient =
                HttpClientBuilder.create().setDefaultRequestConfig(config).build();
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public Optional<List<String>> getFileNamesList() {
        HttpUriRequest request = new HttpGet(baseUrl + "/files/");
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            System.err.println(response.getStatusLine().getStatusCode());
            return response.getStatusLine().getStatusCode() == HttpStatus.SC_OK
                    ? Optional.ofNullable(
                    objectMapper.readValue(response.getEntity().getContent(), new TypeReference<List<String>>() {
                    }))
                    : Optional.empty();
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public boolean deleteFile(String filename) {
        HttpUriRequest request = new HttpDelete(baseUrl + "/files/" + filename);
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            return response.getStatusLine().getStatusCode() == HttpStatus.SC_OK;
        } catch (Exception e) {
            return false;
        }
    }

    public Optional<CloseableHttpResponse> startDownloading(String filename) {
        HttpGet request = new HttpGet(baseUrl + "/files/" + filename);
        request.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_OCTET_STREAM.getMimeType());
        System.err.println(filename);
        try {
            CloseableHttpResponse response = httpClient.execute(request);
            System.err.println(response.getStatusLine());
            return response.getStatusLine().getStatusCode() == HttpStatus.SC_OK
                    ? Optional.of(response)
                    : Optional.empty();
        } catch (Exception e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }

    public boolean uploadFile(String filename, byte[] file) {
        HttpPost request = new HttpPost(baseUrl + "/files");
        System.err.println(filename);
        request.setEntity(MultipartEntityBuilder.create()
                .addBinaryBody("file", file, ContentType.MULTIPART_FORM_DATA, filename).build());
        try (CloseableHttpResponse response = httpClient.execute(request)) {

            System.err.println(response.getStatusLine() + IOUtils.toString(response.getEntity().getContent()));
            return response.getStatusLine().getStatusCode() == HttpStatus.SC_OK;
        } catch (Exception e) {
            return false;
        }
    }
}
