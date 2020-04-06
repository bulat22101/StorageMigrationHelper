package connector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.util.List;
import java.util.Optional;

public class FaultyStorageConnector {
    private CloseableHttpClient httpClient;
    private String baseUrl;
    private ObjectMapper objectMapper;

    public FaultyStorageConnector(String baseUrl) {
        this(baseUrl, 1500);
    }

    public FaultyStorageConnector(String baseUrl, int timeoutMS) {
        this(baseUrl, HttpClientBuilder.create()
                .setDefaultRequestConfig(
                        RequestConfig.custom()
                                .setConnectTimeout(timeoutMS)
                                .setConnectionRequestTimeout(timeoutMS)
                                .setSocketTimeout(timeoutMS)
                                .build()
                ).build()
        );
    }

    public FaultyStorageConnector(String baseUrl, CloseableHttpClient httpClient) {
        this.baseUrl = baseUrl;
        this.objectMapper = new ObjectMapper();
        this.httpClient = httpClient;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public Optional<List<String>> getFileNamesList() {
        HttpUriRequest request = new HttpGet(baseUrl + "/files/");
        request.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType());
        try (CloseableHttpResponse response = httpClient.execute(request)) {
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

    public Optional<byte[]> downloadFile(String filename) {
        HttpGet request = new HttpGet(baseUrl + "/files/" + filename);
        request.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_OCTET_STREAM.getMimeType());
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            return response.getStatusLine().getStatusCode() == HttpStatus.SC_OK
                    ? Optional.of(IOUtils.toByteArray(response.getEntity().getContent()))
                    : Optional.empty();
        } catch (Exception e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }

    public boolean uploadFile(String filename, byte[] file) {
        HttpPost request = new HttpPost(baseUrl + "/files");
        HttpEntity entity = MultipartEntityBuilder.create()
                .addBinaryBody("file", file, ContentType.MULTIPART_FORM_DATA, filename)
                .build();
        request.setEntity(entity);
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            return response.getStatusLine().getStatusCode() == HttpStatus.SC_OK;
        } catch (Exception e) {
            return false;
        }
    }
}
