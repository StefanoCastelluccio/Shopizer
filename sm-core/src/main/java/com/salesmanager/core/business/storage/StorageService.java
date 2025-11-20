package com.salesmanager.core.business.storage;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public interface StorageService {

  InputStream getInputStream(String bucket, String path) throws IOException;

  byte[] getContent(String bucket, String path) throws IOException;

  List<String> list(String bucket, String prefix) throws IOException;

  void store(String bucket, String path, byte[] data, String contentType, Map<String,String> metadata)
      throws IOException;

  boolean delete(String bucket, String path) throws IOException;

  boolean bucketExists(String bucket) throws IOException;

  void createBucket(String bucket) throws IOException;

  default URL signedUrl(String bucket, String path, Duration ttl) {
    throw new UnsupportedOperationException("signedUrl not implemented");
  }

}
