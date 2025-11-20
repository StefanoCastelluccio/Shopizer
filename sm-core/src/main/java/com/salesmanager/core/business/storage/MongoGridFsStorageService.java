package com.salesmanager.core.business.storage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.mongodb.gridfs.GridFsTemplate;
import org.springframework.data.mongodb.gridfs.GridFsResource;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.mongodb.client.gridfs.model.GridFSFile;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

@Component("gridFsStorageService")
@ConditionalOnProperty(name = "storage.backend", havingValue = "GRIDFS")
public class MongoGridFsStorageService implements StorageService {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoGridFsStorageService.class);

  private final GridFsTemplate gridFsTemplate;

  public MongoGridFsStorageService(GridFsTemplate gridFsTemplate) {
    this.gridFsTemplate = gridFsTemplate;
  }

  @Override
  public InputStream getInputStream(String bucket, String path) throws IOException {
    try {
      GridFsResource resource = gridFsTemplate.getResource(path);
      if (resource == null || !resource.exists()) {
        return null;
      }
      return resource.getInputStream();
    } catch (Exception e) {
      LOGGER.error("Error reading from GridFS", e);
      throw new IOException(e);
    }
  }

  @Override
  public byte[] getContent(String bucket, String path) throws IOException {
    try (InputStream is = getInputStream(bucket, path)) {
      if (is == null) {
        return null;
      }
      return is.readAllBytes();
    }
  }

  @Override
  public List<String> list(String bucket, String prefix) throws IOException {
    try {
      Query q = new Query(Criteria.where("filename").regex("^" + (prefix == null ? "" : prefix)));
      List<GridFSFile> files = new ArrayList<>();
      gridFsTemplate.find(q).into(files);
      List<String> names = new ArrayList<>();
      for (GridFSFile f : files) {
        names.add(f.getFilename());
      }
      return names;
    } catch (Exception e) {
      LOGGER.error("Error listing GridFS files", e);
      throw new IOException(e);
    }
  }

  @Override
  public void store(String bucket, String path, byte[] data, String contentType, Map<String, String> metadata)
      throws IOException {
    try (InputStream is = new ByteArrayInputStream(data)) {
      Document md = null;
      if (metadata != null && !metadata.isEmpty()) {
        md = new Document();
        metadata.forEach(md::put);
      }
      gridFsTemplate.store(is, path, StringUtils.hasText(contentType) ? contentType : null, md);
    } catch (Exception e) {
      LOGGER.error("Error storing to GridFS", e);
      throw new IOException(e);
    }
  }

  @Override
  public boolean delete(String bucket, String path) throws IOException {
    try {
      Query q = new Query(Criteria.where("filename").is(path));
      gridFsTemplate.delete(q);
      return true;
    } catch (Exception e) {
      LOGGER.error("Error deleting GridFS file", e);
      throw new IOException(e);
    }
  }

  @Override
  public boolean bucketExists(String bucket) throws IOException {
    // GridFS does not require explicit buckets; treat as always present
    return true;
  }

  @Override
  public void createBucket(String bucket) throws IOException {
    // no-op for GridFS
  }

  @Override
  public URL signedUrl(String bucket, String path, Duration ttl) {
    // not supported; application should stream through endpoints
    throw new UnsupportedOperationException("signedUrl not supported for GridFS");
  }

}
