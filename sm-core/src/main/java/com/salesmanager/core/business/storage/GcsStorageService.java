package com.salesmanager.core.business.storage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BucketField;
import com.google.cloud.storage.Storage.BucketGetOption;
import com.google.cloud.storage.StorageOptions;

@Component("gcsStorageService")
@ConditionalOnProperty(name = "storage.backend", havingValue = "GCS", matchIfMissing = true)
public class GcsStorageService implements StorageService {

  private Storage getStorage() {
    return StorageOptions.getDefaultInstance().getService();
  }

  @Override
  public InputStream getInputStream(String bucket, String path) throws IOException {
    Storage storage = getStorage();
    Blob blob = storage.get(BlobId.of(bucket, path));
    if (blob == null) {
      return null;
    }
    ReadChannel reader = blob.reader();
    return Channels.newInputStream(reader);
  }

  @Override
  public byte[] getContent(String bucket, String path) throws IOException {
    Storage storage = getStorage();
    Blob blob = storage.get(BlobId.of(bucket, path));
    if (blob == null) {
      return null;
    }
    return blob.getContent();
  }

  @Override
  public List<String> list(String bucket, String prefix) throws IOException {
    Storage storage = getStorage();
    Page<Blob> blobs = storage.list(bucket, BlobListOption.currentDirectory(), BlobListOption.prefix(prefix));
    List<String> names = new ArrayList<>();
    for (Blob b : blobs.iterateAll()) {
      names.add(b.getName());
    }
    return names;
  }

  @Override
  public void store(String bucket, String path, byte[] data, String contentType, Map<String, String> metadata)
      throws IOException {
    Storage storage = getStorage();
    BlobId blobId = BlobId.of(bucket, path);
    BlobInfo.Builder builder = BlobInfo.newBuilder(blobId);
    if (contentType != null) {
      builder.setContentType(contentType);
    }
    if (metadata != null && !metadata.isEmpty()) {
      builder.setMetadata(metadata);
    }
    BlobInfo blobInfo = builder.build();
    storage.create(blobInfo, data);
  }

  @Override
  public boolean delete(String bucket, String path) throws IOException {
    Storage storage = getStorage();
    return storage.delete(bucket, path);
  }

  @Override
  public boolean bucketExists(String bucket) throws IOException {
    Storage storage = getStorage();
    Bucket b = storage.get(bucket, BucketGetOption.fields(BucketField.NAME));
    return b != null && b.exists();
  }

  @Override
  public void createBucket(String bucket) throws IOException {
    Storage storage = getStorage();
    storage.create(BucketInfo.of(bucket));
  }

}
