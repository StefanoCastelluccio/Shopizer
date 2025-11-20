package com.salesmanager.core.business.modules.cms.product.gcp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.salesmanager.core.business.storage.StorageService;
import com.salesmanager.core.business.constants.Constants;
import com.salesmanager.core.business.exception.ServiceException;
import com.salesmanager.core.business.modules.cms.impl.CMSManager;
import com.salesmanager.core.business.modules.cms.product.ProductAssetsManager;
import com.salesmanager.core.model.catalog.product.Product;
import com.salesmanager.core.model.catalog.product.file.ProductImageSize;
import com.salesmanager.core.model.catalog.product.image.ProductImage;
import com.salesmanager.core.model.content.FileContentType;
import com.salesmanager.core.model.content.ImageContentFile;
import com.salesmanager.core.model.content.OutputContentFile;

@Component("gcpProductAssetsManager")
public class GCPProductContentFileManager implements ProductAssetsManager {
  
  @Autowired 
  private CMSManager gcpAssetsManager;
  
  @Autowired
  private StorageService storageService;
  
  private static String DEFAULT_BUCKET_NAME = "shopizer";
  
  private static final Logger LOGGER = LoggerFactory.getLogger(GCPProductContentFileManager.class);

  

  private final static String SMALL = "SMALL";
  private final static String LARGE = "LARGE";

  /**
   * 
   */
  private static final long serialVersionUID = 1L;


  @Override
  public OutputContentFile getProductImage(String merchantStoreCode, String productCode,
      String imageName) throws ServiceException {
    // TODO Auto-generated method stub
    
 
    
    return null;
  }

  @Override
  public OutputContentFile getProductImage(String merchantStoreCode, String productCode,
      String imageName, ProductImageSize size) throws ServiceException {
    InputStream inputStream = null;
    try {
      String bucketName = bucketName();

      if (!this.bucketExists(bucketName)) {
        return null;
      }

      inputStream = storageService.getInputStream(bucketName,
          filePath(merchantStoreCode, productCode, size.name(), imageName));

      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      IOUtils.copy(inputStream, outputStream);
      OutputContentFile ct = new OutputContentFile();
      ct.setFile(outputStream);
      ct.setFileName(filePath(merchantStoreCode, productCode, size.name(), imageName));
      return ct;
    } catch (final Exception e) {
      LOGGER.error("Error while getting files", e);
      throw new ServiceException(e);
    } finally {
      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (Exception ignore) {
        }
      }
    }
  
  }

  @Override
  public OutputContentFile getProductImage(ProductImage productImage) throws ServiceException {

    return null;
    
  }

  @Override
  public List<OutputContentFile> getImages(Product product) throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * List files
   */
  @Override
  public List<OutputContentFile> getImages(String merchantStoreCode,
      FileContentType imageContentType) throws ServiceException {
    InputStream inputStream = null;
    try {
      String bucketName = bucketName();

      if (!this.bucketExists(bucketName)) {
        return null;
      }

      List<String> blobNames = storageService.list(bucketName, merchantStoreCode);

      List<OutputContentFile> files = new ArrayList<OutputContentFile>();
      for (String name : blobNames) {
        inputStream = storageService.getInputStream(bucketName, name);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        IOUtils.copy(inputStream, outputStream);
        OutputContentFile ct = new OutputContentFile();
        ct.setFile(outputStream);
        files.add(ct);
      }

      return files;
    } catch (final Exception e) {
      LOGGER.error("Error while getting files", e);
      throw new ServiceException(e);
    } finally {
      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (Exception ignore) {
        }
      }
    }
  }

  @Override
  public void addProductImage(ProductImage productImage, ImageContentFile contentImage)
      throws ServiceException {

    String bucketName = bucketName();

    try {
      if (!this.bucketExists(bucketName)) {
        this.createBucket(bucketName);
      }

      // build filename
      StringBuilder fileName = new StringBuilder()
          .append(filePath(productImage.getProduct().getMerchantStore().getCode(),
              productImage.getProduct().getSku(), contentImage.getFileContentType()))
          .append(productImage.getProductImage());

      byte[] targetArray = IOUtils.toByteArray(contentImage.getFile());
      storageService.store(bucketName, fileName.toString(), targetArray, "image/jpeg", null);
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }

    
  }

  @Override
  public void removeProductImage(ProductImage productImage) throws ServiceException {
    // delete all image sizes
    List<String> sizes = Arrays.asList(SMALL, LARGE);
    for (String size : sizes) {
      String filePath = filePath(productImage.getProduct().getMerchantStore().getCode(),
          productImage.getProduct().getSku(), size, productImage.getProductImage());
      try {
        boolean deleted = storageService.delete(bucketName(), filePath);
        if (!deleted) {
          LOGGER.error("Cannot delete image [" + productImage.getProductImage() + "]");
        }
      } catch (IOException e) {
        LOGGER.error("Error deleting image", e);
      }
    }
  
  }

  @Override
  public void removeProductImages(Product product) throws ServiceException {


    String bucketName = bucketName();
    try {
      List<String> blobNames = storageService.list(bucketName, product.getSku());
      for (String name : blobNames) {
        storageService.delete(bucketName, name);
      }
    } catch (IOException e) {
      LOGGER.error("Error while removing product images", e);
      throw new ServiceException(e);
    }

  }

  @Override
  public void removeImages(String merchantStoreCode) throws ServiceException {

    String bucketName = bucketName();
    try {
      List<String> blobNames = storageService.list(bucketName, merchantStoreCode);
      for (String name : blobNames) {
        storageService.delete(bucketName, name);
      }
    } catch (IOException e) {
      LOGGER.error("Error while removing images", e);
      throw new ServiceException(e);
    }

  }
  
  private String bucketName() {
    String bucketName = gcpAssetsManager.getRootName();
    if (StringUtils.isBlank(bucketName)) {
      bucketName = DEFAULT_BUCKET_NAME;
    }
    return bucketName;
  }
  
  private boolean bucketExists(String bucketName) {
    try {
      return storageService.bucketExists(bucketName);
    } catch (IOException e) {
      LOGGER.error("Error checking bucket existence", e);
      return false;
    }
  }
  
  private void createBucket(String bucketName) {
    try {
      storageService.createBucket(bucketName);
    } catch (IOException e) {
      LOGGER.error("Error creating bucket", e);
    }
  }
  
  private String filePath(String merchant, String sku, FileContentType contentImage) {
      StringBuilder sb = new StringBuilder();
      sb.append("products").append(Constants.SLASH);
      sb.append(merchant)
      .append(Constants.SLASH).append(sku).append(Constants.SLASH);

      // small large
      if (contentImage.name().equals(FileContentType.PRODUCT.name())) {
        sb.append(SMALL);
      } else if (contentImage.name().equals(FileContentType.PRODUCTLG.name())) {
        sb.append(LARGE);
      }

      return sb.append(Constants.SLASH).toString();
    
  }
  
  private String filePath(String merchant, String sku, String size, String fileName) {
    StringBuilder sb = new StringBuilder();
    sb.append("products").append(Constants.SLASH);
    sb.append(merchant)
    .append(Constants.SLASH).append(sku).append(Constants.SLASH);
    
    sb.append(size);
    sb.append(Constants.SLASH).append(fileName);

    return sb.toString();
  
  }


}