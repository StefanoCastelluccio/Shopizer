package com.salesmanager.core.business.modules.cms.content.gcp;

import java.io.IOException;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.salesmanager.core.business.storage.StorageService;
import com.salesmanager.core.business.exception.ServiceException;
import com.salesmanager.core.business.modules.cms.content.ContentAssetsManager;
import com.salesmanager.core.business.modules.cms.impl.CMSManager;
import com.salesmanager.core.model.content.FileContentType;
import com.salesmanager.core.model.content.InputContentFile;
import com.salesmanager.core.model.content.OutputContentFile;

/**
 * Content management for a given retailer using GC{ (Google Cloud Platform)
 * Cloud Storage with buckets
 * 
 * 
 * Linux/ Mac export GOOGLE_APPLICATION_CREDENTIALS="/path/to/file" For Windows
 * set GOOGLE_APPLICATION_CREDENTIALS="C:\path\to\file"
 * 
 * following this article: https://www.baeldung.com/java-google-cloud-storage
 * 
 * gsutil ls -L -b gs://shopizer-content
 * 
 * 
 * @author carlsamson
 *
 */
@Component("gcpContentAssetsManager")
public class GCPStaticContentAssetsManagerImpl implements ContentAssetsManager {

  private static final long serialVersionUID = 1L;

  private static final Logger LOGGER = LoggerFactory.getLogger(GCPStaticContentAssetsManagerImpl.class);

  @Autowired
  @Qualifier("gcpAssetsManager")
  private CMSManager cmsManager;

	@Autowired
	private StorageService storageService;

  @Override
	public OutputContentFile getFile(String merchantStoreCode, Optional<String> folderPath, FileContentType fileContentType, String contentName)
			throws ServiceException {
    try {
      String bucketName = bucketName();
				byte[] content = storageService.getContent(bucketName, nodePath(merchantStoreCode, fileContentType) + contentName);
				LOGGER.info("Content getFile");
				return getOutputContentFile(content);

    } catch (Exception e) {
      LOGGER.error("Error while getting file", e);
      throw new ServiceException(e);
    }
  }

	@Override
	public List<String> getFileNames(String merchantStoreCode, Optional<String> folderPath, FileContentType fileContentType)
			throws ServiceException {
		try {
			String bucketName = bucketName();
				List<String> blobNames = storageService.list(bucketName, nodePath(merchantStoreCode, fileContentType));
	
			List<String> fileNames = new ArrayList<String>();

				for (String blobName : blobNames) {
					if (isInsideSubFolder(blobName))
						continue;
					String mimetype = URLConnection.guessContentTypeFromName(blobName);
					if (!StringUtils.isBlank(mimetype)) {
						fileNames.add(getName(blobName));
					}
				}
	
			LOGGER.info("Content get file names");
			return fileNames;
		} catch (Exception e) {
			LOGGER.error("Error while getting file names", e);
			throw new ServiceException(e);
		}
	}

	@Override
	public List<OutputContentFile> getFiles(String merchantStoreCode, Optional<String> folderPath, FileContentType fileContentType)
			throws ServiceException {
		try {

			List<String> fileNames = getFileNames(merchantStoreCode, folderPath, fileContentType);
			List<OutputContentFile> files = new ArrayList<OutputContentFile>();
		
			for (String fileName : fileNames) {
				files.add(getFile(merchantStoreCode, folderPath, fileContentType, fileName));
			}
		
			LOGGER.info("Content get file names");
			return files;
		} catch (Exception e) {
		LOGGER.error("Error while getting file names", e);
		throw new ServiceException(e);
		}
	}

	@Override
	public void addFile(String merchantStoreCode, Optional<String> folderPath, InputContentFile inputStaticContentData) throws ServiceException {

		try {
			LOGGER.debug("Adding file " + inputStaticContentData.getFileName());
			String bucketName = bucketName();
	  
			String nodePath = nodePath(merchantStoreCode, inputStaticContentData.getFileContentType());
	  
				byte[] targetArray = new byte[inputStaticContentData.getFile().available()];
				inputStaticContentData.getFile().read(targetArray);
				storageService.store(bucketName, nodePath + inputStaticContentData.getFileName(), targetArray,
						inputStaticContentData.getFileContentType().name(), null);
				LOGGER.info("Content add file");
		} catch (IOException e) {
			LOGGER.error("Error while adding file", e);
			throw new ServiceException(e);
		}
	}

	@Override
	public void addFiles(String merchantStoreCode, Optional<String> folderPath, List<InputContentFile> inputStaticContentDataList)
			throws ServiceException {
		if (CollectionUtils.isNotEmpty(inputStaticContentDataList)) {
			for (InputContentFile inputFile : inputStaticContentDataList) {
				this.addFile(merchantStoreCode, folderPath, inputFile);
			}
		}
	}

	@Override
	public void removeFile(String merchantStoreCode, FileContentType staticContentType, String fileName, Optional<String> folderPath)
			throws ServiceException {
		try {
			String bucketName = bucketName();
				storageService.delete(bucketName, nodePath(merchantStoreCode, staticContentType) + fileName);
		
			LOGGER.info("Remove file");
		} catch (final Exception e) {
			LOGGER.error("Error while removing file", e);
			throw new ServiceException(e);
		}			  
	}

	@Override
	public void removeFiles(String merchantStoreCode, Optional<String> folderPath) throws ServiceException {
		try {
			// get buckets
			String bucketName = bucketName();

				List<String> names = storageService.list(bucketName, nodePath(merchantStoreCode));
				for (String n : names) {
					storageService.delete(bucketName, n);
				}
	
			LOGGER.info("Remove folder");
		} catch (final Exception e) {
			LOGGER.error("Error while removing folder", e);
			throw new ServiceException(e);	
		}
	}

  
	public CMSManager getCmsManager() {
		return cmsManager;
	}

	public void setCmsManager(CMSManager cmsManager) {
		this.cmsManager = cmsManager;
	}


	@Override
	public void addFolder(String merchantStoreCode, String folderName, Optional<String> folderPath) throws ServiceException {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeFolder(String merchantStoreCode, String folderName, Optional<String> folderPath) throws ServiceException {
		// TODO Auto-generated method stub

	}

	@Override
	public List<String> listFolders(String merchantStoreCode, Optional<String> folderPath) throws ServiceException {
		// TODO Auto-generated method stub
		return null;
	}

}
