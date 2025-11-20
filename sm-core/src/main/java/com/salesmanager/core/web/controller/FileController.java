package com.salesmanager.core.web.controller;

import java.net.URLConnection;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.StreamingHttpOutputMessage;
import org.springframework.http.converter.ResourceHttpMessageConverter;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.salesmanager.core.business.storage.FileAccessTokenService;
import com.salesmanager.core.business.storage.FileAccessTokenService.FileToken;
import com.salesmanager.core.business.storage.StorageService;

import org.springframework.core.io.InputStreamResource;

@Controller
@RequestMapping("/api/files")
public class FileController {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileController.class);

  @Autowired
  private StorageService storageService;

  @Autowired
  private FileAccessTokenService tokenService;

  /**
   * Stream a file stored in the configured StorageService using a short-lived token.
   * Example: GET /api/files?bucket=shopizer&path=products/merchant/sku/SMALL/img.jpg&token=...
   */
  @GetMapping
  public ResponseEntity<?> getFile(@RequestParam String bucket, @RequestParam String path,
      @RequestParam String token) {
    try {
      FileToken tk = tokenService.validateToken(token);
      if (tk == null) {
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("Invalid or expired token");
      }
      // ensure token matches requested resource
      if (!tk.bucket.equals(bucket) || !tk.path.equals(path)) {
        return ResponseEntity.status(HttpStatus.FORBIDDEN).body("Token does not match resource");
      }

      java.io.InputStream is = storageService.getInputStream(bucket, path);
      if (is == null) {
        return ResponseEntity.notFound().build();
      }

      String mime = URLConnection.guessContentTypeFromName(path);
      if (mime == null) {
        mime = MediaType.APPLICATION_OCTET_STREAM_VALUE;
      }

      InputStreamResource resource = new InputStreamResource(is);
      return ResponseEntity.ok().header(HttpHeaders.CONTENT_TYPE, mime).body(resource);
    } catch (Exception e) {
      LOGGER.error("Error streaming file", e);
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error streaming file");
    }
  }

}
