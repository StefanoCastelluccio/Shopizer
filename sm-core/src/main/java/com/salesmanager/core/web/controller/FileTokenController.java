package com.salesmanager.core.web.controller;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.salesmanager.core.business.storage.FileAccessTokenService;

/**
 * Token generation endpoint for files. Secured: only callers with role AUTH may
 * generate tokens. If you want a different access rule, adjust the
 * @PreAuthorize expression.
 */
@RestController
@PreAuthorize("hasRole('AUTH')")
@RequestMapping("/api/files")
public class FileTokenController {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileTokenController.class);

  @Autowired
  private FileAccessTokenService tokenService;

  @GetMapping("/token")
  public ResponseEntity<?> generateToken(@RequestParam String bucket, @RequestParam String path,
      @RequestParam(required = false, defaultValue = "300") long ttlSeconds) {
    try {
      String token = tokenService.generateToken(bucket, path, ttlSeconds);
      Map<String, Object> body = new HashMap<>();
      body.put("token", token);
      body.put("expiresAt", Instant.now().getEpochSecond() + ttlSeconds);
      return ResponseEntity.ok(body);
    } catch (Exception e) {
      LOGGER.error("Error generating token", e);
      return ResponseEntity.status(500).body("Error generating token");
    }
  }

}
