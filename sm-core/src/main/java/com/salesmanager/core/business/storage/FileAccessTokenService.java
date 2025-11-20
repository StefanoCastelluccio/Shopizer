package com.salesmanager.core.business.storage;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Simple HMAC-based short-lived token generator and validator for file access.
 *
 * Token format: base64url(payload) . base64url(signature)
 * payload = URLEncode(bucket) + '|' + URLEncode(path) + '|' + expiryEpochSeconds
 */
@Component
public class FileAccessTokenService {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileAccessTokenService.class);

  private static final String HMAC_ALGO = "HmacSHA256";

  @Value("${file.token.secret:change-me}")
  private String secret;

  public String generateToken(String bucket, String path, long ttlSeconds) {
    long expiry = Instant.now().getEpochSecond() + ttlSeconds;
    String payload = encode(bucket) + "|" + encode(path) + "|" + expiry;
    String payloadB64 = Base64.getUrlEncoder().withoutPadding().encodeToString(payload.getBytes(StandardCharsets.UTF_8));
    String sig = hmac(payload);
    String sigB64 = Base64.getUrlEncoder().withoutPadding().encodeToString(sig.getBytes(StandardCharsets.UTF_8));
    return payloadB64 + "." + sigB64;
  }

  public FileToken validateToken(String token) {
    try {
      if (token == null || !token.contains(".")) {
        return null;
      }
      String[] parts = token.split("\\.", 2);
      String payloadB64 = parts[0];
      String sigB64 = parts[1];
      String payload = new String(Base64.getUrlDecoder().decode(payloadB64), StandardCharsets.UTF_8);
      String expectedSig = hmac(payload);
      String expectedSigB64 = Base64.getUrlEncoder().withoutPadding().encodeToString(expectedSig.getBytes(StandardCharsets.UTF_8));
      if (!constantTimeEquals(expectedSigB64, sigB64)) {
        LOGGER.debug("Token signature mismatch");
        return null;
      }
      String[] partsPayload = payload.split("\\|", 3);
      if (partsPayload.length != 3) {
        return null;
      }
      String bucket = decode(partsPayload[0]);
      String path = decode(partsPayload[1]);
      long expiry = Long.parseLong(partsPayload[2]);
      if (Instant.now().getEpochSecond() > expiry) {
        LOGGER.debug("Token expired");
        return null;
      }
      return new FileToken(bucket, path, expiry);
    } catch (Exception e) {
      LOGGER.debug("Invalid token", e);
      return null;
    }
  }

  private String hmac(String payload) {
    try {
      Mac mac = Mac.getInstance(HMAC_ALGO);
      SecretKeySpec keySpec = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), HMAC_ALGO);
      mac.init(keySpec);
      byte[] sig = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
      return new String(Base64.getUrlEncoder().withoutPadding().encode(sig), StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new RuntimeException("Unable to compute HMAC", e);
    }
  }

  private static String encode(String s) {
    try {
      return URLEncoder.encode(s == null ? "" : s, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  private static String decode(String s) {
    try {
      return URLDecoder.decode(s == null ? "" : s, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean constantTimeEquals(String a, String b) {
    if (a == null || b == null) return false;
    if (a.length() != b.length()) return false;
    int result = 0;
    for (int i = 0; i < a.length(); i++) {
      result |= a.charAt(i) ^ b.charAt(i);
    }
    return result == 0;
  }

  public static class FileToken {
    public final String bucket;
    public final String path;
    public final long expiry;

    public FileToken(String bucket, String path, long expiry) {
      this.bucket = bucket;
      this.path = path;
      this.expiry = expiry;
    }
  }

}
