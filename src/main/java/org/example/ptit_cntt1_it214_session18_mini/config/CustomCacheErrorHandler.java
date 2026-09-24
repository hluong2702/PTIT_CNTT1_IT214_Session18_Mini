package org.example.ptit_cntt1_it214_session18_mini.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.Cache;
import org.springframework.cache.interceptor.CacheErrorHandler;

/**
 * CustomCacheErrorHandler giúp hệ thống xử lý ngoại lệ khi thao tác với Cache (Redis).
 * Áp dụng chiến lược Silent Fail & High Availability:
 * - Khi Redis gặp sự cố (mất kết nối, timeout, sập node), không ném ngoại lệ làm chết ứng dụng.
 * - Thay vào đó, ghi log cảnh báo rõ ràng để giám sát.
 * - Tự động fallback truy vấn trực tiếp xuống Database.
 */
@Slf4j
public class CustomCacheErrorHandler implements CacheErrorHandler {

    @Override
    public void handleCacheGetError(RuntimeException exception, Cache cache, Object key) {
        log.warn("[CACHE ERROR] Redis error during GET for cache '{}', key '{}'. Falling back to Database. Error: {}",
                cache.getName(), key, exception.getMessage());
    }

    @Override
    public void handleCachePutError(RuntimeException exception, Cache cache, Object key, Object value) {
        log.warn("[CACHE ERROR] Redis error during PUT for cache '{}', key '{}'. Continuing without cache. Error: {}",
                cache.getName(), key, exception.getMessage());
    }

    @Override
    public void handleCacheEvictError(RuntimeException exception, Cache cache, Object key) {
        log.warn("[CACHE ERROR] Redis error during EVICT for cache '{}', key '{}'. Error: {}",
                cache.getName(), key, exception.getMessage());
    }

    @Override
    public void handleCacheClearError(RuntimeException exception, Cache cache) {
        log.warn("[CACHE ERROR] Redis error during CLEAR for cache '{}'. Error: {}",
                cache.getName(), exception.getMessage());
    }
}
