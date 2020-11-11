/*
 * MIT License
 *
 * Copyright (c) 2020 Alen Turkovic
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.github.alturkovic.lock.redis.impl;

import com.github.alturkovic.lock.AbstractSimpleLock;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * Works the same way as {@link MultiRedisLock} but is optimized better to work with a single key.
 */
@Slf4j
public class SimpleRedisLock extends AbstractSimpleLock {
  private static final String LOCK_SCRIPT = "return redis.call('SET', KEYS[1], ARGV[1], 'PX', tonumber(ARGV[2]), 'NX') and true or false";

  private static final String LOCK_FIND_EXISTING_SCRIPT = "return redis.call('GET', KEYS[1]) == ARGV[1] or false";
  
  private static final String LOCK_RELEASE_SCRIPT = "return redis.call('GET', KEYS[1]) == ARGV[1] and (redis.call('DEL', KEYS[1]) == 1) or false";

  private static final String LOCK_REFRESH_SCRIPT = "if redis.call('GET', KEYS[1]) == ARGV[1] then\n" +
    "    redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]))\n" +
    "    return true\n" +
    "end\n" +
    "return false";

  private final RedisScript<Boolean> lockScript = new DefaultRedisScript<>(LOCK_SCRIPT, Boolean.class);
  private final RedisScript<Boolean> lockExistingScript = new DefaultRedisScript<>(LOCK_FIND_EXISTING_SCRIPT, Boolean.class);
  private final RedisScript<Boolean> lockReleaseScript = new DefaultRedisScript<>(LOCK_RELEASE_SCRIPT, Boolean.class);
  private final RedisScript<Boolean> lockRefreshScript = new DefaultRedisScript<>(LOCK_REFRESH_SCRIPT, Boolean.class);

  private final StringRedisTemplate stringRedisTemplate;

  public SimpleRedisLock(final Supplier<String> tokenSupplier, final StringRedisTemplate stringRedisTemplate) {
    super(tokenSupplier);
    this.stringRedisTemplate = stringRedisTemplate;
  }

  /**
   * If alreadyHeldToken was passed, first check in the store that the key is already held with that token. If true, return with the token.
   * 
   * 
   * @param key
   * @param storeId
   * @param token
   * @param expiration
   * @param alreadyHeldToken set if reentrancy is enabled and this Thread already holds a lock on this key
   * @return
   */
  @Override
  protected String acquire(final String key, final String storeId, final String token, final long expiration, final String alreadyHeldToken) {
    final List<String> singletonKeyList = Collections.singletonList(storeId + ":" + key);
    if (alreadyHeldToken != null) {
      log.trace("alreadyHeldToken passed, checking to see if existing lock has that token: {}", key, storeId, alreadyHeldToken);
      final boolean alreadyLocked = stringRedisTemplate.execute(lockExistingScript, singletonKeyList, alreadyHeldToken);
      if (alreadyLocked) {
        log.debug("Lock for key {} in store {}, is already held by this Thread, returning TOKEN_RESPONSE_ALL_KEYS_ALREADY_HELD.", key, storeId);
        return alreadyHeldToken;
      }
      log.warn("Lock for key {} in store {}, is not already held by this Thread despite alreadyHeldToken - maybe it has reached TTL. Will try to grab it...", key, storeId);
    }
    
    //either alreadyHeldToken was not passed or the key is not in the store with it. 
    //TODO enhance redis query LOCK_SCRIPT to return true if an entry exists for the key and has the value if alreadyHeldToken?
    final boolean locked = stringRedisTemplate.execute(lockScript, singletonKeyList, token, String.valueOf(expiration));
    log.debug("Tried to acquire lock for key {} with token {} in store {}. Locked: {}", key, token, storeId, locked);
    return locked ? token : null;
  }

  @Override
  protected boolean release(final String key, final String storeId, final String token) {
    final List<String> singletonKeyList = Collections.singletonList(storeId + ":" + key);

    final boolean released = stringRedisTemplate.execute(lockReleaseScript, singletonKeyList, token);
    if (released) {
      log.debug("Release script deleted the record for key {} with token {} in store {}", key, token, storeId);
    } else {
      log.error("Release script failed for key {} with token {} in store {}", key, token, storeId);
    }
    return released;
  }

  @Override
  protected boolean refresh(final String key, final String storeId, final String token, final long expiration) {
    final List<String> singletonKeyList = Collections.singletonList(storeId + ":" + key);

    final boolean refreshed = stringRedisTemplate.execute(lockRefreshScript, singletonKeyList, token, String.valueOf(expiration));
    if (refreshed) {
      log.debug("Refresh script updated the expiration for key {} with token {} in store {} to {}", key, token, storeId, expiration);
    } else {
      log.debug("Refresh script failed to update expiration for key {} with token {} in store {} with expiration: {}", key, token, storeId, expiration);
    }
    return refreshed;
  }
}