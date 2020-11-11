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

package com.github.alturkovic.lock;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * An abstract lock used as a base for all locks that operate with only 1 key instead of multiple keys.
 */
@Slf4j
@Data
public abstract class AbstractSimpleLock implements Lock {
  private final Supplier<String> tokenSupplier;

  @Override
  public String acquire(final List<String> keys, final String storeId, final long expiration) {
    log.debug("acquire(): called with keys: '" + keys + "'...");
    Assert.isTrue(keys.size() == 1, "Cannot acquire lock for multiple keys with this lock");

    final String token = tokenSupplier.get();
    if (StringUtils.isEmpty(token)) {
      throw new IllegalStateException("Cannot lock with empty token");
    }

    Map<String, String> alreadyHeldKeyTokens = ReentrantUtils.getAlreadyHeldKeysAndTokens();
    String alreadyHeldTokenForOnlyKey = null;
    if (alreadyHeldKeyTokens != null) {
      alreadyHeldTokenForOnlyKey = alreadyHeldKeyTokens.get(keys.get(0));
    }
    String newToken = acquire(keys.get(0), storeId, token, expiration, alreadyHeldTokenForOnlyKey);
    
    if (newToken != null) {
      //acquired ok, possibly using existing token

      //TODO only if reentrancy is enabled for this acquire()
      
      Map<String, String> updatedKeys = new HashMap<>();
      updatedKeys.put(keys.get(0), newToken);
      
      ReentrantUtils.updateAllocationsAfterLock(updatedKeys, newToken);
      return newToken;
    } else {
      return null;
    }
  }

  @Override
  public boolean release(final Collection<String> keys, final String storeId, final String token) {
    Assert.isTrue(keys.size() == 1, "Cannot release lock for multiple keys with this lock");
    
    log.debug("release(): called with keys: " + keys + ", calling decrementAndGetTokensToReleaseFromStore()...");
    Map<String, String> keysWithTokensToReleaseFromStores = ReentrantUtils.decrementAndGetTokensToReleaseFromStore(keys, token);
    log.trace("release(): decrementAndGetTokensToReleaseFromStore() returned " + keysWithTokensToReleaseFromStores.size() + " keysWithTokensToReleaseFromStores...");
    
    if (! keysWithTokensToReleaseFromStores.isEmpty()) {
      //if there has been a TTL expiry and a key has been re-acquired since then the token passed from the original locker's scope will be wrong.
      //the correct token will be in the map keysWithTokensToReleaseFromStores
      String firstKey = keysWithTokensToReleaseFromStores.keySet().iterator().next();
      String firstToken = keysWithTokensToReleaseFromStores.get(firstKey);
      return(release(firstKey, storeId, firstToken));
    } else {
      log.error("release(): decrementAndGetTokensToReleaseFromStore() returned empty Map. Not releasing.");
      return(false);
    }
  }

  @Override
  public boolean refresh(final List<String> keys, final String storeId, final String token, final long expiration) {
    Assert.isTrue(keys.size() == 1, "Cannot refresh lock for multiple keys with this lock");
    return refresh(keys.get(0), storeId, token, expiration);
  }

  protected abstract String acquire(String key, String storeId, String token, long expiration, final String alreadyHeldKeyToken);
  protected abstract boolean release(String key, String storeId, String token);
  protected abstract boolean refresh(String key, String storeId, String token, long expiration);
}
