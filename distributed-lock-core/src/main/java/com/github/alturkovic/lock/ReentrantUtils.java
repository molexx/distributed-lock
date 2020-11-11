package com.github.alturkovic.lock;

import com.github.alturkovic.lock.exception.DistributedLockException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReentrantUtils {

  /**
   * Instances of this are stored in ThreadLocal to keep track of re-entrant locks
   */
  private static class TokenAndCount {
    String token;
    AtomicInteger count = new AtomicInteger(1);
    boolean reAcquiredAfterExpiry = false;

    TokenAndCount(String token) {
      this.token = token;
    }
  }
  
  private static ThreadLocal<Map<String, TokenAndCount>> threadsHeldLocks = null;
  
  private static Map<String, TokenAndCount> createThreadLocalMap() {
    threadsHeldLocks = ThreadLocal.withInitial(() -> {
      log.debug("ThreadLocal.initialValue(): creating threadsHeldLocks Map<String, TokenAndCount> for Thread '" + Thread.currentThread().getName() + "'...");
      return new HashMap<>();
    });
    return threadsHeldLocks.get();
  }
  
  private static Map<String, TokenAndCount> getOrCreateThreadLocalMap() {
    if (threadsHeldLocks == null) {
      createThreadLocalMap();
    }
    return threadsHeldLocks.get();
  }

  /**
   * Does not create if it does not already exist
   */
  private static Map<String, TokenAndCount> getThreadLocalMap() {
    if (threadsHeldLocks == null) {
      return null;
    }
    return threadsHeldLocks.get();
  }
  
  public static void clearThreadLocalMap() {
    log.debug("clearThreadLocalMap(): called.");
    threadsHeldLocks = null;
  }
  
  /**
   * If reentrancy is enabled this is called by each store implementations' acquire() method when a lock has been successfully acquired from the store - either:
   * - newly locked, or 
   * - an existing lock that has the correct token in this Thread's Map
   * - an expired lock that has been freshly acquired but still has the expired token in this Thread's Map
   * 
   * 
   * @param lockedKeysAndTokens keys that have been newly locked or locked again
   * @param newToken token used for new locks in the store
   */
  public static void updateAllocationsAfterLock(Map<String, String> lockedKeysAndTokens, String newToken) {
    log.trace("updateAllocationsAfterLock(): called with keys '" + lockedKeysAndTokens + "', token: " + newToken);
        
    //will create the ThreadLocal Map if it does not already exist
    Map<String, TokenAndCount> existingHeldLocks = getOrCreateThreadLocalMap();
    
    log.trace("updateAllocationsAfterLock(): existingHeldLocks.size(): " + existingHeldLocks.size() + ".");
    
    //If we already have any of the lockedKeysAndTokens keys in this Thead's Map, check that we have the same token stored with it.
    //If the token is different that (probably) means that the store record's TTL was reached and it expired, and was then re-acquired by the calling acquire() method.
    //In this case we need to update the token we are storing so that we can use it at release time.
    lockedKeysAndTokens.forEach((key, token) -> {
      if (existingHeldLocks.containsKey(key)) {
        TokenAndCount existingTac = existingHeldLocks.get(key);
        if (!existingTac.token.equals(token)) {
          //The acquiration of the key has reached TTL and then been re-acquired.
          //The old token stored against the key in the LockContext, ThreadLocal Map and calling code scope is now wrong
          log.trace("updateAllocationsAfterLock(): key already allocated to Thread '" + Thread.currentThread() + "' must have reached TTL and been re-acquired as the new token is different.");
          //don't remove the allocation because the key is still in use by earlier callers and we need to keep the count so it is not prematurely released       existingHeldLocks.remove(updatedKey);

          existingTac.token = token;
          existingTac.reAcquiredAfterExpiry = true;
        }
      }
    });
    
    log.debug("updateAllocationsAfterLock(): calling incrementThreadLock()...");
    incrementThreadLock(lockedKeysAndTokens, newToken); //, true);
  }
  
  /**
   * Formats the threadsHeldLocks Map to parse out a Map of key, token
   * @return
   */
  public static Map<String, String> getAlreadyHeldKeysAndTokens() {
    Map<String, TokenAndCount> map = getThreadLocalMap();
    if (map == null) {
      return(null);
    }
    return map.entrySet().stream()
      .map(e -> new SimpleEntry<>(e.getKey(), e.getValue().token))
      .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
  }



  /**
   * Adds an entry to the ThreadLocal Map for this key, or increments the count on an existing entry.
   *
   * @param lockedKeysAndTokens
   * @param newToken
   */
  private static void incrementThreadLock(Map<String, String> lockedKeysAndTokens, String newToken) {
    log.trace("incrementThreadLock(): called, lockedKeysAndTokens.size: " + lockedKeysAndTokens.size() + "...");
    Map<String, TokenAndCount> locksForThread = getOrCreateThreadLocalMap();

    //no need for synchronized as this code can only run once at a time in the same Thread!
    lockedKeysAndTokens.forEach((key, keysToken) -> {
      TokenAndCount tac = locksForThread.get(key);
      boolean keyIsNew = tac != null;
      log.trace("incrementThreadLock(): incrementing count for '" + key + "'. keyIsNew: " + keyIsNew + ", TokenAndCount for key: " + tac + "...");
      if (tac == null) {
        //a new key not already in the Map, add it
        log.debug("incrementThreadLock(): initialising lock count for key '" + key + "'...");
        tac = new TokenAndCount(newToken);   //store only the token used to grab the first lock. AtomicInteger 'count' is Initialised with value 1.
        locksForThread.put(key, tac);
      } else {
        //key is already in the map, increment its count
        int count = tac.count.incrementAndGet();
        
        log.debug("incrementThreadLock(): incremented count for '" + key + "', now: " + count + (count == 0 ? ", removed entry" : "") + ".");
      }
    });
    log.trace("incrementThreadLock(): done.");
  }
  
  /**
   * Called by release(), before the store's release() is called.
   * Will:
   *   - sanity check that acquire() was called on the same key
   *   - in the case that this key is already held more than once by the same thread, decrement the count
   *
   * Returns a Map of keys and tokens for which there are no further locks held by this Thread. 
   * If all passed keys still have outstanding locks from other calls in this Thread then an empty map is returned
   * If a lock had previously timed out (reached TTL) and was re-acquired from the store the caller's code-scoped token will be invalid - this will return the token needed to release.  
   * 
   * @param keysToRelease
   * @param token
   * @return
   */
  public static Map<String, String> decrementAndGetTokensToReleaseFromStore(final Collection<String> keysToRelease, String token) {
    log.trace("decrementAndGetTokensToReleaseFromStore(): called, keysToRelease.size: " + keysToRelease.size() + "...");
    
    
    Map<String, TokenAndCount> locksForThread = getThreadLocalMap();  //does not create if does not exist
    log.trace("decrementAndGetTokensToReleaseFromStore(): locksForThread: '" + locksForThread + "'...");
    if (locksForThread != null) {
      log.trace("decrementAndGetTokensToReleaseFromStore(): locksForThread has {} entries", locksForThread.size());
    }
    
    
    Map<String, String> keysToReleaseFromStore = new HashMap<>();
    keysToRelease.forEach((key) -> {
      TokenAndCount tac = null;
      
      if (locksForThread != null) {
        tac = locksForThread.get(key);
        log.trace("decrementAndGetTokensToReleaseFromStore(): locksForThread is not null, tac for key {} count: {}...", key, tac.count);
      }
      
      if (tac == null) {
        //this happens if reentrancy was disabled for the acquiration, just add to the returned Map with the provided token
        keysToReleaseFromStore.put(key, token);
        log.trace("decrementAndGetTokensToReleaseFromStore(): locksForThread is null or contained no tac for key {}, added to keysToReleaseFromStore, size now: {}", key, keysToReleaseFromStore.size());
      } else {
        int count = tac.count.decrementAndGet();
        //tidy up if we've decremented all the way to 0
        if (count == 0) {
          //sanity check every acquire() is matched with a release()
          //passed release token can be wrong if previous lock reached TTL and was re-acquired with a different token
          
          if (!tac.reAcquiredAfterExpiry && !tac.token.equals(token)) {
            throw (new DistributedLockException(
              "manipulateThreadLock() this Thread's list of already held keys contains key '" + key + "' and we have just reduced its count to 0, but it was originally " +
                "grabbed using a different token to the one passed in to this call. Originally stored: '" + tac.token + "', this call: '" + token + "'."));
          }
          locksForThread.remove(key);
          
          keysToReleaseFromStore.put(key, tac.token);
        }
      }
    });
    
    log.trace("decrementAndGetTokensToReleaseFromStore(): done, returning: " + keysToReleaseFromStore.size() + " keysToReleaseFromStore.");
    return(keysToReleaseFromStore);
  }

}
