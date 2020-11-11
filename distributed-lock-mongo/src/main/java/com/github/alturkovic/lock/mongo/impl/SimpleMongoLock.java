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

package com.github.alturkovic.lock.mongo.impl;

import com.github.alturkovic.lock.AbstractSimpleLock;
import com.github.alturkovic.lock.mongo.model.LockDocument;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

@Slf4j
public class SimpleMongoLock extends AbstractSimpleLock {
  private final MongoTemplate mongoTemplate;

  public SimpleMongoLock(final Supplier<String> tokenSupplier, final MongoTemplate mongoTemplate) {
    super(tokenSupplier);
    this.mongoTemplate = mongoTemplate;
  }

  @Override
  protected String acquire(final String key, final String storeId, final String token, final long expiration, final String alreadyHeldKeyToken) {
    final Query query = Query.query(Criteria.where("_id").is(key));
    final Update update = new Update()
      .setOnInsert("_id", key)
      .setOnInsert("expireAt", LocalDateTime.now().plus(expiration, ChronoUnit.MILLIS))
      .setOnInsert("token", token);

    final FindAndModifyOptions options = new FindAndModifyOptions().upsert(true).returnNew(true);
    final LockDocument doc = mongoTemplate.findAndModify(query, update, options, LockDocument.class, storeId);

    final boolean locked = doc.getToken().equals(token);
    log.debug("Tried to acquire lock for key {} with token {} in store {}. Locked: {}", key, token, storeId, locked);
    return locked ? token : null;
  }

  @Override
  protected boolean release(final String key, final String storeId, final String token) {
    final DeleteResult deleted = mongoTemplate.remove(Query.query(Criteria.where("_id").is(key).and("token").is(token)), storeId);
    final boolean released = deleted.getDeletedCount() == 1;
    if (released) {
      log.debug("Remove query successfully affected 1 record for key {} with token {} in store {}", key, token, storeId);
    } else if (deleted.getDeletedCount() > 0) {
      log.error("Unexpected result from release for key {} with token {} in store {}, released {}", key, token, storeId, deleted);
    } else {
      log.error("Remove query did not affect any records for key {} with token {} in store {}", key, token, storeId);
    }

    return released;
  }

  @Override
  protected boolean refresh(final String key, final String storeId, final String token, final long expiration) {
    final UpdateResult updated = mongoTemplate.updateFirst(Query.query(Criteria.where("_id").is(key).and("token").is(token)),
      Update.update("expireAt", LocalDateTime.now().plus(expiration, ChronoUnit.MILLIS)),
      storeId);

    final boolean refreshed = updated.getModifiedCount() == 1;
    if (refreshed) {
      log.debug("Refresh query successfully affected 1 record for key {} with token {} in store {}", key, token, storeId);
    } else if (updated.getModifiedCount() > 0) {
      log.error("Unexpected result from refresh for key {} with token {} in store {}, released {}", key, token, storeId, updated);
    } else {
      log.warn("Refresh query did not affect any records for key {} with token {} in store {}. This is possible when refresh interval fires for the final time after the lock has been released",
        key, token, storeId);
    }

    return refreshed;
  }
}