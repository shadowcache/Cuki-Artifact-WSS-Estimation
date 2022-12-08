/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.cache.cuckoofilter;

import alluxio.Constants;
import alluxio.client.file.cache.ShadowCacheParameters;
import alluxio.client.file.cache.cuckoofilter.size.ISizeEncoder;
import alluxio.client.file.cache.cuckoofilter.size.LogSizeEncoder;
import alluxio.client.file.cache.cuckoofilter.size.NoOpSizeEncoder;
import alluxio.client.file.cache.cuckoofilter.size.SizeEncodeType;
import alluxio.client.file.cache.cuckoofilter.size.SizeEncoder;
import alluxio.client.file.cache.cuckoofilter.size.TruncateSizeEncoder;
import alluxio.client.quota.CacheScope;
import alluxio.collections.BitSet;
import alluxio.collections.SimpleBitSet;
import alluxio.util.FormatUtils;

import com.google.common.base.Preconditions;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A concurrent cuckoo filter with three customized field: clock, size and scope.
 *
 * @param <T> the type of item
 */
public class SimpleClockCuckooFilter<T> implements ClockCuckooFilter<T>, Serializable {
  private static final long serialVersionUID = 20221217L;

  private static final double DEFAULT_FPP = 0.01;
  // The default load factor is from "Cuckoo Filter: Practically Better Than Bloom" by Fan et al.
  private static final double DEFAULT_LOAD_FACTOR = 0.955;
  private static final int DEFAULT_NUM_LOCKS = 4096;
  // the maximum number of entries in a cuckoo path from "Algorithmic Improvements for Fast
  // Concurrent Cuckoo Hashing" by Li et al.
  private static final int MAX_BFS_PATH_LEN = 5;
  // aging configurations
  // we do not want to block user operations for too long,
  // so hard limited the aging number of each operation.
  private static final int MAX_AGING_PER_OPERATION = 500;
  private static final int AGING_STEP_SIZE = 5;
  protected static int TAGS_PER_BUCKET = 4;
  private static final boolean MRC_OPEN = true;
  private final AtomicLong mTotalNum = new AtomicLong(0);
  private final int mNumBuckets;
  private final int mBitsPerTag;
  private final int mBitsPerClock;
  private final int mBitsPerSize;
  private final int mBitsPerScope;
  private final int mMaxSize;
  private final int mMaxAge;
  private final Funnel<? super T> mFunnel;
  private final HashFunction mHashFunction;
  private final ScopeEncoder mScopeEncoder;
  private final ISizeEncoder mSizeEncoder;

  // if count-based sliding window, windowSize is the number of operations;
  // if time-based sliding window, windowSize is the milliseconds in a period.
  private final SlidingWindowType mSlidingWindowType;
  private final long mWindowSize;
  private final long mStartTime = System.currentTimeMillis();
  protected final CuckooTable mTable;
  protected final CuckooTable mClockTable;
  protected final CuckooTable mSizeTable;
  protected final CuckooTable mScopeTable;
  private long[] clockDist = null;
  private static long[] RD = null;
  private static long RDWidth;
  private static long RDLength;

  /**
   * The constructor of concurrent clock cuckoo filter.
   *
   * @param table the table to store fingerprint
   * @param clockTable the table to store clock
   * @param sizeTable the table to store size
   * @param scopeTable the table to store scope
   * @param slidingWindowType the type of sliding window
   * @param windowSize the size of sliding window
   * @param funnel the funnel of T's that the constructed cuckoo filter will use
   * @param hasher the hash function the constructed cuckoo filter will use
   */
  protected SimpleClockCuckooFilter(CuckooTable table, CuckooTable clockTable,
                                    CuckooTable sizeTable, CuckooTable scopeTable, SlidingWindowType slidingWindowType,
                                    long windowSize, Funnel<? super T> funnel, HashFunction hasher) {
    mTable = table;
    mNumBuckets = table.getNumBuckets();
    mBitsPerTag = table.getBitsPerTag();
    mClockTable = clockTable;
    mBitsPerClock = clockTable.getBitsPerTag();
    mSizeTable = sizeTable;
    mBitsPerSize = sizeTable.getBitsPerTag();
    mScopeTable = scopeTable;
    mBitsPerScope = scopeTable.getBitsPerTag();
    mMaxSize = (1 << mBitsPerSize);
    mMaxAge = (1 << mBitsPerClock) - 1;
    mSlidingWindowType = slidingWindowType;
    mWindowSize = windowSize;
    mFunnel = funnel;
    mHashFunction = hasher;
    // init scope statistics
    // note that the GLOBAL scope is the default scope and is always encoded to zero
    mScopeEncoder = new ScopeEncoder(mBitsPerScope);
    mScopeEncoder.encode(CacheScope.GLOBAL);
    // init size encoder
    mSizeEncoder = new NoOpSizeEncoder(mBitsPerSize);
    int maxNumScopes = (1 << mBitsPerScope);
    // init aging pointers for each lock
    if(MRC_OPEN){
      clockDist = new long[mMaxAge+1];
    }else {
      clockDist = null;
    }
  }

  protected SimpleClockCuckooFilter(CuckooTable table, CuckooTable clockTable,
                                    CuckooTable sizeTable, CuckooTable scopeTable, SlidingWindowType slidingWindowType,
                                    long windowSize, ISizeEncoder sizeEncoder, Funnel<? super T> funnel, HashFunction hasher) {
    mTable = table;
    mNumBuckets = table.getNumBuckets();
    mBitsPerTag = table.getBitsPerTag();
    mClockTable = clockTable;
    mBitsPerClock = clockTable.getBitsPerTag();
    mSizeTable = sizeTable;
    mBitsPerSize = sizeTable.getBitsPerTag();
    mScopeTable = scopeTable;
    mBitsPerScope = scopeTable.getBitsPerTag();
    mMaxSize = (1 << mBitsPerSize);
    mMaxAge = (1 << mBitsPerClock) - 1;
    mSlidingWindowType = slidingWindowType;
    mWindowSize = windowSize;
    mFunnel = funnel;
    mHashFunction = hasher;
    // init scope statistics
    // note that the GLOBAL scope is the default scope and is always encoded to zero
    mScopeEncoder = new ScopeEncoder(mBitsPerScope);
    mScopeEncoder.encode(CacheScope.GLOBAL);
    mSizeEncoder = sizeEncoder;
    int maxNumScopes = (1 << mBitsPerScope);


    //init clock dist
    if(MRC_OPEN){
      clockDist = new long[mMaxAge+1];
    }else {
      clockDist = null;
    }
  }

  public static <T> SimpleClockCuckooFilter<T> create(Funnel<? super T> funnel,
                                                      ShadowCacheParameters conf) {
    // make expectedInsertions a power of 2
    TAGS_PER_BUCKET = conf.mTagsPerBucket;
    int bitsPerTag = conf.mTagBits;
    long budgetInBits = FormatUtils.parseSpaceSize(conf.mMemoryBudget) * 8;
    int bitsPerClock = conf.mClockBits;
    int bitsPerSize = conf.mSizeBits;
    int bitsPerScope = conf.mScopeBits;
    long bitsPerSlot = bitsPerTag + bitsPerClock + bitsPerSize + bitsPerScope;
    long totalBuckets = budgetInBits / bitsPerSlot / TAGS_PER_BUCKET;
    long numBuckets = Long.highestOneBit(totalBuckets);
    long numBits = numBuckets * TAGS_PER_BUCKET * bitsPerTag;
    BitSet bits = BitSet.createBitSet(conf.mBitSetType, (int) numBits);
    CuckooTable table = new SimpleCuckooTable(bits, (int) numBuckets, TAGS_PER_BUCKET, bitsPerTag);

    BitSet clockBits =
        BitSet.createBitSet(conf.mBitSetType, (int) (numBuckets * TAGS_PER_BUCKET * bitsPerClock));
    CuckooTable clockTable =
        new SimpleCuckooTable(clockBits, (int) numBuckets, TAGS_PER_BUCKET, bitsPerClock);

    BitSet sizeBits =
        BitSet.createBitSet(conf.mBitSetType, (int) (numBuckets * TAGS_PER_BUCKET * bitsPerSize));
    CuckooTable sizeTable =
        new SimpleCuckooTable(sizeBits, (int) numBuckets, TAGS_PER_BUCKET, bitsPerSize);

    // NOTE: scope may be empty
    BitSet scopeBits =
        BitSet.createBitSet(conf.mBitSetType, (int) (numBuckets * TAGS_PER_BUCKET * bitsPerScope));
    CuckooTable scopeTable = (bitsPerScope == 0) ? new EmptyCuckooTable()
        : new SimpleCuckooTable(scopeBits, (int) numBuckets, TAGS_PER_BUCKET, bitsPerScope);

    // sliding window
    SlidingWindowType slidingWindowType = SlidingWindowType.NONE;
    if (conf.mOpportunisticAging) {
      slidingWindowType = conf.mSlidingWindowType;
    }

    // size encoding
    ISizeEncoder sizeEncoder;
    if (conf.mSizeEncodeType == SizeEncodeType.BUCKET) {
      sizeEncoder =
          new SizeEncoder(conf.mNumSizeBucketBits + conf.mSizeBucketBits, conf.mNumSizeBucketBits);
    } else if (conf.mSizeEncodeType == SizeEncodeType.TRUNCATE) {
      sizeEncoder = new TruncateSizeEncoder(conf.mNumSizeBucketBits + conf.mSizeBucketTruncateBits,
          conf.mSizeBucketTruncateBits);
    } else if (conf.mSizeEncodeType == SizeEncodeType.LOG) {
      sizeEncoder =
              new LogSizeEncoder(conf.mNumSizeBucketBits,
                      conf.mSizeBucketFirst, conf.mSizeBucketBase, conf.mSizeBucketBias);
    } else {
      sizeEncoder = new NoOpSizeEncoder(conf.mNumSizeBucketBits + conf.mSizeBucketBits);
    }

    if(MRC_OPEN){
      RDLength = conf.mRDLength;
      RDWidth = conf.mRDWidth;
      RD = new long[(int)RDLength];
    }

    return new SimpleClockCuckooFilter<>(table, clockTable, sizeTable, scopeTable,
        slidingWindowType, conf.mWindowSize, sizeEncoder, funnel, Hashing.murmur3_128());
  }

  /**
   * Create a concurrent cuckoo filter with specified parameters.
   *
   * @param funnel the funnel of T's that the constructed cuckoo filter will use
   * @param expectedInsertions the number of expected insertions to the constructed {@code
   *      ConcurrentClockCuckooFilter}; must be positive
   * @param bitsPerClock the number of bits the clock field has
   * @param bitsPerSize the number of bits the size field has
   * @param bitsPerScope the number of bits the scope field has
   * @param slidingWindowType the type of sliding window
   * @param windowSize the size of the sliding window
   * @param fpp the desired false positive probability (must be positive and less than 1.0)
   * @param loadFactor the load factor of cuckoo filter (must be positive and less than 1.0)
   * @param hasher the hash function to be used
   * @param <T> the type of item
   * @return a {@code ConcurrentClockCuckooFilter}
   */
  public static <T> SimpleClockCuckooFilter<T> create(Funnel<? super T> funnel,
                                                      long expectedInsertions, int bitsPerClock, int bitsPerSize, int bitsPerScope,
                                                      SlidingWindowType slidingWindowType, long windowSize, double fpp, double loadFactor,
                                                      HashFunction hasher) {
    // make expectedInsertions a power of 2
    int bitsPerTag = CuckooUtils.optimalBitsPerTag(fpp, loadFactor);
    long numBuckets = CuckooUtils.optimalBuckets(expectedInsertions, loadFactor, TAGS_PER_BUCKET);
    long numBits = numBuckets * TAGS_PER_BUCKET * bitsPerTag;
    BitSet bits = new SimpleBitSet((int) numBits);
    CuckooTable table = new SimpleCuckooTable(bits, (int) numBuckets, TAGS_PER_BUCKET, bitsPerTag);

    BitSet clockBits = new SimpleBitSet((int) (numBuckets * TAGS_PER_BUCKET * bitsPerClock));
    CuckooTable clockTable =
        new SimpleCuckooTable(clockBits, (int) numBuckets, TAGS_PER_BUCKET, bitsPerClock);

    BitSet sizeBits = new SimpleBitSet((int) (numBuckets * TAGS_PER_BUCKET * bitsPerSize));
    CuckooTable sizeTable =
        new SimpleCuckooTable(sizeBits, (int) numBuckets, TAGS_PER_BUCKET, bitsPerSize);

    // NOTE: scope may be empty
    BitSet scopeBits = new SimpleBitSet((int) (numBuckets * TAGS_PER_BUCKET * bitsPerScope));
    CuckooTable scopeTable = (bitsPerScope == 0) ? new EmptyCuckooTable()
        : new SimpleCuckooTable(scopeBits, (int) numBuckets, TAGS_PER_BUCKET, bitsPerScope);
    return new SimpleClockCuckooFilter<>(table, clockTable, sizeTable, scopeTable,
        slidingWindowType, windowSize, funnel, hasher);
  }

  /**
   * Create a concurrent cuckoo filter with specified parameters.
   *
   * @param funnel the funnel of T's that the constructed {@code BloomFilter} will use
   * @param expectedInsertions the number of expected insertions to the constructed {@code
   *      ConcurrentClockCuckooFilter}; must be positive
   * @param bitsPerClock the number of bits the clock field has
   * @param bitsPerSize the number of bits the size field has
   * @param bitsPerScope the number of bits the scope field has
   * @param slidingWindowType the type of sliding window
   * @param windowSize the size of the sliding window
   * @param fpp the desired false positive probability (must be positive and less than 1.0)
   * @param loadFactor the load factor of cuckoo filter (must be positive and less than 1.0)
   * @param <T> the type of item
   * @return a {@code ConcurrentClockCuckooFilter}
   */
  public static <T> SimpleClockCuckooFilter<T> create(Funnel<? super T> funnel,
                                                      long expectedInsertions, int bitsPerClock, int bitsPerSize, int bitsPerScope,
                                                      SlidingWindowType slidingWindowType, long windowSize, double fpp, double loadFactor) {
    return create(funnel, expectedInsertions, bitsPerClock, bitsPerSize, bitsPerScope,
        slidingWindowType, windowSize, fpp, loadFactor, Hashing.murmur3_128());
  }

  /**
   * Create a concurrent cuckoo filter with specified parameters.
   *
   * @param funnel the funnel of T's that the constructed {@code BloomFilter} will use
   * @param expectedInsertions the number of expected insertions to the constructed {@code
   *      ConcurrentClockCuckooFilter}; must be positive
   * @param bitsPerClock the number of bits the clock field has
   * @param bitsPerSize the number of bits the size field has
   * @param bitsPerScope the number of bits the scope field has
   * @param slidingWindowType the type of sliding window
   * @param windowSize the size of the sliding window
   * @param fpp the desired false positive probability (must be positive and less than 1.0)
   * @param <T> the type of item
   * @return a {@code ConcurrentClockCuckooFilter}
   */
  public static <T> SimpleClockCuckooFilter<T> create(Funnel<? super T> funnel,
                                                      long expectedInsertions, int bitsPerClock, int bitsPerSize, int bitsPerScope,
                                                      SlidingWindowType slidingWindowType, long windowSize, double fpp) {
    return create(funnel, expectedInsertions, bitsPerClock, bitsPerSize, bitsPerScope,
        slidingWindowType, windowSize, fpp, DEFAULT_LOAD_FACTOR);
  }

  /**
   * Create a concurrent cuckoo filter with specified parameters.
   *
   * @param funnel the funnel of T's that the constructed {@code BloomFilter} will use
   * @param expectedInsertions the number of expected insertions to the constructed {@code
   *      ConcurrentClockCuckooFilter}; must be positive
   * @param bitsPerClock the number of bits the clock field has
   * @param bitsPerSize the number of bits the size field has
   * @param bitsPerScope the number of bits the scope field has
   * @param slidingWindowType the type of sliding window
   * @param windowSize the size of the sliding window
   * @param <T> the type of item
   * @return a {@code ConcurrentClockCuckooFilter}
   */
  public static <T> SimpleClockCuckooFilter<T> create(Funnel<? super T> funnel,
                                                      long expectedInsertions, int bitsPerClock, int bitsPerSize, int bitsPerScope,
                                                      SlidingWindowType slidingWindowType, long windowSize) {
    return create(funnel, expectedInsertions, bitsPerClock, bitsPerSize, bitsPerScope,
        slidingWindowType, windowSize, DEFAULT_FPP);
  }

  /**
   * Create a concurrent cuckoo filter with specified parameters.
   *
   * @param funnel the funnel of T's that the constructed {@code BloomFilter} will use
   * @param expectedInsertions the number of expected insertions to the constructed {@code
   *      ConcurrentClockCuckooFilter}; must be positive
   * @param bitsPerClock the number of bits the clock field has
   * @param bitsPerSize the number of bits the size field has
   * @param bitsPerScope the number of bits the scope field has
   * @param <T> the type of item
   * @return a {@code ConcurrentClockCuckooFilter}
   */
  public static <T> SimpleClockCuckooFilter<T> create(Funnel<? super T> funnel,
                                                      long expectedInsertions, int bitsPerClock, int bitsPerSize, int bitsPerScope) {
    Preconditions.checkNotNull(funnel);
    Preconditions.checkArgument(expectedInsertions > 0);
    Preconditions.checkArgument(bitsPerClock > 0);
    Preconditions.checkArgument(bitsPerSize > 0);
    Preconditions.checkArgument(bitsPerScope > 0);
    return create(funnel, expectedInsertions, bitsPerClock, bitsPerSize, bitsPerScope,
        SlidingWindowType.NONE, -1, DEFAULT_FPP);
  }

  @Override
  public boolean put(T item, int size, CacheScope scopeInfo) {
    // NOTE: zero size is not allowed in our clock filter, because we use zero size as
    // a special case to indicate an size overflow (size > mMaxSize), and all the
    // overflowed size will be revised to mMaxSize in method encodeSize()
    if (size <= 0) {
      return false;
    }
    long hv = hashValue(item);
    int tag = tagHash(hv);
    int b1 = indexHash(hv);
    int b2 = altIndex(b1, tag);
    int scope = encodeScope(scopeInfo);
    // size = encodeSize(size);
    // Generally, we will hold write locks in two places:
    // 1) put/delete;
    // 2) cuckooPathSearch & cuckooPathMove.
    // But We only execute opportunistic aging in put/delete.
    // This is because we expect cuckoo path search & move to be as fast as possible,
    // or it may be more possible to fail.
    writeLockAndOpportunisticAging(b1, b2);
    TagPosition pos = cuckooInsertLoop(b1, b2, tag);
    if (pos.mStatus == CuckooStatus.OK) {
      // b1 and b2 should be insertable for fp, which means:
      // 1. b1 or b2 have at least one empty slot (this is guaranteed until we unlock two buckets);
      // 2. b1 and b2 do not contain duplicated fingerprint.
      mTable.writeTag(pos.getBucketIndex(), pos.getSlotIndex(), tag);
      mClockTable.set(pos.getBucketIndex(), pos.getSlotIndex());
      // update clock dist.
      clockDist[mMaxAge] = clockDist[mMaxAge] + size;
      mScopeTable.writeTag(pos.getBucketIndex(), pos.getSlotIndex(), scope);
      mSizeEncoder.add(size);
      int encodedSize = mSizeEncoder.encode(size);
      mSizeTable.writeTag(pos.getBucketIndex(), pos.getSlotIndex(), encodedSize);
      // update statistics
      //updateScopeStatistics(scope, 1, encodedSize);
      updateScopeStatistics(scope, 1, size);
      return true;
    }
    return false;
  }

  @Override
  public boolean mightContainAndResetClock(T item) {
    return mightContainAndOptionalResetClock(item, true);
  }

  @Override
  public boolean mightContain(T item) {
    return mightContainAndOptionalResetClock(item, false);
  }

  /**
   * @param item the item to be checked
   * @param shouldReset the flag to indicate whether to reset clock field
   * @return true if item is in cuckoo filter; false otherwise
   */
  private boolean mightContainAndOptionalResetClock(T item, boolean shouldReset) {
    long hv = hashValue(item);
    int tag = tagHash(hv);
    int b1 = indexHash(hv);
    int b2 = altIndex(b1, tag);
    TagPosition pos = mTable.findTag(b1, b2, tag);
    mTotalNum.incrementAndGet();
    boolean found = pos.getStatus() == CuckooStatus.OK;
    if (found && shouldReset) {
      // set C to MAX
      int oldClock =  mClockTable.readTag(pos.getBucketIndex(),pos.getSlotIndex());
      int encoded_size = mSizeTable.readTag(pos.getBucketIndex(), pos.getSlotIndex());

      if(MRC_OPEN){
        updateMaxMrcSize(oldClock);
        int decoded_size = mSizeEncoder.dec(encoded_size);
        mSizeEncoder.add(decoded_size);
        clockDist[oldClock] = clockDist[oldClock] - decoded_size;
        clockDist[mMaxAge] = clockDist[mMaxAge] + decoded_size;
      }
      mClockTable.set(pos.getBucketIndex(), pos.getSlotIndex());
    }
    return found;
  }

  @Override
  public boolean delete(T item) {
    long hv = hashValue(item);
    int tag = tagHash(hv);
    int b1 = indexHash(hv);
    int b2 = altIndex(b1, tag);
    writeLockAndOpportunisticAging(b1, b2);
    TagPosition pos = mTable.deleteTag(b1, tag);
    if (pos.getStatus() != CuckooStatus.OK) {
      pos = mTable.deleteTag(b2, tag);
    }
    if (pos.getStatus() == CuckooStatus.OK) {
      int scope = mScopeTable.readTag(pos.getBucketIndex(), pos.getSlotIndex());
      int encodedSize = mSizeTable.readTag(pos.getBucketIndex(), pos.getSlotIndex());
      updateScopeStatistics(scope, -1, -mSizeEncoder.dec(encodedSize));
      // Clear Clock
      mClockTable.clear(pos.getBucketIndex(), pos.getSlotIndex());
      return true;
    }
    return false;
  }

  @Override
  public void aging() {
    for (int i = 0; i < mNumBuckets; i++) {
      // TODO(iluoeli): avoid acquire locks here since it may be blocked
      // for a long time if this segment is contended by multiple users.
      agingBucket(i);
    }
  }

  /**
   * Get the item's clock value (age).
   *
   * @param item the item to be queried
   * @return the clock value
   */
  public int getAge(T item) {
    long hv = hashValue(item);
    int tag = tagHash(hv);
    int b1 = indexHash(hv);
    int b2 = altIndex(b1, tag);
    TagPosition pos = mTable.findTag(b1, b2, tag);
    if (pos.getStatus() == CuckooStatus.OK) {
      int clock = mClockTable.readTag(pos.getBucketIndex(), pos.getSlotIndex());
      return clock;
    }
    return 0;
  }

  /**
   * @return the summary of this cuckoo filter
   */
  public String getSummary() {
    return "numBuckets: " + getNumBuckets() + "\ntagsPerBucket: " + getTagsPerBucket()
        + "\nbitsPerTag: " + getBitsPerTag() + "\nbitsPerClock: " + getBitsPerClock()
        + "\nbitsPerSize: " + mBitsPerSize + "\nbitsPerScope: " + mBitsPerScope + "\nSizeInMB: "
        + (getNumBuckets() * getTagsPerBucket() * getBitsPerTag() / 8.0 / Constants.MB
            + getNumBuckets() * getTagsPerBucket() * getBitsPerClock() / 8.0 / Constants.MB
            + getNumBuckets() * getTagsPerBucket() * mBitsPerSize / 8.0 / Constants.MB
            + getNumBuckets() * getTagsPerBucket() * mBitsPerScope / 8.0 / Constants.MB);
  }

  @Override
  public double expectedFpp() {
    // equation from "Cuckoo Filter: Simplification and Analysis" by David Eppstein (Theorem 5.1)
    return 0;
  }

  @Override
  public long approximateElementCount() {
    return mSizeEncoder.getTotalCount();
  }

  @Override
  public long approximateElementCount(CacheScope scopeInfo) {
    return 0;
  }

  @Override
  public long approximateElementSize() {
    //return mSizeEncoder.getTotalSize();
    return 0;
  }

  @Override
  public long approximateElementSize(CacheScope scopeInfo) {

    return 0;
  }

  /**
   * By calling this method, cuckoo filter is informed of the number of entries have passed.
   *
   * @param count the number of operations have passed
   */
  public void increaseOperationCount(int count) {
    //no - op
  }

  /**
   * @return the number of buckets this cuckoo filter has
   */
  public int getNumBuckets() {
    return mTable.getNumBuckets();
  }

  /**
   * @return the number of slots per bucket has
   */
  public int getTagsPerBucket() {
    return mTable.getNumTagsPerBuckets();
  }

  /**
   * @return the number of bits per slot has
   */
  public int getBitsPerTag() {
    return mTable.getBitsPerTag();
  }

  /**
   * @return the number of bits per slot's clock field has
   */
  public int getBitsPerClock() {
    return mClockTable.getBitsPerTag();
  }

  /**
   * @return the number of bits per slot's size field has
   */
  public int getBitsPerSize() {
    return mSizeTable.getBitsPerTag();
  }

  /**
   * @return the number of bits per slot's scope field has
   */
  public int getBitsPerScope() {
    return mScopeTable.getBitsPerTag();
  }

  @Override
  public String dumpDebugInfo() {
    return mSizeEncoder.dumpInfo();
  }

  /**
   * @param item the object to be hashed
   * @return the hash code of this item
   */
  protected long hashValue(T item) {
    return mHashFunction.newHasher().putObject(item, mFunnel).hash().asLong();
  }

  /**
   * Compute the index from a hash value.
   *
   * @param hv the hash value used for computing
   * @return the bucket computed on given hash value
   */
  protected int indexHash(long hv) {
    return CuckooUtils.indexHash((int) (hv >> 32), mNumBuckets);
  }

  /**
   * Compute the tag from a hash value.
   *
   * @param hv the hash value used for computing
   * @return the fingerprint computed on given hash value
   */
  protected int tagHash(long hv) {
    return CuckooUtils.tagHash((int) hv, mBitsPerTag);
  }

  /**
   * Compute the alternative index from index and tag.
   *
   * @param index the bucket for computing
   * @param tag the fingerprint for computing
   * @return the alternative bucket
   */
  protected int altIndex(int index, int tag) {
    return CuckooUtils.altIndex(index, tag, mNumBuckets);
  }

  /**
   * Encode a scope information into a integer type (storage type). We only store table-level or
   * higher level scope information, so file-level (partition-level) scope will not be stored.
   *
   * @param scopeInfo the scope to be encoded
   * @return the encoded number of scope
   */
  protected int encodeScope(CacheScope scopeInfo) {
    if (scopeInfo.level() == CacheScope.Level.PARTITION) {
      return mScopeEncoder.encode(scopeInfo.parent());
    }
    return mScopeEncoder.encode(scopeInfo);
  }

  /**
   * Encode the original size to internal storage types, used for overflow handling etc.
   *
   * @param size the size to be encoded
   * @return the storage type of the encoded size
   */
  private int encodeSize(int size) {
    return Math.min(mMaxSize, size);
  }

  /**
   * Decode the storage type of size to original integer.
   *
   * @param size the storage type of size to be decoded
   * @return the decoded size of the storage type
   */
  private int decodeSize(int size) {
    if (size == 0) {
      size = mMaxSize;
    }
    return size;
  }

  /**
   * A thread-safe method to update scope statistics.
   *
   * @param scope the scope be be updated
   * @param number the number of items this scope have changed
   * @param size the size of this scope have changed
   */
  private void updateScopeStatistics(int scope, int number, int size) {
   // no -op
  }

  /**
   * Try find an empty slot for the item. Assume already held the lock of buckets i1 and i2.
   *
   * @param b1 the first bucket
   * @param b2 the second bucket
   * @param fp the fingerprint
   * @return a valid tag position pointing to the empty slot; otherwise an invalid tat position
   *         indicating failure
   */
  protected TagPosition cuckooInsertLoop(int b1, int b2, int fp) {
    int maxRetryNum = 1;
    while (maxRetryNum-- > 0) {
      TagPosition pos = cuckooInsert(b1, b2, fp);
      if (pos.getStatus() == CuckooStatus.OK) {
        return pos;
      }
    }
    return new TagPosition(-1, -1, CuckooStatus.FAILURE);
  }

  /**
   * Try find an empty slot for the item. Assume already held the lock of buckets i1 and i2.
   *
   * @param b1 the first bucket
   * @param b2 the second bucket
   * @param fp the fingerprint
   * @return a valid tag position pointing to the empty slot; otherwise an invalid tat position
   *         indicating failure
   */
  private TagPosition cuckooInsert(int b1, int b2, int fp) {
    // try find b1 and b2 firstly
    TagPosition pos1 = tryFindInsertBucket(b1, fp);
    if (pos1.getStatus() == CuckooStatus.FAILURE_KEY_DUPLICATED) {
      return pos1;
    }
    TagPosition pos2 = tryFindInsertBucket(b2, fp);
    if (pos2.getStatus() == CuckooStatus.FAILURE_KEY_DUPLICATED) {
      return pos2;
    }
    if (pos1.getStatus() == CuckooStatus.OK) {
      return pos1;
    }
    if (pos2.getStatus() == CuckooStatus.OK) {
      return pos2;
    }
    // then BFS search from b1 and b2
    TagPosition pos = runCuckoo(b1, b2, fp);
    if (pos.getStatus() == CuckooStatus.OK) {
      // avoid another duplicated key is inserted during runCuckoo.
      if (mTable.findTag(b1, b2, fp).getStatus() == CuckooStatus.OK) {
        pos.setStatus(CuckooStatus.FAILURE_KEY_DUPLICATED);
      }
    }
    return pos;
  }

  /**
   * Try find an empty slot for the item. Assume already held the lock of buckets i1 and i2.
   *
   * @param b1 the first bucket
   * @param b2 the second bucket
   * @param fp the fingerprint
   * @return a valid tag position pointing to the empty slot; otherwise an invalid tat position
   *         indicating failure
   */
  private TagPosition runCuckoo(int b1, int b2, int fp) {
    TagPosition pos = new TagPosition(-1, -1, CuckooStatus.FAILURE);
    int maxPathLen = MAX_BFS_PATH_LEN;
    CuckooRecord[] cuckooPath = new CuckooRecord[maxPathLen];
    for (int i = 0; i < maxPathLen; i++) {
      cuckooPath[i] = new CuckooRecord();
    }
    boolean done = false;
    while (!done) {
      int depth = cuckooPathSearch(b1, b2, fp, cuckooPath);
      if (depth < 0) {
        break;
      }
      if (cuckooPathMove(b1, b2, fp, cuckooPath, depth)) {
        pos.setBucketAndSlot(cuckooPath[0].mBucketIndex, cuckooPath[0].mSlotIndex);
        pos.setStatus(CuckooStatus.OK);
        done = true;
      }
    }
    if (!done) {
      // NOTE: since we assume holding the locks of two buckets before calling this method,
      // we keep this assumptions after return.
    }
    return pos;
  }

  /**
   * Search for an empty slot from two initial buckets b1 and b2, and move items backwards along the
   * path.
   *
   * @param b1 the first bucket
   * @param b2 the second bucket
   * @param fp the fingerprint
   * @param cuckooPath the information of searched path
   * @return the depth the path
   */
  private int cuckooPathSearch(int b1, int b2, int fp, CuckooRecord[] cuckooPath) {
    // 1. search a path
    BFSEntry x = slotBFSSearch(b1, b2, fp);
    if (x.mDepth == -1) {
      return -1;
    }
    // 2. re-construct path from x
    for (int i = x.mDepth; i >= 0; i--) {
      cuckooPath[i].mSlotIndex = x.mPathcode % TAGS_PER_BUCKET;
      x.mPathcode /= TAGS_PER_BUCKET;
    }
    if (x.mPathcode == 0) {
      cuckooPath[0].mBucketIndex = b1;
    } else {
      cuckooPath[0].mBucketIndex = b2;
    }
    {
      int tag = mTable.readTag(cuckooPath[0].mBucketIndex, cuckooPath[0].mSlotIndex);
      if (tag == 0) {
        return 0;
      }
      cuckooPath[0].mFingerprint = tag;
    }
    for (int i = 1; i <= x.mDepth; i++) {
      CuckooRecord curr = cuckooPath[i];
      CuckooRecord prev = cuckooPath[i - 1];
      curr.mBucketIndex = altIndex(prev.mBucketIndex, prev.mFingerprint);
      int tag = mTable.readTag(curr.mBucketIndex, curr.mSlotIndex);
      if (tag == 0) {
        return i;
      }
      curr.mFingerprint = tag;
    }
    return x.mDepth;
  }

  /**
   * Search an empty slot from two initial buckets.
   *
   * @param b1 the first bucket
   * @param b2 the second bucket
   * @param fp the fingerprint
   * @return the last entry of searched path
   */
  private BFSEntry slotBFSSearch(int b1, int b2, int fp) {
    Queue<BFSEntry> queue = new LinkedList<>();
    queue.offer(new BFSEntry(b1, 0, 0));
    queue.offer(new BFSEntry(b2, 1, 0));
    while (!queue.isEmpty()) {
      BFSEntry x = queue.poll();
      // pick a random slot to start on
      int startingSlot = x.mPathcode % TAGS_PER_BUCKET;
      for (int i = 0; i < TAGS_PER_BUCKET; i++) {
        int slot = (startingSlot + i) % TAGS_PER_BUCKET;
        int tag = mTable.readTag(x.mBucketIndex, slot);
        if (tag == 0) {
          x.mPathcode = x.mPathcode * TAGS_PER_BUCKET + slot;
          return x;
        }
        if (x.mDepth < MAX_BFS_PATH_LEN - 1) {
          queue.offer(new BFSEntry(altIndex(x.mBucketIndex, tag),
              x.mPathcode * TAGS_PER_BUCKET + slot, x.mDepth + 1));
        }
      }
    }
    return new BFSEntry(0, 0, -1);
  }

  /**
   * Move items backward along the cuckoo path.
   *
   * @param b1 the first bucket
   * @param b2 the second bucket
   * @param fp the fingerprint
   * @param cuckooPath the path to move along
   * @param depth the depth of the path
   * @return true if successfully moved items along the path; false otherwise
   */
  private boolean cuckooPathMove(int b1, int b2, int fp, CuckooRecord[] cuckooPath, int depth) {
    if (depth == 0) {
      if (mTable.readTag(cuckooPath[0].mBucketIndex, cuckooPath[0].mSlotIndex) == 0) {
        return true;
      } else {
        return false;
      }
    }

    while (depth > 0) {
      CuckooRecord from = cuckooPath[depth - 1];
      CuckooRecord to = cuckooPath[depth];
      int fromTag = mTable.readTag(from.mBucketIndex, from.mSlotIndex);
      // if `to` is nonempty, or `from` is not occupied by original tag,
      // in both cases, abort this insertion.
      if (mTable.readTag(to.mBucketIndex, to.mSlotIndex) != 0 || fromTag != from.mFingerprint) {
        return false;
      }
      int clock = mClockTable.readTag(from.mBucketIndex, from.mSlotIndex);
      mTable.writeTag(to.mBucketIndex, to.mSlotIndex, fromTag);
      mClockTable.writeTag(to.mBucketIndex, to.mSlotIndex, clock);
      mScopeTable.writeTag(to.mBucketIndex, to.mSlotIndex,
          mScopeTable.readTag(from.mBucketIndex, from.mSlotIndex));
      mSizeTable.writeTag(to.mBucketIndex, to.mSlotIndex,
          mSizeTable.readTag(from.mBucketIndex, from.mSlotIndex));
      mTable.clear(from.mBucketIndex, from.mSlotIndex);
      depth--;
    }
    return true;
  }

  /**
   * Find tag `fp` in bucket `i`.
   *
   * @param i the bucket index
   * @param fp the fingerprint
   * @return true if no duplicated key is found, and `pos.slot` points to an empty slot (if pos.tag
   *         != -1); otherwise return false, and store the position of duplicated key in `pos.slot`.
   */
  private TagPosition tryFindInsertBucket(int i, int fp) {
    TagPosition pos = new TagPosition(i, -1, CuckooStatus.FAILURE_TABLE_FULL);
    for (int slotIndex = 0; slotIndex < TAGS_PER_BUCKET; slotIndex++) {
      int tag = mTable.readTag(i, slotIndex);
      if (tag != 0) {
        if (tag == fp) {
          pos.setSlotIndex(slotIndex);
          pos.setStatus(CuckooStatus.FAILURE_KEY_DUPLICATED);
          return pos;
        }
      } else {
        pos.setSlotIndex(slotIndex);
        pos.setStatus(CuckooStatus.OK);
      }
    }
    return pos;
  }

  /**
   * Lock two buckets and try opportunistic aging. Since we hold write locks, we can assure that
   * there are no other threads aging the same segment.
   *
   * @param b1 the first bucket
   * @param b2 the second bucket
   */
  protected void writeLockAndOpportunisticAging(int b1, int b2) {
    //no - op in simple cuckoo
  }

  /**
   * Try opportunistic aging ith segment. Assume holding the lock of this segment.
   *
   * @param i the index of the segment to be aged
   */
  private void opportunisticAgingSegment(int i) {
    //no - op  in simple cuckoo
  }

  /**
   * @Return the number of buckets should be aged.
   */
  private int computeAgingNumber() {
    return 0;
  }

  /**
   * Aging the ith segment at most `maxAgingNumber` buckets. Assume holding the lock of this
   * segment.
   *
   * @param i the index of the segment to be aged
   * @param maxAgingNumber the maximum number of buckets to be aged
   */
  private int agingSegment(int i, int maxAgingNumber) {
    //no - op
    return 0;
  }

  /**
   * Aging buckets in range [from, to]. Assume holding the locks of this range.
   *
   * @param from the start bucket of the range to be aged
   * @param to the end bucket of the range to be aged
   * @return the number of cleaned buckets
   */
  private int agingRange(int from, int to) {
    int numCleaned = 0;
    for (int i = from; i < to; i++) {
      numCleaned += agingBucket(i);
    }
    return numCleaned;
  }

  /**
   * @param b the bucket to be aged
   * @return the number of cleaned slots
   */
  private int agingBucket(int b) {
    int numCleaned = 0;
    for (int slotIndex = 0; slotIndex < TAGS_PER_BUCKET; slotIndex++) {
      int tag = mTable.readTag(b, slotIndex);
      if (tag == 0) {
        continue;
      }
      int oldClock = mClockTable.readTag(b, slotIndex);
      if (oldClock > 0) {
        mClockTable.writeTag(b, slotIndex, oldClock - 1);
        int encodedSize = mSizeTable.readTag(b, slotIndex);
        int size = mSizeEncoder.dec(encodedSize);
        mSizeEncoder.add(size);
        clockDist[oldClock] = clockDist[oldClock] - size;
        clockDist[oldClock - 1] = clockDist[oldClock-1] + size;
      } else {
        // evict stale item
        numCleaned++;
        mTable.clear(b, slotIndex);
        int scope = mScopeTable.readTag(b, slotIndex);
        int encodedSize = mSizeTable.readTag(b, slotIndex);
        // encodedSize = decodeSize(encodedSize);
        int size = mSizeEncoder.dec(encodedSize);
        updateScopeStatistics(scope, -1, -size);
        clockDist[0] -= size;
      }
    }
    return numCleaned;
  }

  private void updateMaxMrcSize(int oldClock){
    long sumSize = clockDist[oldClock] / 2;
    for(int i  = oldClock+1; i<=mMaxAge; i++){
      sumSize += clockDist[i];
    }
    int stackDistance = (int)Math.round((double)sumSize / RDWidth);
    if(stackDistance >= RDLength){
       stackDistance = (int)RDLength-1;
       System.out.println("[CCF] too larger distance");
    }
    RD[stackDistance] ++;
  }

  public double[] getMRC(){
    double[] mrc = new double[(int)RDLength];
    long hit_ops = 0;
    for(int i = 0; i<RDLength; i++){
      hit_ops += RD[i];
      mrc[i] = (double)hit_ops / mTotalNum.get();
    }
    return mrc;
  }

  public String getMRCSummary() {
    long hit_ops = 0;
    for(int i = 0; i<RDLength; i++){
      hit_ops += RD[i];
    }
    return String.format("hit_ops: %d, total_ops: %d", hit_ops, mTotalNum.get());
  }

  /**
   * A basic entry that records information of a path node during cuckoo BFS search.
   */
  static final class BFSEntry {
    public int mBucketIndex;
    public int mPathcode; // encode slot position of ancestors and it own nodes
    public int mDepth;

    /**
     * @param bucketIndex the bucket of entry
     * @param pathcode the encoded slot position of ancestors and it own
     * @param depth the depth of the entry in path
     */
    BFSEntry(int bucketIndex, int pathcode, int depth) {
      mBucketIndex = bucketIndex;
      mPathcode = pathcode;
      mDepth = depth;
    }
  }

  /**
   * This class represents a detailed cuckoo record, include its position and stored value.
   */
  static final class CuckooRecord {
    public int mBucketIndex;
    public int mSlotIndex;
    public int mFingerprint;

    CuckooRecord() {
      this(-1, -1, 0);
    }

    /**
     * @param bucketIndex the bucket of this record
     * @param slotIndex the slot of this record
     * @param fingerprint the fingerprint of this record
     */
    CuckooRecord(int bucketIndex, int slotIndex, int fingerprint) {
      mBucketIndex = bucketIndex;
      mSlotIndex = slotIndex;
      mFingerprint = fingerprint;
    }
  }
}
