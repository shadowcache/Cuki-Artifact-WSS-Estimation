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

package alluxio.client.file.cache;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import alluxio.Constants;
import alluxio.client.file.cache.cuckoofilter.SlidingWindowType;
import alluxio.client.quota.CacheScope;
import alluxio.util.FormatUtils;
import sun.rmi.runtime.Log;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.awt.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RARCMCacheManager implements ShadowCache{

  protected int mNumBuckets;
  protected long []timestampTable;
  protected long []sizeTable;
  protected Funnel<PageId> mFunnel;
  protected HashFunction mHashFunction;

  private final AtomicLong mTotalNum = new AtomicLong(0);
  private final AtomicLong mReAccessNum = new AtomicLong(0);
  private final AtomicLong mTotalSize = new AtomicLong(0);
  private final AtomicLong mReAccessSize = new AtomicLong(0);
  private final AtomicLong mReAccessUniqueSize = new AtomicLong(0);

  private final static int mBitsPerSize = 64;
  private final static int mBitsPerTimestamp = 64;

  private final ScheduledExecutorService mScheduler = Executors.newScheduledThreadPool(0);
  private long[] RD;
  private int maxRDLength = 0;
  private int RDLength;
  private int RDWidth;

  private double []RAR;
  private long startTime = 0;
  private int agingCount = 1;
  private SlidingWindowType mSlidingWindowType;

  private final Lock lock = new ReentrantLock();

  public RARCMCacheManager(ShadowCacheParameters parameters){
    mFunnel = PageIdFunnel.FUNNEL;
    mHashFunction = Hashing.murmur3_128();
    long memoryInBits = FormatUtils.parseSpaceSize(parameters.mMemoryBudget) * 8;
    int numBuckets = (int) (memoryInBits / (mBitsPerSize+mBitsPerTimestamp));
    RAR = new double[(int)parameters.mWindowSize+1];
    mNumBuckets = numBuckets;
    mSlidingWindowType = parameters.mSlidingWindowType;
    timestampTable = new long[numBuckets];
    sizeTable = new long[numBuckets];
    RDWidth = parameters.mRDWidth;
    RDLength = parameters.mRDLength;
    RD = new long[RDLength];
    if(mSlidingWindowType==SlidingWindowType.TIME_BASED){
      startTime = System.currentTimeMillis();
      mScheduler.scheduleAtFixedRate(this::aging, 1, 1, MILLISECONDS);
    }
  }

  /**
   * Puts a page with specified size and scope into the shadow cache manager.
   *
   * @param pageId page identifier
   * @param size   page size
   * @param scope  cache scope
   * @return true if the put was successful, false otherwise
   */
  @Override
  public boolean put(PageId pageId, int size, CacheScope scope) {
    System.out.println("no op");
    return false;
  }

  /**
   * Reads the entire page and refresh its access time if the queried page is found in the cache.
   *
   * @param pageId      page identifier
   * @param bytesToRead number of bytes to read in this page
   * @param scope       cache scope
   * @return the number of bytes read, 0 if page is not found, -1 on errors
   */
  @Override
  public int get(PageId pageId, int bytesToRead, CacheScope scope) {
    int pos = bucketIndex(pageId, mHashFunction);
    long currentTime;
    lock.lock();
    mTotalSize.addAndGet(bytesToRead);
    mTotalNum.incrementAndGet();
    if(mSlidingWindowType==SlidingWindowType.COUNT_BASED){
      currentTime = mTotalNum.get();
    }else{
      currentTime = System.currentTimeMillis();
    }
    if(timestampTable[pos]!=0) {
      mReAccessNum.incrementAndGet();
      mReAccessSize.addAndGet(bytesToRead);
      long sizeInterval = mTotalSize.get() - sizeTable[pos];
      long timeInterval = currentTime - timestampTable[pos];
      int distance = getStackDistance(timeInterval, sizeInterval);
      if(distance >= RDLength){
        System.out.println("[RARCM]: distance is too large");
        distance = RDLength-1;
      }
      RD[distance]++;
      if(distance > maxRDLength){
        maxRDLength = distance;
      }
    }
    timestampTable[pos] = currentTime;
    sizeTable[pos] = mTotalSize.get();
    lock.unlock();
    return bytesToRead;
  }

  /**
   * Deletes a page from the cache.
   *
   * @param pageId page identifier
   * @return true if the page is successfully deleted, false otherwise
   */
  @Override
  public boolean delete(PageId pageId) {
    return false;
  }

  /**
   * Aging all the pages stored in this shadow cache. Specifically, aging operation removes all the
   * stale pages which are not accessed for more than a sliding window.
   */
  @Override
  public void aging() {
    RAR[agingCount++] = (double) mReAccessSize.get()/mTotalSize.get();
  }

  /**
   * Update working set size in number of pages and bytes. Suggest calling this method before
   * getting the number of pages or bytes.
   */
  @Override
  public void updateWorkingSetSize() {
    //no-op;
  }

  /**
   * Stop the background aging task.
   */
  @Override
  public void stopUpdate() {
    //no-op
  }

  /**
   * @param increment the incremental value to apply to timestamp
   */
  @Override
  public void updateTimestamp(long increment) {
    //no-op
  }

  /**
   * @return the number of pages in this shadow cache
   */
  @Override
  public long getShadowCachePages() {
    return mTotalNum.get() - mReAccessNum.get();
  }

  /**
   * @param scope cache scope
   * @return the number of pages of given cache scope in this shadow cache
   */
  @Override
  public long getShadowCachePages(CacheScope scope) {
    return mTotalNum.get() - mReAccessNum.get();
  }

  /**
   * @return the number of bytes in this shadow cache
   */
  @Override
  public long getShadowCacheBytes() {
    return (maxRDLength + 1) * RDWidth;
  }

  /**
   * @param scope cache scope
   * @return the number of bytes of given cache scope in this shadow cache
   */
  @Override
  public long getShadowCacheBytes(CacheScope scope) {
    return 0;
  }

  /**
   * @return the number of pages read in this shadow cache
   */
  @Override
  public long getShadowCachePageRead() {
    return 0;
  }

  /**
   * @return the number of pages hit in this shadow cache
   */
  @Override
  public long getShadowCachePageHit() {
    return 0;
  }

  /**
   * @return the number of bytes read in this shadow cache
   */
  @Override
  public long getShadowCacheByteRead() {
    return 0;
  }

  /**
   * @return the number of bytes hit in this shadow cache
   */
  @Override
  public long getShadowCacheByteHit() {
    return 0;
  }

  /**
   * @return the false positive ratio
   */
  @Override
  public double getFalsePositiveRatio() {
    return 0;
  }

  /**
   * @return the space overhead in terms of bytes
   */
  @Override
  public long getSpaceBits() {
    return 0;
  }



  /**
   * @return the summary of this shadow cache
   */
  @Override
  public String getSummary() {
    return "sizeBits: " + mBitsPerSize + ", timestampBits: " + mBitsPerTimestamp
        + ", BucketsNum: " + mNumBuckets
        + ", mMemoryInBits: " + (mBitsPerSize+mBitsPerTimestamp)*mNumBuckets/8/Constants.MB + "MB"
        + "\nitemReAccessRatio: " + (double)mReAccessNum.get()/mTotalNum.get()
        + ", byteReAccessRatio: " + (double)mReAccessSize.get()/mTotalSize.get();
  }

  private int bucketIndex(PageId pageId, HashFunction hashFunc) {
    return Math.abs(hashFunc.newHasher().putObject(pageId, mFunnel).hash().asInt() % mNumBuckets);
  }

  public double[] getMRC(){
    double[] mrc = new double[RDLength];
    int hit_ops = 0;
    for(int i=0;i<RDLength;i++){
      hit_ops+=RD[i];
      mrc[i] = (double)hit_ops/mTotalNum.get();
    }
    return mrc;
  }

  private int getStackDistance(long timeInterval, long sizeInterval){
    double reAccessRatio = RAR[(int)timeInterval];

    for(int i=(int)timeInterval; i<agingCount; i++){
      if(RAR[i]!=0){
        reAccessRatio = RAR[i];
        break;
      }
    }

    if(reAccessRatio==0){
      System.out.println("could not find a valid reAccessRatio in rar table");
      reAccessRatio =  (double)mReAccessSize.get() / mTotalSize.get();
    }
    double uniqueRatio = (1 - reAccessRatio);
    double stackDistance = (sizeInterval * uniqueRatio) / RDWidth;
    return (int)Math.round(stackDistance);
  }
}