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

package alluxio.client.file.cache.benchmark;

import alluxio.client.file.cache.IdealMRCShadowCacheManager;
import alluxio.client.file.cache.IdealMRCShadowCacheManager2;
import alluxio.client.file.cache.IdealShadowCacheManager;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.ShadowCache;
import alluxio.client.file.cache.dataset.Dataset;
import alluxio.client.file.cache.dataset.DatasetEntry;
import alluxio.client.file.cache.dataset.GeneralDataset;
import alluxio.client.file.cache.dataset.generator.EntryGenerator;

import java.util.Arrays;

public class MRCAccuracyBenchmark implements Benchmark {
  private final BenchmarkContext mBenchmarkContext;
  private final BenchmarkParameters mBenchmarkParameters;
  private final ShadowCache mShadowCache;
  private final ShadowCache mIdealShadowCache;
  private Dataset<String> mDataset;

  public MRCAccuracyBenchmark(BenchmarkContext benchmarkContext,
                              BenchmarkParameters benchmarkParameters) {
    mBenchmarkContext = benchmarkContext;
    mBenchmarkParameters = benchmarkParameters;
    mShadowCache = ShadowCache.create(benchmarkParameters);
    mIdealShadowCache = new IdealMRCShadowCacheManager2(benchmarkParameters);
    createDataset();
    mShadowCache.stopUpdate();
  }

  private void createDataset() {
    EntryGenerator<String> generator = BenchmarkUtils.createGenerator(mBenchmarkParameters);
    mDataset = new GeneralDataset<>(generator, (int) mBenchmarkParameters.mWindowSize);
  }

  @Override
  public boolean prepare() {
    return false;
  }

  @Override
  public void run() {
    long opsCount = 0;
    long agingCount = 0;
    long agingDuration = 0;
    long cacheDuration = 0;
    double numARE = 0.0;
    double byteARE = 0.0;
    double pageHitARE = 0.0;
    double byteHitARE = 0.0;
    long errCnt = 0;
    long numFP = 0; // number of pages are seen as existent in cache but in fact not
    long numFN = 0; // number of pages are seen as inexistent in cache but in fact existed
    long byteFP = 0; // number of bytes are seen as existent in cache but in fact not
    long byteFN = 0; // number of bytes are seen as inexistent in cache but in fact existed
    long totalBytes = 0; // number of bytes passed the shadow cache
    long agingPeriod = mBenchmarkParameters.mWindowSize / mBenchmarkParameters.mAgeLevels;
    if (agingPeriod <= 0) {
      agingPeriod = 1;
    }
    System.out.printf("agingPeriod:%d\n", agingPeriod);

    System.out.println(mShadowCache.getSummary());
    mBenchmarkContext.mStream.println(
        "#operation\tMAE");

    long startTick = System.currentTimeMillis();
    while (mDataset.hasNext() && opsCount < mBenchmarkParameters.mMaxEntries) {
      opsCount++;
      DatasetEntry<String> entry = mDataset.next();

      PageId item = new PageId(entry.getScopeInfo().toString(), entry.getItem().hashCode());
      long startCacheTick = System.currentTimeMillis();
      int nread = mShadowCache.get(item, entry.getSize(), entry.getScopeInfo());
      if (nread <= 0) {
        mShadowCache.put(item, entry.getSize(), entry.getScopeInfo());
      }
      mShadowCache.updateTimestamp(1);
      cacheDuration += (System.currentTimeMillis() - startCacheTick);

      // Aging
      if (opsCount % agingPeriod == 0) {
        agingCount++;
        long startAgingTick = System.currentTimeMillis();
        mShadowCache.aging();
        agingDuration += System.currentTimeMillis() - startAgingTick;
      }

      // update ideal cache
      int nread2 = mIdealShadowCache.get(item, entry.getSize(), entry.getScopeInfo());
      if (nread2 <= 0) {
        mIdealShadowCache.put(item, entry.getSize(), entry.getScopeInfo());
      }
      mIdealShadowCache.updateTimestamp(1);
      mIdealShadowCache.aging();

      // membership statistics
      if (nread > nread2) {
        // false positive: hit in shadow cache (nread > 0), but miss in real cache (nread2 == 0)
        numFP++;
        byteFP += entry.getSize();
      } else if (nread < nread2) {
        // false negative: miss in shadow cache (nread == 0), but hit in real cache (nread2 > 0)
        numFN++;
        byteFN += entry.getSize();
      }
      totalBytes += entry.getSize();

      // report
      if (opsCount % mBenchmarkParameters.mReportInterval == 0) {
        mIdealShadowCache.updateWorkingSetSize();
        mShadowCache.updateWorkingSetSize();
        double[] realMRC = mIdealShadowCache.getMRC();
        double[] estMRC = mShadowCache.getMRC();
        double MAE = 0.0;
        for(int i=0;i<realMRC.length;i++) {
          MAE += Math.abs(realMRC[i] - estMRC[i]);
        }
        MAE /= realMRC.length;
        mBenchmarkContext.mStream.printf("%d\t%.4f%%\n", opsCount, MAE*100);
      }
    }

    double[] realMRC = mIdealShadowCache.getMRC();
    double[] estMRC = mShadowCache.getMRC();
    double MAE = 0.0;
    for(int i=0;i<realMRC.length;i++) {
      MAE += Math.abs(realMRC[i] - estMRC[i]);
    }
    MAE /= realMRC.length;
    mBenchmarkContext.mStream.printf("%d\t%.4f%%\n", opsCount, MAE*100);
    //System.out.println("realMRC:\n" + Arrays.toString(realMRC));
    //System.out.println("estMRC:\n" + Arrays.toString(estMRC));
    System.out.println("real maxium hit ratio: "+ realMRC[realMRC.length-1]);
    System.out.println("est maxium hit ratio: "+ estMRC[estMRC.length-1]);
  }

  @Override
  public boolean finish() {
    return false;
  }
}
