/**
 * lru_k_replacer_test.cpp
 */

#include "buffer/lru_k_replacer.h"

#include <algorithm>
#include <cstdio>
#include <memory>
#include <random>
#include <set>
#include <thread>  // NOLINT
#include <vector>

#include "gtest/gtest.h"

namespace bustub {

TEST(LRUKReplacerTest, SampleTest1) {
  LRUKReplacer lru_replacer(7, 2);

  // Scenario: add six elements to the replacer. We have [1,2,3,4,5]. Frame 6 is non-evictable.
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(6);

  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(6);

  lru_replacer.SetEvictable(1, false);
  lru_replacer.SetEvictable(2, false);
  lru_replacer.SetEvictable(3, false);
  lru_replacer.SetEvictable(4, true);
  lru_replacer.SetEvictable(5, true);
  lru_replacer.SetEvictable(6, false);
  ASSERT_EQ(2, lru_replacer.Size());

  // Scenario: Insert access history for frame 1. Now frame 1 has two access histories.
  // All other frames have max backward k-dist. The order of eviction is [2,3,4,5,1].
  lru_replacer.RecordAccess(1);

  // Scenario: Evict three pages from the replacer. Elements with max k-distance should be popped
  // first based on LRU.
  int value;
  lru_replacer.Evict(&value);
  ASSERT_EQ(4, value);

  lru_replacer.Evict(&value);
  ASSERT_EQ(5, value);

  lru_replacer.Evict(&value);
  // ASSERT_EQ(, value);

  ASSERT_EQ(0, lru_replacer.Size());

  // Scenario: Now replacer has frames [5,1].
  // Insert new frames 3, 4, and update access history for 5. We should end with [3,1,5,4]
  lru_replacer.RecordAccess(3);  // [5,3, 1]
  lru_replacer.RecordAccess(4);  // [5,3,4 ,1]
  lru_replacer.RecordAccess(5);  // [3,4, 1,5]
  lru_replacer.RecordAccess(4);  // [3, 1,5,4]
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);
  ASSERT_EQ(4, lru_replacer.Size());

  // Scenario: continue looking for victims. We expect 3 to be evicted next.
  lru_replacer.Evict(&value);  // [ 1,5,4]
  ASSERT_EQ(3, value);
  ASSERT_EQ(3, lru_replacer.Size());

  // Set 6 to be evictable. 6 Should be evicted next since it has max backward k-dist.
  lru_replacer.SetEvictable(6, true);  // [6, 1,5,4]
  ASSERT_EQ(4, lru_replacer.Size());
  lru_replacer.Evict(&value);  // [ 1,5,4]
  ASSERT_EQ(6, value);
  ASSERT_EQ(3, lru_replacer.Size());

  // Now we have [1,5,4]. Continue looking for victims.
  lru_replacer.SetEvictable(1, false);
  ASSERT_EQ(2, lru_replacer.Size());
  ASSERT_EQ(true, lru_replacer.Evict(&value));  // [ 4]
  ASSERT_EQ(5, value);
  ASSERT_EQ(1, lru_replacer.Size());

  // Update access history for 1. Now we have [4,1]. Next victim is 4.
  lru_replacer.RecordAccess(1);  // []
  lru_replacer.RecordAccess(1);
  lru_replacer.SetEvictable(1, true);  // [ 4,1]
  ASSERT_EQ(2, lru_replacer.Size());
  ASSERT_EQ(true, lru_replacer.Evict(&value));
  ASSERT_EQ(value, 4);

  ASSERT_EQ(1, lru_replacer.Size());
  lru_replacer.Evict(&value);
  ASSERT_EQ(value, 1);
  ASSERT_EQ(0, lru_replacer.Size());

  // This operation should not modify size
  ASSERT_EQ(false, lru_replacer.Evict(&value));
  ASSERT_EQ(0, lru_replacer.Size());
}

TEST(LRUKReplacerTest, SampleTest2) {
  LRUKReplacer lru_replacer(5000, 2);
  // lru_replacer.RecordAccess(7);
  // lru_replacer.RecordAccess(-1);
  std::vector<std::thread> threads;
  // for(int i = 2000; i < 5000; i++) {
  //   lru_replacer.RecordAccess(i);
  // }

  for (int tid = 0; tid < 4; tid++) {
    std::thread t([&lru_replacer, tid]() {
      // for(int i = 0; i < 5000; i++) {
      //   lru_replacer.RecordAccess(i);
      //   // ASSERT_EQ(0, lru_replacer.Size());
      // }
      for (int i = 0; i < 500; i++) {
        lru_replacer.RecordAccess(i * 4 + tid);
      }
      for (int i = 0; i < 1250; i++) {
        lru_replacer.RecordAccess(i * 4 + tid);
      }
      for (int i = 0; i < 1250; i++) {
        lru_replacer.SetEvictable(i * 4 + tid, true);
      }
      // for(int i = 0; i < 500; i++) {
      //   lru_replacer.SetEvictable(i * 4 + tid, false);
      // }
      // for(int i = 0; i < 500; i++) {
      //   lru_replacer.SetEvictable(i * 4 + tid, true);
      // }
      // for(int i = 0; i < 500; i++) {
      //   lru_replacer.SetEvictable(i * 4 + tid, true);
      // }

      for (int i = 0; i < 750; i++) {
        int value;
        ASSERT_EQ(true, lru_replacer.Evict(&value));
        // ASSERT_TRUE(value >= 2000);
      }
    });

    threads.push_back(std::move(t));
  }

  for (auto &x : threads) {
    x.join();
  }
  // ASSERT_EQ(5000, lru_replacer.Size());

  // for(int i = 0; i < 2000; i++) {
  //   int value;
  //   ASSERT_EQ(true, lru_replacer.Evict(&value));
  //   ASSERT_TRUE(value >= 2000);
  // }
  for (int i = 0; i < 233; i++) {
    lru_replacer.Remove(i);
  }
  ASSERT_EQ(1767, lru_replacer.Size());
}
}  // namespace bustub
