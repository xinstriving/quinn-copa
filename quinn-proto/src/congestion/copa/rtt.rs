use std::time::{Duration, Instant};

// The max RTT in the last 4 rounds.
#[derive(Debug, Clone)]
pub(crate) struct RttMaxTracker{
    rtt: [Duration; 4],
    idx: usize,
}

impl RttMaxTracker {
    pub(crate) fn new() -> Self {
        Self {
            rtt: [Duration::ZERO; 4],
            idx: 0,
        }
    }

    pub(crate) fn update(&mut self, rtt: Duration) {
        // 更新当前索引
        self.idx = (self.idx + 1) % 4;

        // 更新RTT值
        self.rtt[self.idx] = rtt;
    }

    pub(crate) fn max_rtt(&self) -> Duration {
        self.rtt.iter().copied().max().unwrap_or(Duration::ZERO)
    }
    
}


// The min RTT in the srtt / 2
#[derive(Debug, Clone)]
struct RttTracker {
    rtt: Duration,
    time: Instant,
}

#[derive(Debug, Clone)]
pub(crate) struct RttMinTracker {
    rtt: Vec<RttTracker>,
}

impl RttMinTracker {
    pub(crate) fn new() -> Self {
        Self {
            rtt: Vec::new(),
        }
    }

    pub(crate) fn update(&mut self, rtt: Duration, now: Instant, window: Duration) {
        // 清理过期的RTT
        self.rtt.retain(|tracker| now.duration_since(tracker.time) < window);

        // 添加新的RTT
        self.rtt.push(RttTracker {
            rtt,
            time: now,
        });
    }

    pub(crate) fn min_rtt(&self) -> Duration {
        // 返回最小RTT
        self.rtt.iter().map(|tracker| tracker.rtt).min().unwrap_or(Duration::MAX)
    }
}

// 基于时间桶的实现
#[derive(Debug, Clone)]
pub(crate) struct BucketRttMinTracker {
    buckets: [Duration; 100],  // 100个桶,每个桶表示100ms
    current_bucket: usize,
    last_update: Instant,
}

impl BucketRttMinTracker {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            buckets: [Duration::MAX; 100],
            current_bucket: 0,
            last_update: now,
        }
    }

    pub(crate) fn update(&mut self, rtt: Duration, now: Instant) {
        // 计算经过的时间桶数
        let elapsed = now.duration_since(self.last_update).as_millis() as usize / 100;
        
        if elapsed > 0 {
            // 清理过期的桶
            for i in 1..elapsed.min(100) {
                let bucket = (self.current_bucket + i) % 100;
                self.buckets[bucket] = Duration::MAX;
            }
            self.current_bucket = (self.current_bucket + elapsed) % 100;
            
            self.last_update = now;
        }

        // 更新当前桶的最小值
        self.buckets[self.current_bucket] = self.buckets[self.current_bucket].min(rtt);
    }

    pub(crate) fn min_rtt(&self) -> Duration {
        self.buckets.iter().min().copied().unwrap_or(Duration::MAX)
    }
}