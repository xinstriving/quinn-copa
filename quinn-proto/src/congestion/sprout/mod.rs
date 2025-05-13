use std::any::Any;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::io::{BufWriter, Write};
use std::fs::OpenOptions;

use super::{Controller, ControllerFactory, BASE_DATAGRAM_SIZE};
use crate::connection::RttEstimator;

const NUM_TICKS: usize = 16;
const TARGET_DELAY_TICKS: usize = 6;

/// Copa congestion controller
#[derive(Debug, Clone)]
pub struct Sprout {
    /// Configuration for the controller
    /// The current congestion window
    cwnd: u64,
    /// the initial cwnd
    initial_cwnd: u64,
    /// The number of bytes sent in the current tick
    _count_this_tick: u64,
    /// The time of the last tick
    _last_tick: Instant,

    /// The estimated rate of the connection
    _ewma_rate_estimate: f64,

    counts: [f64;NUM_TICKS],

    /// recevive
    last_forcast: Instant,

    config: Arc<SproutConfig>,
}

impl Sprout {
    /// Construct a state using the given `config` and current time `now`
    pub fn new(config: Arc<SproutConfig>, now: Instant, current_mtu: u16) -> Self {
        Self {
            cwnd: config.initial_cwnd,
            initial_cwnd: config.initial_cwnd,
            _count_this_tick: 0,
            _last_tick: now,
            _ewma_rate_estimate: 0.0,
            counts: [0.0; NUM_TICKS],
            last_forcast: now,
            config,
        }
    }

    fn forecast(&mut self) {
        let current_forecast_tick = std::cmp::min((Instant::now() - self.last_forcast).as_millis() as usize / 20, NUM_TICKS - 1);
        self.last_forcast = Instant::now();
        let cumulative_delivery_tick = if current_forecast_tick + TARGET_DELAY_TICKS >= NUM_TICKS {
            NUM_TICKS - 1
        } else {
            current_forecast_tick + TARGET_DELAY_TICKS
        };

        print!("forecast: {} \n", cumulative_delivery_tick);
        print!("current_forecast_tick: {} \n", current_forecast_tick);
        print!("self.counts: {:?} \n", self.counts);

        self.cwnd = (self.counts[cumulative_delivery_tick] - self.counts[current_forecast_tick]) as u64;

        if self.cwnd < self.initial_cwnd {
            self.cwnd = self.initial_cwnd;
        }
    }

    fn advance_to(&mut self, now: Instant) {
        while self._last_tick + self.config.tick_interval < now {
            if self._count_this_tick > 0 {
                self._ewma_rate_estimate = (1.0 - self.config.alpha) * self._ewma_rate_estimate + ( self.config.alpha * self._count_this_tick as f64);
                print!("_ewma_rate_estimate:{}\n",self._ewma_rate_estimate);
                self._count_this_tick = 0;
                for i in 1..=NUM_TICKS {
                    self.counts[i - 1] = self._ewma_rate_estimate * i as f64;
                }

                self.forecast();
            }
            self._last_tick += self.config.tick_interval;
        }
    }

    fn recv(&mut self, bytes: u64) {
        self._count_this_tick += bytes;
    }
}

impl Controller for Sprout {
    fn on_sent(&mut self, now: Instant, bytes: u64, last_packet_number: u64) {
    }

    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        rtt: &RttEstimator,
    ) {
        let file = OpenOptions::new()
        .append(true)
        .create(true)
        .open("SDK_info/quic-rtt.txt")
        .expect("Failed to open file");
        let mut writer = BufWriter::new(file);
        writeln!(writer, "{:?}", rtt.get_latest()).expect("Failed to write to file");
        
        print!("on_ack: {} \n", bytes);
        self.advance_to(now);
        self.recv(bytes);
    }

    fn on_congestion_event(
        &mut self,
        _now: Instant,
        _sent: Instant,
        _is_persistent_congestion: bool,
        _lost_bytes: u64,
    ) {

    }

    fn on_mtu_update(&mut self, _new_mtu: u16) {
    }

    fn window(&self) -> u64 {
        print!("cwnd:{}\n", self.cwnd);
        self.cwnd
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }

    fn initial_window(&self) -> u64 {
        self.config.initial_cwnd
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn pacing_window(&self) -> u64 {
        self.window()
    }
}

/// Configuration for the `SPROUT` congestion controller
#[derive(Debug, Clone)]
pub struct SproutConfig {
    /// Initial congestion window
    initial_cwnd: u64,

    tick_interval: Duration,

    alpha: f64,
}

impl SproutConfig {
    
}

impl Default for SproutConfig {
    fn default() -> Self {
        Self {
            initial_cwnd: 4 * BASE_DATAGRAM_SIZE,
            tick_interval: Duration::from_millis(20),
            alpha: 1.0 / 8.0,
        }
    }
}

impl ControllerFactory for SproutConfig {
    fn build(self: Arc<Self>, now: Instant, current_mtu: u16) -> Box<dyn Controller> {
        Box::new(Sprout::new(self, now, current_mtu))
    }
}
