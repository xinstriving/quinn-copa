use std::any::Any;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::io::{BufWriter, Write};
use std::fs::OpenOptions;

use super::{Controller, ControllerFactory, BASE_DATAGRAM_SIZE};
use crate::connection::RttEstimator;

use crate::congestion::copa::rtt::{BucketRttMinTracker, RttMaxTracker, RttMinTracker};

pub mod rtt;

/// Delta: determines how much to weigh delay compared to throughput.
pub const COPA_DELTA: f64 = 0.04;

const DEFAULT_DELTA: f64 = 0.5;

/// Max count while cwnd grows with the same direction. Speed up if
/// the count exceeds threshold.
const SPEED_UP_THRESHOLD: u64 = 3;

/// Default standing rtt filter length.
const STANDING_RTT_FILTER_WINDOW: Duration = Duration::from_millis(100);

/// Default min rtt filter length.
const MIN_RTT_FILTER_WINDOW: Duration = Duration::from_secs(10);

/// Pacing gain to cope with ack compression.
const PACING_GAIN: u64 = 2;

/// Delay oscillation rate to check if queueing delay is nearly empty:
/// queueing_delay < 0.1 * (Rtt_max - Rtt_min)
/// Where Rtt_max and Rtt_min is the max and min RTT in the last 4 rounds.
const DELAY_OSCILLATION_THRESHOLD: f64 = 0.1;

/// Max loss rate in one round. If the loss rate exceeds the threshold, switch
/// the mode to competitive mode.
const LOSS_RATE_THRESHOLD: f64 = 0.1;

const DEFAULT_SEND_UDP_PAYLOAD_SIZE: usize = 1200;

const INITIAL_RTT: Duration = Duration::from_millis(333);


/// Copa competing mode with other flows.
#[derive(Eq, PartialEq, Debug, Clone)]
enum CompetingMode {
    /// Default mode, no competing flows.
    Default,

    /// Competitive mode, use adaptive delta.
    Competitive,
}

/// Copa congestion window growth direction.
#[derive(Eq, PartialEq, Debug, Clone)]
enum Direction {
    /// Cwnd increasing.
    Up,

    /// Cwnd decreasing.
    Down,
}

/// Velocity control states.
#[derive(Debug, Clone)]
struct Velocity {
    /// Cwnd growth direction.
    direction: Direction,

    /// Velocity coef.
    velocity: u64,

    /// Cwnd recorded at last time.
    last_cwnd: u64,

    /// Times while cwnd grows with the same direction. Speed up if cnt
    /// exceeds threshold.
    same_direction_cnt: u64,
}

impl Default for Velocity {
    fn default() -> Self {
        Self {
            direction: Direction::Up,
            velocity: 1,
            last_cwnd: 0,
            same_direction_cnt: 0,
        }
    }
}

/// Copa congestion controller
#[derive(Debug, Clone)]
pub struct Copa {
    /// Config
    config: Arc<CopaConfig>,

    /// The time origin point when Copa init, used for window filter updating.
    init_time: Instant,

    /// Competing mode.
    mode: CompetingMode,

    /// Is in slow start state.
    slow_start: bool,

    /// Congestion window/
    cwnd: u64,

    /// Velocity parameter, speeds-up convergence.
    velocity: Velocity,

    /// Weight factor for queueing delay. Use default value in default mode,
    /// and use an adaptive one in competitive mode.
    delta: f64,

    increase_cwnd: bool,

    /// Target pacing rate.
    target_rate: u64,

    /// The last sent packet number.
    last_sent_pkt_num: u64,

    queueing_delay: Duration,

    rtt_max: RttMaxTracker,
    /// The max RTT in the last 4 rounds.
    rtt_min: BucketRttMinTracker,
    rtt_standing: RttMinTracker,
}

impl Copa {
    /// Construct a state using the given `config` and current time `now`
    pub fn new(config: Arc<CopaConfig>, now: Instant, current_mtu: u16) -> Self {
        let slow_start_delta = config.slow_start_delta;
        let initial_cwnd = config.initial_cwnd;

        Self {
            config,
            init_time: Instant::now(),
            mode: CompetingMode::Default,
            slow_start: true,
            cwnd: initial_cwnd,
            velocity: Velocity::default(),
            delta: DEFAULT_DELTA,
            increase_cwnd: false,
            target_rate: 0,
            last_sent_pkt_num: 0,
            queueing_delay: Duration::ZERO,
            rtt_max: RttMaxTracker::new(),
            rtt_min: BucketRttMinTracker::new(Instant::now()),
            rtt_standing: RttMinTracker::new(),
        }
    }

    fn debug_info(&self) -> String {
        format!(
            "rtt_standing: {:<10}, rtt_min: {:<12}, queueing_delay: {:<12}, cwnd: {:<10}, target_rate: {:<10}, increase_cwnd:{}, direction: {:<?} velocity: {:<10}, mode: {:<10?}, delta: {:<.3}, rtt_max: {:<10}\n",
            self.rtt_standing.min_rtt().as_secs_f64(),
            self.rtt_min.min_rtt().as_secs_f64(),
            self.queueing_delay.as_secs_f64(),
            self.cwnd,
            self.target_rate,
            self.increase_cwnd,
            self.velocity.direction,
            self.velocity.velocity,
            self.mode,
            self.delta,
            self.rtt_max.max_rtt().as_secs_f64(),
        )
    }

    fn update_mode(&mut self) {
        // Check if loss rate exceeds threshold when a new round starts. If so,
        // We assume that Copa should switch to competitive mode, to competing with
        // other buffer-filling flows.
        let loss_rate = self.queueing_delay.as_secs_f64()
            / (self.rtt_max.max_rtt().as_secs_f64() - self.rtt_min.min_rtt().as_secs_f64());
        self.mode = if LOSS_RATE_THRESHOLD <= loss_rate {
            CompetingMode::Competitive
        } else {
            CompetingMode::Default
        };

        match self.mode {
            CompetingMode::Default => {
                // self.delta = if self.slow_start {
                //     self.config.slow_start_delta
                // } else {
                //     self.config.steady_delta
                // };
                self.delta = DEFAULT_DELTA;
            }
            CompetingMode::Competitive => {
                self.delta = self.delta / (1 as f64 + self.delta);
                self.delta = self.delta.min(0.5);
            }
        }
    }

    /// Update congestion window.
    fn update_cwnd(&mut self) {
        // Deal with the following cases:
        // 1. slow_start, cwnd to increase: double cwnd
        // 2. slow_start, cwnd to decrease: exiting slow_start and decrease cwnd
        // 3. not slow_start, cwnd to increase: increase cwnd
        // 4. not slow_start, cwnd to decrease: decrease cwnd

        // Exit slow start once cwnd begins to decrease, i.e. rate reaches target rate.
        if self.slow_start && !self.increase_cwnd {
            self.slow_start = false;
        }

        if self.slow_start {
            // Stay in slow start until the target rate is reached.
            if self.increase_cwnd {
                self.cwnd *= 2;
            }
        } else {
            // Not in slow start. Adjust cwnd.
            let cwnd_delta = (4.0 * (self.velocity.velocity as f64)
                * self.config.max_datagram_size as f64
                / (self.delta * (self.cwnd as f64))) as u64;

            self.cwnd = if self.increase_cwnd {
                self.cwnd.saturating_add(cwnd_delta)
            } else {
                self.cwnd.saturating_sub(cwnd_delta)
            };

            // set an appropriate value
            if self.cwnd == 0 {
                self.cwnd = self.config.min_cwnd;
                self.velocity.velocity = 1;
            }
        }
    }

    fn update_velocity(&mut self) {
        // in the case that cwnd should increase in slow start, we do not need
        // to update velocity, since cwnd is always doubled.
        if self.slow_start && self.increase_cwnd {
            return;
        }

        // First time to run here.
        if self.velocity.last_cwnd == 0 {
            self.velocity.direction = Direction::Down;
            self.velocity.last_cwnd = self.cwnd.max(self.config.min_cwnd);
            self.velocity.velocity = 1;
            self.velocity.same_direction_cnt = 0;

            return;
        }

        // Check cwnd growth direction.
        // if in slow start, and target rate is not reached, then increase cwnd anyway.
        // otherwise, check and update direction to determine cwnd growth in next steps.
        let new_direction = if self.increase_cwnd {
            Direction::Up
        } else {
            Direction::Down
        };

        if new_direction != self.velocity.direction {
            // Direction changes, reset velocity.
            self.velocity.velocity = 1;
            self.velocity.same_direction_cnt = 0;
        } else {
            // Same direction, check to speed up.
            self.velocity.same_direction_cnt = self.velocity.same_direction_cnt.saturating_add(1);

            if self.velocity.same_direction_cnt >= SPEED_UP_THRESHOLD {
                self.velocity.velocity = self.velocity.velocity.saturating_mul(2);
            }
        }

        // if our current rate is much different than target, we double v every
        // RTT. That could result in a high v at some point in time. If we
        // detect a sudden direction change here, while v is still very high but
        // meant for opposite direction, we should reset it to 1.
        //
        // e.g. cwnd < last_recorded_cwnd && rate < target_rate
        // cwnd < last_recorded_cwnd means that direction is still DOWN while velocity may be large
        // rate < target_rate means that cwnd is about to increase
        // so a switch point is produced, we hope copa switch to increase up as soon as possible。
        // do not exist in the original paper.
        // if self.increase_cwnd
        //     && self.velocity.direction != Direction::Up
        //     && self.velocity.velocity > 1
        // {
        //     self.velocity.direction = Direction::Up;
        //     self.velocity.velocity = 1;
        // } else if !self.increase_cwnd
        //     && self.velocity.direction != Direction::Down
        //     && self.velocity.velocity > 1
        // {
        //     self.velocity.direction = Direction::Down;
        //     self.velocity.velocity = 1;
        // }

        self.velocity.direction = new_direction;
        self.velocity.last_cwnd = self.cwnd;
    }

}

impl Controller for Copa {
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

        // 1. update d_q = RTTstanding − RTTmin and srtt
        //      RTTmin is the smallest RTT in ten seconds.
        //      RTTstanding is the smallest RTT in the τ = srtt/2 srtt is the current value of the standard smoothed RTT estimate
        // 2. set lamda_t = 1 / (delta * d_q); lamda_t is the target rate
        // 3. if lamda = cwnd / RTTstanding < lamda_t,
        //      cwnd = cwnd + v / (delta * cwnd)
        //    else
        //      cwnd = cwnd - v / (delta * cwnd)
        // 4. if the current cwnd > newest cwnd, direction = up
        //      else direction = down
        //    if direction == last_direction && hold three RTT
        //      v *= 2
        //    else v = 1

        self.rtt_max.update(rtt.latest());
        self.rtt_min.update(rtt.latest(), now);
        self.rtt_standing.update(rtt.latest(), now, rtt.smoothed().div_f64(2.0));

        let standing_rtt = self.rtt_standing.min_rtt();
        let min_rtt = self.rtt_min.min_rtt();
        let current_rate: u64 = (self.cwnd as f64 / standing_rtt.as_secs_f64()) as u64;

        self.queueing_delay = standing_rtt.saturating_sub(min_rtt);
        if self.queueing_delay.is_zero() {
            // taking care of inf targetRate case here, this happens in beginning where
            // we do want to increase cwnd, e.g. slow start or no queuing happens.
            self.increase_cwnd = true;
            self.target_rate = (self.cwnd as f64 / standing_rtt.as_secs_f64()) as u64;
        } else {
            // Limit queueing_delay in case it's too small and get a huge target rate.
            self.target_rate = (self.config.max_datagram_size as f64
                / self.delta
                / self.queueing_delay.max(Duration::from_micros(1)).as_secs_f64())
                as u64;
            // self.target_rate = (1.0 as f64
            //     / self.delta
            //     / self.queueing_delay.max(Duration::from_micros(1)).as_secs_f64())
            //     as u64;
            self.increase_cwnd = self.target_rate >= current_rate;
        }

        self.update_mode();

        self.update_velocity();

        self.update_cwnd();


        print!("rtt: {:<12}, current_rate: {:<12} , {}", rtt.latest().as_secs_f64(), current_rate, self.debug_info());
        // print!("time: {}, cwnd: {}, target_rate: {}, mode: {:?}\n", now.duration_since(self.init_time).as_secs_f64(), self.cwnd, self.target_rate, self.mode);
    }

    fn on_congestion_event(
        &mut self,
        _now: Instant,
        _sent: Instant,
        _is_persistent_congestion: bool,
        _lost_bytes: u64,
    ) {
        if self.mode == CompetingMode::Competitive {
            self.delta *= 2.0 as f64;
            self.delta = self.delta.min(0.5); 
        }
    }

    fn on_mtu_update(&mut self, _new_mtu: u16) {
    }

    fn window(&self) -> u64 {
        self.cwnd
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }

    fn initial_window(&self) -> u64 {
        return self.config.initial_cwnd;
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn pacing_window(&self) -> u64 {
        self.window()
    }
}

/// Configuration for the `Copa` congestion controller
#[derive(Debug, Clone)]
pub struct CopaConfig {
    /// Minimal congestion window in bytes.
    min_cwnd: u64,

    /// Initial congestion window in bytes.
    initial_cwnd: u64,

    /// Initial Smoothed rtt.
    initial_rtt: Option<Duration>,

    /// Max datagram size in bytes.
    max_datagram_size: u64,

    /// Delta in slow start. Delta determines how much to weigh delay compared to
    /// throughput. A larger delta signifies that lower packet delays are preferable.
    slow_start_delta: f64,

    /// Delta in steady state.
    steady_delta: f64,

    /// Use rtt standing or latest rtt to calculate queueing delay.
    use_standing_rtt: bool,
}

impl CopaConfig {
    
}

impl Default for CopaConfig {
    fn default() -> Self {
        Self {
            min_cwnd: 4 * DEFAULT_SEND_UDP_PAYLOAD_SIZE as u64,
            initial_cwnd: 4 * DEFAULT_SEND_UDP_PAYLOAD_SIZE as u64,
            initial_rtt: Some(INITIAL_RTT),
            max_datagram_size: DEFAULT_SEND_UDP_PAYLOAD_SIZE as u64,
            slow_start_delta: DEFAULT_DELTA,
            steady_delta: DEFAULT_DELTA,
            use_standing_rtt: true,
        }
    }
}

impl ControllerFactory for CopaConfig {
    fn build(self: Arc<Self>, now: Instant, current_mtu: u16) -> Box<dyn Controller> {
        Box::new(Copa::new(self, now, current_mtu))
    }
}
