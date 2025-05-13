// Copyright (C) 2022, Cloudflare, Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
// IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
// THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
// PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

//! BBR v2 Congestion Control
//!
//! This implementation is based on the following draft:
//! <https://tools.ietf.org/html/draft-cardwell-iccrg-bbr-congestion-control-02>

// use crate::minmax::Minmax;
// use crate::recovery::*;

// use std::time::Duration;
// use std::time::Instant;

// use super::CongestionControlOps;

use std::any::Any;
use std::default;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, Instant};

// use rand::{Rng, SeedableRng};

use crate::congestion::bbr2::bw_estimation::BandwidthEstimation;
use crate::congestion::bbr2::min_max::MinMax;
use crate::connection::RttEstimator;
use super::{Controller, ControllerFactory, BASE_DATAGRAM_SIZE};

mod bw_estimation;
mod min_max;
mod init;
mod pacing;
mod per_ack;
mod per_loss;
mod per_transmit;

#[derive(Debug, Clone)]
pub struct Bbr2 {
    config: Arc<BbrConfig2>,
    current_mtu: u64,
    max_bandwidth: BandwidthEstimation,
    acked_bytes: u64,
    bbr2_state: State,
    loss_state: LossState,
    // recovery_state: RecoveryState,
    // recovery_window: u64,
    // is_at_full_bandwidth: bool,
    // pacing_gain: f32,
    // high_gain: f32,
    // drain_gain: f32,
    // cwnd_gain: f32,
    // high_cwnd_gain: f32,
    // last_cycle_start: Option<Instant>,
    // current_cycle_offset: u8,
    init_cwnd: u64,
    min_cwnd: u64,
    prev_in_flight_count: u64,
    // exit_probe_rtt_at: Option<Instant>,
    // probe_rtt_last_started_at: Option<Instant>,
    min_rtt: Duration,
    // exiting_quiescence: bool,
    // pacing_rate: u64,
    max_acked_packet_number: u64,
    max_sent_packet_number: u64,
    end_recovery_at_packet_number: u64,
    // cwnd: u64,
    current_round_trip_end_packet_number: u64,
    round_count: u64,
    // bw_at_last_round: u64,
    // round_wo_bw_gain: u64,
    ack_aggregation: AckAggregationState,
    // random_number_generator: rand::rngs::StdRng,
    // bbrv2新增
    
    max_datagram_size: usize,
    send_quantum:usize,
    // rtt_estimator: Arc<RttEstimator>,
    delivered: usize,
    extra_acked: usize,
    congestion_window: usize,
    congestion_recovery_start_time: Option<Instant>,
    in_flight_size: usize,
    app_limited: bool,
    initial_congestion_window_packets: usize,
    estimated_flight_size: usize,
    latest_rtt: Duration,
}

impl Bbr2 {
    pub fn new(config: Arc<BbrConfig2>, current_mtu: u16) -> Self {
        let initial_window: u64 = config.initial_window;
        let mut bbr2 = Self  {
            config,
            current_mtu: current_mtu as u64,
            max_bandwidth: BandwidthEstimation::default(),
            acked_bytes: 0,
            bbr2_state: State::new(),
            loss_state: Default::default(),
            init_cwnd: initial_window,
            min_cwnd: calculate_min_window(current_mtu as u64),
            prev_in_flight_count: 0,
            min_rtt: Default::default(),
            max_acked_packet_number: 0,
            max_sent_packet_number: 0,
            end_recovery_at_packet_number: 0,
            current_round_trip_end_packet_number: 0,
            round_count: 0,
            ack_aggregation: AckAggregationState::default(),

            max_datagram_size: BASE_DATAGRAM_SIZE as usize, 
            send_quantum: BASE_DATAGRAM_SIZE as usize,
            delivered:0,
            extra_acked: 0,
            congestion_window: initial_window as usize,
            congestion_recovery_start_time: None,
            in_flight_size: 0,
            app_limited: false,
            initial_congestion_window_packets:10,
            estimated_flight_size: 0,
            latest_rtt: Default::default(),
        };
        bbr2.on_init();
        bbr2
    }
    
    // When entering the recovery episode.
    fn bbr2_enter_recovery(&mut self, in_flight: usize, now: Instant) {
        eprintln!("bbr2_enter_recovery");
        self.bbr2_state.prior_cwnd = per_ack::bbr2_save_cwnd(self);
        eprint!("cwnd: {}",self.congestion_window);
        self.congestion_window =
            in_flight + self.bbr2_state.newly_acked_bytes.max(self.max_datagram_size * 4);
        eprintln!(" ===> {}",self.congestion_window);
        self.congestion_recovery_start_time = Some(now);

        self.bbr2_state.packet_conservation = true;
        self.bbr2_state.in_recovery = true;

        // Start round now.
        self.bbr2_state.next_round_delivered = self.delivered;
    }

    // When exiting the recovery episode.
    fn bbr2_exit_recovery(&mut self) {
        self.congestion_recovery_start_time = None;

        self.bbr2_state.packet_conservation = false;
        self.bbr2_state.in_recovery = false;

        per_ack::bbr2_restore_cwnd(self);
    }

    // Congestion Control Hooks.
    //
    fn on_init(&mut self) {
        init::bbr2_init(self);
    }

    fn on_packet_sent(
        &mut self, _sent_bytes: usize, bytes_in_flight: usize, now: Instant,
    ) {
        per_transmit::bbr2_on_transmit(self, bytes_in_flight, now);
    }
    // 这个packets: &mut Vec<Acked>需要自己搞一下
    // fn on_packets_acked(
    //     &mut self, bytes_in_flight: usize, pkt: &mut Acked,
    //     now: Instant, _rtt_stats: &RttStats,
    // )
    fn on_packets_acked(
        &mut self, bytes_in_flight: usize, newly_acked_size: usize,now: Instant
    ) {
        self.bbr2_state.newly_acked_bytes = newly_acked_size;

        // let time_sent = pkt.time_sent;

        self.bbr2_state.prior_bytes_in_flight = bytes_in_flight;
        // let mut bytes_in_flight = bytes_in_flight;

        
        // per_ack::bbr2_update_model_and_state(self, &pkt, bytes_in_flight, now);
        per_ack::bbr2_update_model_and_state(self, bytes_in_flight, now);

        // self.bbr2_state.prior_bytes_in_flight = bytes_in_flight;
        // bytes_in_flight -= pkt.size;

        // self.bbr2_state.newly_acked_bytes += pkt.size;
        
        if !self.loss_state.has_losses() && self.max_acked_packet_number > self.end_recovery_at_packet_number {
            self.bbr2_exit_recovery();
        }
        // if let Some(ts) = time_sent {
        //     if !self.in_congestion_recovery(ts) {
        //         // Upon exiting loss recovery.
        //         self.bbr2_exit_recovery();
        //     }
        // }

        per_ack::bbr2_update_control_parameters(self, bytes_in_flight, now);

        self.bbr2_state.newly_lost_bytes = 0;
    }

    // fn congestion_event(
    //     &mut self, bytes_in_flight: usize, lost_bytes: usize,
    //     largest_lost_pkt: &Sent, now: Instant,
    // ) 
    fn congestion_event(
        &mut self, bytes_in_flight: usize, lost_bytes: usize,
        now: Instant,
    ) {
        self.bbr2_state.newly_lost_bytes = lost_bytes;

        // per_loss::bbr2_update_on_loss(self, largest_lost_pkt, lost_bytes, now);
        per_loss::bbr2_update_on_loss(self, lost_bytes, now);

        // Upon entering Fast Recovery.
        // if !self.in_congestion_recovery(largest_lost_pkt.time_sent) {
        //     // Upon entering Fast Recovery.
        //     self.bbr2_enter_recovery(bytes_in_flight - lost_bytes, now);
        // }
        if lost_bytes != 0 {
            self.end_recovery_at_packet_number = self.max_sent_packet_number;
            if !self.in_congestion_recovery(now) {
                // Upon entering Fast Recovery.
                eprintln!("congestion_event: bytes_in_flight:{}, lost_bytes:{}", bytes_in_flight, lost_bytes);
                self.bbr2_enter_recovery(bytes_in_flight - lost_bytes, now);
            }
        }
        
    }

    fn in_congestion_recovery(&self, sent_time: Instant) -> bool {
        match self.congestion_recovery_start_time {
            Some(congestion_recovery_start_time) =>
                sent_time <= congestion_recovery_start_time,

            None => false,
        }
    }

    fn checkpoint(&mut self) {}

    fn rollback(&mut self) -> bool {
        false
    }

    fn has_custom_pacing() -> bool {
        true
    }

    // rate -> kbit/sec. if inf, return -1
    fn rate_kbps(rate: u64) -> isize {
        if rate == u64::MAX {
            -1
        } else {
            (rate * 8 / 1000) as isize
        }
    }

}


impl Controller for Bbr2 {
    fn on_sent(&mut self, now: Instant, bytes: u64, last_packet_number: u64) {
        // eprintln!("on_sent start");
        self.max_sent_packet_number = last_packet_number;
        self.max_bandwidth.on_sent(now, bytes);
        
        self.estimated_flight_size = self.estimated_flight_size.saturating_add(bytes as usize); // 预估的flight 的 size
        self.on_packet_sent(bytes as usize, self.estimated_flight_size, now);
        // eprintln!("on_sent over");
    }

    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        rtt: &RttEstimator,
    ) {
        // eprintln!("on_ack start");
        self.max_bandwidth
            .on_ack(now, sent, bytes, self.round_count, app_limited);
        self.acked_bytes += bytes;
        self.estimated_flight_size = self.estimated_flight_size.saturating_sub(bytes as usize); // 预估的flight 的 size
        
        if ((now > self.bbr2_state.min_rtt_stamp + rtt.get().saturating_mul(MIN_RTT_FILTER_LEN)) && !app_limited) || self.min_rtt > rtt.min() {
            self.min_rtt = rtt.min();
        }
        // eprintln!("on_ack over");
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
    ) {
        // eprintln!("on_end_acks start");
        let bytes_acked = self.max_bandwidth.bytes_acked_this_window();
        self.delivered += bytes_acked as usize;
        let excess_acked = self.ack_aggregation.update_ack_aggregation_bytes(
            bytes_acked,
            now,
            self.round_count,
            self.max_bandwidth.get_estimate(),
        );
        self.in_flight_size = in_flight as usize;// 更新
        self.estimated_flight_size = in_flight as usize;
        // 删除函数 bbr2_update_ack_aggregation
        self.bbr2_state.extra_acked = excess_acked as usize;
        self.bbr2_state.extra_acked_delivered += excess_acked as usize;
        // 删除函数 bbr2_update_ack_aggregation

        self.max_bandwidth.end_acks(self.round_count, app_limited);
        if let Some(largest_acked_packet) = largest_packet_num_acked {
            self.max_acked_packet_number = largest_acked_packet;
        }

        let mut is_round_start = false;
        if bytes_acked > 0 {
            is_round_start =
                self.max_acked_packet_number > self.current_round_trip_end_packet_number;
            if is_round_start {
                self.current_round_trip_end_packet_number = self.max_sent_packet_number;
                self.round_count += 1;
            }
        }


        self.on_packets_acked(self.estimated_flight_size,bytes_acked as usize,  now);

        // self.update_recovery_state(is_round_start);

        // if self.mode == Mode::ProbeBw {
        //     self.update_gain_cycle_phase(now, in_flight);
        // }

        // if is_round_start && !self.is_at_full_bandwidth {
        //     self.check_if_full_bw_reached(app_limited);
        // }

        // self.maybe_exit_startup_or_drain(now, in_flight);

        // self.maybe_enter_or_exit_probe_rtt(now, is_round_start, in_flight, app_limited);

        // After the model is updated, recalculate the pacing rate and congestion window.
        // self.calculate_pacing_rate();
        // self.calculate_cwnd(bytes_acked, excess_acked);
        // self.calculate_recovery_window(bytes_acked, self.loss_state.lost_bytes, in_flight);

        self.prev_in_flight_count = in_flight;
        self.loss_state.reset();
        // eprintln!("on_end_acks over");
    }

    fn on_congestion_event(
        &mut self,
        now: Instant,
        _sent: Instant,
        _is_persistent_congestion: bool,
        lost_bytes: u64,
    ) {
        // eprintln!("on_congestion_event start");
        self.loss_state.lost_bytes += lost_bytes;
        self.congestion_event(self.estimated_flight_size, lost_bytes as usize, now);
        // eprintln!("on_congestion_event over");
    }

    fn on_mtu_update(&mut self, new_mtu: u16) {
        // eprintln!("on_mtu_update start");
        self.current_mtu = new_mtu as u64;
        self.min_cwnd = calculate_min_window(self.current_mtu);
        self.init_cwnd = self.config.initial_window.max(self.min_cwnd);
        // self.cwnd = self.cwnd.max(self.min_cwnd);
        self.congestion_window = self.congestion_window.max(self.min_cwnd as usize);
        // eprintln!("on_mtu_update over");
    }

    fn window(&self) -> u64 {
        // if self.mode == Mode::ProbeRtt {
        //     return self.get_probe_rtt_cwnd();
        // } else if self.recovery_state.in_recovery() && self.mode != Mode::Startup {
        //     return self.cwnd.min(self.recovery_window);
        // }
        // self.cwnd
        eprintln!("get congestion_window");
        // return 10000000;
        self.congestion_window as u64
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }

    fn initial_window(&self) -> u64 {
        self.config.initial_window
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn pacing_window(&self) -> u64 {
        
        // return 10000000;
        // self.congestion_window as u64
        let min_rtt_secs = self.bbr2_state.min_rtt.as_secs_f64();
        if self.bbr2_state.pacing_rate == 0 || min_rtt_secs < 0.01 {
            // eprintln!("using cwnd, pacing_rate:{}, min_rtt:{}, cwnd:{}", self.bbr2_state.pacing_rate, min_rtt_secs, self.congestion_window);
            self.congestion_window as u64
        }
        else {
            let mut pacwid = (self.bbr2_state.pacing_rate as f64 * min_rtt_secs) as u64;
            if pacwid < (0.2*self.congestion_window as f64) as u64 {
                // eprintln!("pacwid < 0.2*cwnd, pacing_rate:{}, min_rtt:{}, cwnd:{}", self.bbr2_state.pacing_rate, min_rtt_secs, self.congestion_window);
                // self.congestion_window = self.congestion_window.max(self.max_datagram_size * 2);
                pacwid = self.congestion_window as u64;  
            }
            else {
                // eprintln!("using origin pacwid pacing_rate:{}, min_rtt:{}, cwnd:{}", self.bbr2_state.pacing_rate, min_rtt_secs, self.congestion_window);
            }
            pacwid
        }
    }
}


/// Configuration for the [`Bbr`] congestion controller
#[derive(Debug, Clone)]
pub struct BbrConfig2 {
    initial_window: u64,
}

impl BbrConfig2 {
    /// Default limit on the amount of outstanding data in bytes.
    ///
    /// Recommended value: `min(10 * max_datagram_size, max(2 * max_datagram_size, 14720))`
    pub fn initial_window(&mut self, value: u64) -> &mut Self {
        self.initial_window = value;
        self
    }
}
// Do not allow initial congestion window to be greater than 200 packets.
const K_MAX_INITIAL_CONGESTION_WINDOW: u64 = 200;
impl Default for BbrConfig2 {
    fn default() -> Self {
        Self {
            initial_window: K_MAX_INITIAL_CONGESTION_WINDOW * BASE_DATAGRAM_SIZE,
        }
    }
}

impl ControllerFactory for BbrConfig2 {
    fn build(self: Arc<Self>, _now: Instant, current_mtu: u16) -> Box<dyn Controller> {
        Box::new(Bbr2::new(self, current_mtu))
    }
}

/// The static discount factor of 1% used to scale BBR.bw to produce
/// BBR.pacing_rate.
const PACING_MARGIN_PERCENT: f64 = 0.01;

/// A constant specifying the minimum gain value
/// for calculating the pacing rate that will allow the sending rate to
/// double each round (4*ln(2) ~=2.77 ) BBRStartupPacingGain; used in
/// Startup mode for BBR.pacing_gain.
const STARTUP_PACING_GAIN: f64 = 2.77;

/// A constant specifying the pacing gain value for Probe Down mode.
const PROBE_DOWN_PACING_GAIN: f64 = 3_f64 / 4_f64;

/// A constant specifying the pacing gain value for Probe Up mode.
const PROBE_UP_PACING_GAIN: f64 = 5_f64 / 4_f64;

/// A constant specifying the pacing gain value for Probe Refill, Probe RTT,
/// Cruise mode.
const PACING_GAIN: f64 = 1.0;

/// A constant specifying the minimum gain value for the cwnd in the Startup
/// phase
const STARTUP_CWND_GAIN: f64 = 2.77;

/// A constant specifying the minimum gain value for
/// calculating the cwnd that will allow the sending rate to double each
/// round (2.0); used in Probe and Drain mode for BBR.cwnd_gain.
const CWND_GAIN: f64 = 2.0;

/// The maximum tolerated per-round-trip packet loss rate
/// when probing for bandwidth (the default is 2%).
const LOSS_THRESH: f64 = 0.02;

/// Exit startup if the number of loss marking events is >=FULL_LOSS_COUNT
const FULL_LOSS_COUNT: u32 = 8;

/// The default multiplicative decrease to make upon each round
/// trip during which the connection detects packet loss (the value is
/// 0.7).
const BETA: f64 = 0.7;

/// The multiplicative factor to apply to BBR.inflight_hi
/// when attempting to leave free headroom in the path (e.g. free space
/// in the bottleneck buffer or free time slots in the bottleneck link)
/// that can be used by cross traffic (the value is 0.85).
const HEADROOM: f64 = 0.85;

/// The minimal cwnd value BBR targets, to allow
/// pipelining with TCP endpoints that follow an "ACK every other packet"
/// delayed-ACK policy: 4 * SMSS.
const MIN_PIPE_CWND_PKTS: usize = 4;

// To do: Tune window for expiry of Max BW measurement
// The filter window length for BBR.MaxBwFilter = 2 (representing up to 2
// ProbeBW cycles, the current cycle and the previous full cycle).
// const MAX_BW_FILTER_LEN: Duration = Duration::from_secs(2);

// To do: Tune window for expiry of ACK aggregation measurement
// The window length of the BBR.ExtraACKedFilter max filter window: 10 (in
// units of packet-timed round trips).
// const EXTRA_ACKED_FILTER_LEN: Duration = Duration::from_secs(10);

/// A constant specifying the length of the BBR.min_rtt min filter window,
/// MinRTTFilterLen is 10 secs.
const MIN_RTT_FILTER_LEN: u32 = 1;

/// A constant specifying the gain value for calculating the cwnd during
/// ProbeRTT: 0.5 (meaning that ProbeRTT attempts to reduce in-flight data to
/// 50% of the estimated BDP).
const PROBE_RTT_CWND_GAIN: f64 = 0.5;

/// A constant specifying the minimum duration for which ProbeRTT state holds
/// inflight to BBRMinPipeCwnd or fewer packets: 200 ms.
const PROBE_RTT_DURATION: Duration = Duration::from_millis(200);

/// ProbeRTTInterval: A constant specifying the minimum time interval between
/// ProbeRTT states. To do: investigate probe duration. Set arbitrarily high for
/// now.
const PROBE_RTT_INTERVAL: Duration = Duration::from_secs(86400);

/// Threshold for checking a full bandwidth growth during Startup.
const MAX_BW_GROWTH_THRESHOLD: f64 = 1.25;

/// Threshold for determining maximum bandwidth of network during Startup.
const MAX_BW_COUNT: usize = 3;

/// BBR2 Internal State Machine.
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
enum BBR2StateMachine {
    Startup,
    Drain,
    ProbeBWDOWN,
    ProbeBWCRUISE,
    ProbeBWREFILL,
    ProbeBWUP,
    ProbeRTT,
}

/// BBR2 Ack Phases.
#[derive(Debug, PartialEq, Eq, Clone)]
enum BBR2AckPhase {
    Init,
    ProbeFeedback,
    ProbeStarting,
    ProbeStopping,
    Refilling,
}
#[derive(Debug, Clone)]
/// BBR2 Specific State Variables.
pub struct State {
    // 2.3.  Per-ACK Rate Sample State
    // It's stored in rate sample but we keep in BBR state here.

    // The volume of data that was estimated to be in
    // flight at the time of the transmission of the packet that has just
    // been ACKed.
    tx_in_flight: usize,

    // The volume of data that was declared lost between the
    // transmission and acknowledgement of the packet that has just been
    // ACKed.
    lost: usize,

    // The volume of data cumulatively or selectively acknowledged upon the ACK
    // that was just received.  (This quantity is referred to as "DeliveredData"
    // in [RFC6937].)
    newly_acked_bytes: usize,

    // The volume of data newly marked lost upon the ACK that was just received.
    newly_lost_bytes: usize,

    // 2.4.  Output Control Parameters
    // The current pacing rate for a BBR2 flow, which controls inter-packet
    // spacing.
    pacing_rate: u64,

    // Save initial pacing rate so we can update when more reliable bytes
    // delivered and RTT samples are available
    init_pacing_rate: u64,

    // 2.5.  Pacing State and Parameters
    // The dynamic gain factor used to scale BBR.bw to
    // produce BBR.pacing_rate.
    pacing_gain: f64,

    // 2.6.  cwnd State and Parameters
    // The dynamic gain factor used to scale the estimated BDP to produce a
    // congestion window (cwnd).
    cwnd_gain: f64,

    // A boolean indicating whether BBR is currently using packet conservation
    // dynamics to bound cwnd.
    packet_conservation: bool,

    // 2.7.  General Algorithm State
    // The current state of a BBR2 flow in the BBR2 state machine.
    state: BBR2StateMachine,

    // Count of packet-timed round trips elapsed so far.
    round_count: u64,

    // A boolean that BBR2 sets to true once per packet-timed round trip,
    // on ACKs that advance BBR2.round_count.
    round_start: bool,

    // packet.delivered value denoting the end of a packet-timed round trip.
    next_round_delivered: usize,

    // A boolean that is true if and only if a connection is restarting after
    // being idle.
    idle_restart: bool,

    // 2.9.1.  Data Rate Network Path Model Parameters
    // The windowed maximum recent bandwidth sample - obtained using the BBR
    // delivery rate sampling algorithm
    // [draft-cheng-iccrg-delivery-rate-estimation] - measured during the current
    // or previous bandwidth probing cycle (or during Startup, if the flow is
    // still in that state).  (Part of the long-term model.)
    max_bw: u64,

    // The long-term maximum sending bandwidth that the algorithm estimates will
    // produce acceptable queue pressure, based on signals in the current or
    // previous bandwidth probing cycle, as measured by loss.  (Part of the
    // long-term model.)
    bw_hi: u64,

    // The short-term maximum sending bandwidth that the algorithm estimates is
    // safe for matching the current network path delivery rate, based on any
    // loss signals in the current bandwidth probing cycle.  This is generally
    // lower than max_bw or bw_hi (thus the name).  (Part of the short-term
    // model.)
    bw_lo: u64,

    // The maximum sending bandwidth that the algorithm estimates is appropriate
    // for matching the current network path delivery rate, given all available
    // signals in the model, at any time scale.  It is the min() of max_bw,
    // bw_hi, and bw_lo.
    bw: u64,

    // 2.9.2.  Data Volume Network Path Model Parameters
    // The windowed minimum round-trip time sample measured over the last
    // MinRTTFilterLen = 10 seconds.  This attempts to estimate the two-way
    // propagation delay of the network path when all connections sharing a
    // bottleneck are using BBR, but also allows BBR to estimate the value
    // required for a bdp estimate that allows full throughput if there are
    // legacy loss-based Reno or CUBIC flows sharing the bottleneck.
    min_rtt: Duration,

    // The estimate of the network path's BDP (Bandwidth-Delay Product), computed
    // as: BBR.bdp = BBR.bw * BBR.min_rtt.
    bdp: usize,

    // A volume of data that is the estimate of the recent degree of aggregation
    // in the network path.
    extra_acked: usize,

    // The estimate of the minimum volume of data necessary to achieve full
    // throughput when using sender (TSO/GSO) and receiver (LRO, GRO) host
    // offload mechanisms.
    offload_budget: usize,

    // The estimate of the volume of in-flight data required to fully utilize the
    // bottleneck bandwidth available to the flow, based on the BDP estimate
    // (BBR.bdp), the aggregation estimate (BBR.extra_acked), the offload budget
    // (BBR.offload_budget), and BBRMinPipeCwnd.
    max_inflight: usize,

    // Analogous to BBR.bw_hi, the long-term maximum volume of in-flight data
    // that the algorithm estimates will produce acceptable queue pressure, based
    // on signals in the current or previous bandwidth probing cycle, as measured
    // by loss.  That is, if a flow is probing for bandwidth, and observes that
    // sending a particular volume of in-flight data causes a loss rate higher
    // than the loss rate objective, it sets inflight_hi to that volume of data.
    // (Part of the long-term model.)
    inflight_hi: usize,

    // Analogous to BBR.bw_lo, the short-term maximum volume of in-flight data
    // that the algorithm estimates is safe for matching the current network path
    // delivery process, based on any loss signals in the current bandwidth
    // probing cycle.  This is generally lower than max_inflight or inflight_hi
    // (thus the name).  (Part of the short-term model.)
    inflight_lo: usize,

    // 2.10.  State for Responding to Congestion
    // a 1-round-trip max of delivered bandwidth (rs.delivery_rate).
    bw_latest: u64,

    // a 1-round-trip max of delivered volume of data (rs.delivered).
    inflight_latest: usize,

    // 2.11.  Estimating BBR.max_bw
    // The filter for tracking the maximum recent rs.delivery_rate sample, for
    // estimating BBR.max_bw.
    // max_bw_filter: Minmax<u64>,

    // The virtual time used by the BBR.max_bw filter window.  Note that
    // BBR.cycle_count only needs to be tracked with a single bit, since the
    // BBR.MaxBwFilter only needs to track samples from two time slots: the
    // previous ProbeBW cycle and the current ProbeBW cycle.
    cycle_count: u64,

    // 2.12.  Estimating BBR.extra_acked
    // the start of the time interval for estimating the excess amount of data
    // acknowledged due to aggregation effects.
    extra_acked_interval_start: Instant,

    // the volume of data marked as delivered since
    // BBR.extra_acked_interval_start.
    extra_acked_delivered: usize,

    // BBR.ExtraACKedFilter: the max filter tracking the recent maximum degree of
    // aggregation in the path.
    // extra_acked_filter: Minmax<usize>,

    // 2.13.  Startup Parameters and State
    // A boolean that records whether BBR estimates that it has ever fully
    // utilized its available bandwidth ("filled the pipe").
    filled_pipe: bool,

    // A recent baseline BBR.max_bw to estimate if BBR has "filled the pipe" in
    // Startup.
    full_bw: u64,

    // The number of non-app-limited round trips without large increases in
    // BBR.full_bw.
    full_bw_count: usize,

    // 2.14.1.  Parameters for Estimating BBR.min_rtt
    // The wall clock time at which the current BBR.min_rtt sample was obtained.
    min_rtt_stamp: Instant,

    // 2.14.2.  Parameters for Scheduling ProbeRTT
    // The minimum RTT sample recorded in the last ProbeRTTInterval.
    probe_rtt_min_delay: Duration,

    // The wall clock time at which the current BBR.probe_rtt_min_delay sample
    // was obtained.
    probe_rtt_min_stamp: Instant,

    // A boolean recording whether the BBR.probe_rtt_min_delay has expired and is
    // due for a refresh with an application idle period or a transition into
    // ProbeRTT state.
    probe_rtt_expired: bool,

    // Others
    // A state indicating we are in the recovery.
    in_recovery: bool,

    // Start time of the connection.
    start_time: Instant,

    // Saved cwnd before loss recovery.
    prior_cwnd: usize,

    // Whether we have a bandwidth probe samples.
    bw_probe_samples: bool,

    // Others
    probe_up_cnt: usize,

    prior_bytes_in_flight: usize,

    probe_rtt_done_stamp: Option<Instant>,

    probe_rtt_round_done: bool,

    bw_probe_wait: Duration,

    rounds_since_probe: usize,

    cycle_stamp: Instant,

    ack_phase: BBR2AckPhase,

    bw_probe_up_rounds: usize,

    bw_probe_up_acks: usize,

    loss_round_start: bool,

    loss_round_delivered: usize,

    loss_in_round: bool,

    loss_events_in_round: usize,
}

impl State {
    pub fn new() -> Self {
        let now = Instant::now();

        State {
            tx_in_flight: 0,

            lost: 0,

            newly_acked_bytes: 0,

            newly_lost_bytes: 0,

            pacing_rate: 0,

            init_pacing_rate: 0,

            pacing_gain: 0.0,

            cwnd_gain: 0.0,

            packet_conservation: false,

            state: BBR2StateMachine::Startup,

            round_count: 0,

            round_start: false,

            next_round_delivered: 0,

            idle_restart: false,

            max_bw: 0,

            bw_hi: u64::MAX,

            bw_lo: u64::MAX,

            bw: 0,

            min_rtt: Duration::MAX,

            bdp: 0,

            extra_acked: 0,

            offload_budget: 0,

            max_inflight: 0,

            inflight_hi: usize::MAX,

            inflight_lo: usize::MAX,

            bw_latest: 0,

            inflight_latest: 0,

            // max_bw_filter: Minmax::new(0),

            cycle_count: 0,

            extra_acked_interval_start: now,

            extra_acked_delivered: 0,

            // extra_acked_filter: Minmax::new(0),

            filled_pipe: false,

            full_bw: 0,

            full_bw_count: 0,

            min_rtt_stamp: now,

            probe_rtt_min_delay: Duration::MAX,

            probe_rtt_min_stamp: now,

            probe_rtt_expired: false,

            in_recovery: false,

            start_time: now,

            prior_cwnd: 0,

            bw_probe_samples: false,

            probe_up_cnt: 0,

            prior_bytes_in_flight: 0,

            probe_rtt_done_stamp: None,

            probe_rtt_round_done: false,

            bw_probe_wait: Duration::ZERO,

            rounds_since_probe: 0,

            cycle_stamp: now,

            ack_phase: BBR2AckPhase::Init,

            bw_probe_up_rounds: 0,

            bw_probe_up_acks: 0,

            loss_round_start: false,

            loss_round_delivered: 0,

            loss_in_round: false,

            loss_events_in_round: 0,
        }
    }
}

#[derive(Debug, Default, Copy, Clone)]
struct AckAggregationState {
    max_ack_height: MinMax,
    aggregation_epoch_start_time: Option<Instant>,
    aggregation_epoch_bytes: u64,
}

impl AckAggregationState {
    fn update_ack_aggregation_bytes(
        &mut self,
        newly_acked_bytes: u64,
        now: Instant,
        round: u64,
        max_bandwidth: u64,
    ) -> u64 {
        // Compute how many bytes are expected to be delivered, assuming max
        // bandwidth is correct.
        let expected_bytes_acked = max_bandwidth
            * now
                .saturating_duration_since(self.aggregation_epoch_start_time.unwrap_or(now))
                .as_micros() as u64
            / 1_000_000;

        // Reset the current aggregation epoch as soon as the ack arrival rate is
        // less than or equal to the max bandwidth.
        if self.aggregation_epoch_bytes <= expected_bytes_acked {
            // Reset to start measuring a new aggregation epoch.
            self.aggregation_epoch_bytes = newly_acked_bytes;
            self.aggregation_epoch_start_time = Some(now);
            return 0;
        }

        // Compute how many extra bytes were delivered vs max bandwidth.
        // Include the bytes most recently acknowledged to account for stretch acks.
        self.aggregation_epoch_bytes += newly_acked_bytes;
        let diff = self.aggregation_epoch_bytes - expected_bytes_acked;
        self.max_ack_height.update_max(round, diff);
        diff
    }
}
// 有可能是聚合的ack或者是单个的ack
#[derive(Clone)]
pub struct Acked {
    pub pkt_num: u64,

    pub time_sent: Option<Instant>,// 本次ack的发送时间

    pub size: usize,// 本次ack确认发送的数据量

    pub rtt: Duration,

    pub delivered: usize,

    pub delivered_time: Instant,

    pub first_sent_time: Instant,

    pub is_app_limited: bool,
}
// Indicates how the congestion control limits the amount of bytes in flight.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum RecoveryState {
    // Do not limit.
    NotInRecovery,
    // Allow an extra outstanding byte for each byte acknowledged.
    Conservation,
    // Allow two extra outstanding bytes for each byte acknowledged (slow
    // start).
    Growth,
}

impl RecoveryState {
    pub(super) fn in_recovery(&self) -> bool {
        !matches!(self, Self::NotInRecovery)
    }
}

#[derive(Debug, Clone, Default)]
struct LossState {
    lost_bytes: u64,
}

impl LossState {
    pub(super) fn reset(&mut self) {
        self.lost_bytes = 0;
    }

    pub(super) fn has_losses(&self) -> bool {
        self.lost_bytes != 0
    }
}

fn calculate_min_window(current_mtu: u64) -> u64 {
    4 * current_mtu
}

// TODO: write more tests
// #[cfg(test)]
// mod tests {
//     use super::*;

//     use smallvec::smallvec;

//     use crate::recovery;

//     #[test]
//     fn bbr_init() {
//         let mut cfg = crate::Config::new(crate::PROTOCOL_VERSION).unwrap();
//         cfg.set_cc_algorithm(recovery::CongestionControlAlgorithm::BBR2);

//         let r = Recovery::new(&cfg);

//         // on_init() is called in Connection::new(), so it need to be
//         // called manually here.

//         assert_eq!(
//             r.cwnd(),
//             r.max_datagram_size * r.congestion.initial_congestion_window_packets
//         );
//         assert_eq!(r.bytes_in_flight, 0);

//         assert_eq!(r.congestion.bbr2_state.state, BBR2StateMachine::Startup);
//     }

//     #[test]
//     fn bbr2_startup() {
//         let mut cfg = crate::Config::new(crate::PROTOCOL_VERSION).unwrap();
//         cfg.set_cc_algorithm(recovery::CongestionControlAlgorithm::BBR2);

//         let mut r = Recovery::new(&cfg);
//         let now = Instant::now();
//         let mss = r.max_datagram_size;

//         // Send 5 packets.
//         for pn in 0..5 {
//             let pkt = Sent {
//                 pkt_num: pn,
//                 frames: smallvec![],
//                 time_sent: now,
//                 time_acked: None,
//                 time_lost: None,
//                 size: mss,
//                 ack_eliciting: true,
//                 in_flight: true,
//                 delivered: 0,
//                 delivered_time: now,
//                 first_sent_time: now,
//                 is_app_limited: false,
//                 tx_in_flight: 0,
//                 lost: 0,
//                 has_data: false,
//                 pmtud: false,
//             };

//             r.on_packet_sent(
//                 pkt,
//                 packet::Epoch::Application,
//                 HandshakeStatus::default(),
//                 now,
//                 "",
//             );
//         }

//         let rtt = Duration::from_millis(50);
//         let now = now + rtt;
//         let cwnd_prev = r.cwnd();

//         let mut acked = ranges::RangeSet::default();
//         acked.insert(0..5);

//         assert!(r
//             .on_ack_received(
//                 &acked,
//                 25,
//                 packet::Epoch::Application,
//                 HandshakeStatus::default(),
//                 now,
//                 "",
//             )
//             .is_ok());

//         assert_eq!(r.congestion.bbr2_state.state, BBR2StateMachine::Startup);
//         assert_eq!(r.cwnd(), cwnd_prev + mss * 5);
//         assert_eq!(r.bytes_in_flight, 0);
//         assert_eq!(
//             r.delivery_rate(),
//             ((mss * 5) as f64 / rtt.as_secs_f64()) as u64
//         );
//         assert_eq!(r.congestion.bbr2_state.full_bw, r.delivery_rate());
//     }

//     #[test]
//     fn bbr2_congestion_event() {
//         let mut cfg = crate::Config::new(crate::PROTOCOL_VERSION).unwrap();
//         cfg.set_cc_algorithm(recovery::CongestionControlAlgorithm::BBR2);

//         let mut r = Recovery::new(&cfg);
//         let now = Instant::now();
//         let mss = r.max_datagram_size;

//         // Send 5 packets.
//         for pn in 0..5 {
//             let pkt = Sent {
//                 pkt_num: pn,
//                 frames: smallvec![],
//                 time_sent: now,
//                 time_acked: None,
//                 time_lost: None,
//                 size: mss,
//                 ack_eliciting: true,
//                 in_flight: true,
//                 delivered: 0,
//                 delivered_time: now,
//                 first_sent_time: now,
//                 is_app_limited: false,
//                 tx_in_flight: 0,
//                 lost: 0,
//                 has_data: false,
//                 pmtud: false,
//             };

//             r.on_packet_sent(
//                 pkt,
//                 packet::Epoch::Application,
//                 HandshakeStatus::default(),
//                 now,
//                 "",
//             );
//         }

//         let rtt = Duration::from_millis(50);
//         let now = now + rtt;

//         // Make a packet loss to trigger a congestion event.
//         let mut acked = ranges::RangeSet::default();
//         acked.insert(4..5);

//         // 2 acked, 2 x MSS lost.
//         assert!(r
//             .on_ack_received(
//                 &acked,
//                 25,
//                 packet::Epoch::Application,
//                 HandshakeStatus::default(),
//                 now,
//                 "",
//             )
//             .is_ok());

//         assert!(r.congestion.bbr2_state.in_recovery);

//         // Still in flight: 2, 3.
//         assert_eq!(r.bytes_in_flight, mss * 2);

//         assert_eq!(r.congestion.bbr2_state.newly_acked_bytes, mss);

//         assert_eq!(r.cwnd(), mss * 3);
//     }

//     #[test]
//     fn bbr2_probe_bw() {
//         let mut cfg = crate::Config::new(crate::PROTOCOL_VERSION).unwrap();
//         cfg.set_cc_algorithm(recovery::CongestionControlAlgorithm::BBR2);

//         let mut r = Recovery::new(&cfg);
//         let now = Instant::now();
//         let mss = r.max_datagram_size;

//         let mut pn = 0;

//         // Stop right before filled_pipe=true.
//         for _ in 0..3 {
//             let pkt = Sent {
//                 pkt_num: pn,
//                 frames: smallvec![],
//                 time_sent: now,
//                 time_acked: None,
//                 time_lost: None,
//                 size: mss,
//                 ack_eliciting: true,
//                 in_flight: true,
//                 delivered: r.congestion.delivery_rate.delivered(),
//                 delivered_time: now,
//                 first_sent_time: now,
//                 is_app_limited: false,
//                 tx_in_flight: 0,
//                 lost: 0,
//                 has_data: false,
//                 pmtud: false,
//             };

//             r.on_packet_sent(
//                 pkt,
//                 packet::Epoch::Application,
//                 HandshakeStatus::default(),
//                 now,
//                 "",
//             );

//             pn += 1;

//             let rtt = Duration::from_millis(50);

//             let now = now + rtt;

//             let mut acked = ranges::RangeSet::default();
//             acked.insert(0..pn);

//             assert!(r
//                 .on_ack_received(
//                     &acked,
//                     25,
//                     packet::Epoch::Application,
//                     HandshakeStatus::default(),
//                     now,
//                     "",
//                 )
//                 .is_ok());
//         }

//         // Stop at right before filled_pipe=true.
//         for _ in 0..5 {
//             let pkt = Sent {
//                 pkt_num: pn,
//                 frames: smallvec![],
//                 time_sent: now,
//                 time_acked: None,
//                 time_lost: None,
//                 size: mss,
//                 ack_eliciting: true,
//                 in_flight: true,
//                 delivered: r.congestion.delivery_rate.delivered(),
//                 delivered_time: now,
//                 first_sent_time: now,
//                 is_app_limited: false,
//                 tx_in_flight: 0,
//                 lost: 0,
//                 has_data: false,
//                 pmtud: false,
//             };

//             r.on_packet_sent(
//                 pkt,
//                 packet::Epoch::Application,
//                 HandshakeStatus::default(),
//                 now,
//                 "",
//             );

//             pn += 1;
//         }

//         let rtt = Duration::from_millis(50);
//         let now = now + rtt;

//         let mut acked = ranges::RangeSet::default();

//         // We sent 5 packets, but ack only one, so stay
//         // in Drain state.
//         acked.insert(0..pn - 4);

//         assert!(r
//             .on_ack_received(
//                 &acked,
//                 25,
//                 packet::Epoch::Application,
//                 HandshakeStatus::default(),
//                 now,
//                 "",
//             )
//             .is_ok());

//         assert_eq!(r.congestion.bbr2_state.state, BBR2StateMachine::Drain);
//         assert!(r.congestion.bbr2_state.filled_pipe);
//         assert!(r.congestion.bbr2_state.pacing_gain < 1.0);
//     }

//     #[test]
//     fn bbr2_probe_rtt() {
//         let mut cfg = crate::Config::new(crate::PROTOCOL_VERSION).unwrap();
//         cfg.set_cc_algorithm(recovery::CongestionControlAlgorithm::BBR2);

//         let mut r = Recovery::new(&cfg);
//         let now = Instant::now();
//         let mss = r.max_datagram_size;

//         let mut pn = 0;

//         // At 4th roundtrip, filled_pipe=true and switch to Drain,
//         // but move to ProbeBW immediately because bytes_in_flight is
//         // smaller than BBRInFlight(1).
//         for _ in 0..4 {
//             let pkt = Sent {
//                 pkt_num: pn,
//                 frames: smallvec![],
//                 time_sent: now,
//                 time_acked: None,
//                 time_lost: None,
//                 size: mss,
//                 ack_eliciting: true,
//                 in_flight: true,
//                 delivered: r.congestion.delivery_rate.delivered(),
//                 delivered_time: now,
//                 first_sent_time: now,
//                 is_app_limited: false,
//                 tx_in_flight: 0,
//                 lost: 0,
//                 has_data: false,
//                 pmtud: false,
//             };

//             r.on_packet_sent(
//                 pkt,
//                 packet::Epoch::Application,
//                 HandshakeStatus::default(),
//                 now,
//                 "",
//             );

//             pn += 1;

//             let rtt = Duration::from_millis(50);
//             let now = now + rtt;

//             let mut acked = ranges::RangeSet::default();
//             acked.insert(0..pn);

//             assert!(r
//                 .on_ack_received(
//                     &acked,
//                     25,
//                     packet::Epoch::Application,
//                     HandshakeStatus::default(),
//                     now,
//                     "",
//                 )
//                 .is_ok());
//         }

//         // Now we are in ProbeBW state.
//         assert_eq!(
//             r.congestion.bbr2_state.state,
//             BBR2StateMachine::ProbeBWCRUISE
//         );

//         // After RTPROP_FILTER_LEN (10s), switch to ProbeRTT.
//         let now = now + PROBE_RTT_INTERVAL;

//         let pkt = Sent {
//             pkt_num: pn,
//             frames: smallvec![],
//             time_sent: now,
//             time_acked: None,
//             time_lost: None,
//             size: mss,
//             ack_eliciting: true,
//             in_flight: true,
//             delivered: r.congestion.delivery_rate.delivered(),
//             delivered_time: now,
//             first_sent_time: now,
//             is_app_limited: false,
//             tx_in_flight: 0,
//             lost: 0,
//             has_data: false,
//             pmtud: false,
//         };

//         r.on_packet_sent(
//             pkt,
//             packet::Epoch::Application,
//             HandshakeStatus::default(),
//             now,
//             "",
//         );

//         pn += 1;

//         // Don't update rtprop by giving larger rtt than before.
//         // If rtprop is updated, rtprop expiry check is reset.
//         let rtt = Duration::from_millis(100);
//         let now = now + rtt;

//         let mut acked = ranges::RangeSet::default();
//         acked.insert(0..pn);

//         assert!(r
//             .on_ack_received(
//                 &acked,
//                 25,
//                 packet::Epoch::Application,
//                 HandshakeStatus::default(),
//                 now,
//                 "",
//             )
//             .is_ok());

//         assert_eq!(r.congestion.bbr2_state.state, BBR2StateMachine::ProbeRTT);
//         assert_eq!(r.congestion.bbr2_state.pacing_gain, 1.0);
//     }
// }

// mod init;
// mod pacing;
// mod per_ack;
// mod per_loss;
// mod per_transmit;
