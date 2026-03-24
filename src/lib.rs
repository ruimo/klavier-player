pub mod player;
pub mod play_error;
pub mod midi_output;
pub mod player_status;
mod play_cmd;
mod play_resp;
mod player_task;
pub mod tracker;

// Audio cycle duration in nanoseconds (1 microsecond = 1000 nanoseconds)
pub(crate) const CYCLE_DURATION_NANOS: u64 = 1_000;

// Sampling rate calculated from cycle duration
// 1 second = 1_000_000_000 nanoseconds
// Sampling rate = 1_000_000_000 / 1_000 = 1_000_000 Hz
pub(crate) const SAMPLING_RATE_U32: u32 = (1_000_000_000 / CYCLE_DURATION_NANOS) as u32;