use klavier_core::repeat::AccumTick;

pub enum PlayerStatus {
  Stopped,
  Playing { seq: usize, tick: u32, accum_tick: AccumTick },
  MidiOutputError(String),
  Disconnected,
}
