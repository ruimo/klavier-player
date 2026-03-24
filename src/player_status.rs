use klavier_core::repeat::AccumTick;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlayerStatus {
  Stopped,
  StartingPlay { seq: usize },
  Playing { seq: usize, tick: u32, accum_tick: AccumTick },
  MidiOutputError(String),
  Disconnected,
}
