use klavier_core::repeat::AccumTick;

pub enum PlayerState {
    Idle,
    #[allow(dead_code)]
    Playing { seq: usize, tick: u32, accum_tick: AccumTick },
    Aborted,
}
