use klavier_core::repeat::AccumTick;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Status {
    Stopped,
    Playing,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum CmdInfo {
    PlayingEnded,
    CurrentLoc {
        seq: usize,
        tick: u32,
        accum_tick: AccumTick,
    },
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Resp {
    Err { seq: usize, msg: String },
    Info { seq: usize, info: CmdInfo },
    Ok { seq: usize, status: Status },
    Aborted,
}
