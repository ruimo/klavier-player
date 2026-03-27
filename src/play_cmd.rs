use klavier_core::midi_events::PlayData;

#[derive(Clone)]
pub struct PlayCmdData {
    pub seq: usize,
    pub play_data: PlayData,
    pub start_cycle: u64,
}

#[derive(Clone)]
pub enum Cmd {
    Play(PlayCmdData),
    Stop {
        seq: usize,
    },
    Terminate,
}
