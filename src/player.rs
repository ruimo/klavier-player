use std::{rc::Rc, sync::{mpsc::{Receiver, SyncSender, sync_channel}}};

use error_stack::Report;
use klavier_core::{bar::Bar, ctrl_chg::CtrlChg, duration::Duration, global_repeat::RenderRegionWarning, key::Key, midi_events::{MidiEvents, PlayData, create_midi_events}, note::Note, play_start_tick::PlayStartTick, project::ModelChangeMetadata, rhythm::Rhythm, tempo::{Tempo, TempoValue}};
use klavier_helper::{bag_store::BagStore, store::Store};
use midir::MidiOutputConnection;
use thread_priority::{ThreadBuilder, ThreadPriority};
use tracing::error;
use crate::player_task;
use crate::{play_cmd::{Cmd, PlayCmdData}, play_error::PlayError, play_resp::Resp, midi_output::RealMidiOutput};

pub struct Player {
    pub tick_resolution: u32,
    #[allow(dead_code)]
    thread: std::thread::JoinHandle<()>,
    cmd_channel: SyncSender<Cmd>,
    #[allow(dead_code)]
    resp_channel: Option<Receiver<Resp>>,
}

impl Player {
    /// Create a new Player with a real MIDI connection
    pub fn new(tick_resolution: u32, midi_conn: MidiOutputConnection) -> Self {
        Self::new_with_output(tick_resolution, RealMidiOutput::new(midi_conn))
    }

    /// Create a new Player with a custom MIDI output implementation (for testing)
    pub fn new_with_output(tick_resolution: u32, midi_output: impl crate::midi_output::MidiOutput + 'static) -> Self {
        let (cmd_sender, cmd_receiver) = sync_channel::<Cmd>(64);
        let (resp_sender, resp_receiver) = sync_channel::<Resp>(64);

        let thread: std::thread::JoinHandle<()> = ThreadBuilder::default()
        .name("Klavier Player")
        .priority(ThreadPriority::Max)
        .spawn(move |result| {
            if let Err(e) = result {
                error!("Cannot set thread priority: {:?}", e);
            }
            
            player_task::run(midi_output, cmd_receiver, resp_sender);
        }).unwrap_or_else(|e| {
            panic!("Cannot spawn thread: {:?}", e);
        });

        Self {
            tick_resolution,
            thread,
            cmd_channel: cmd_sender,
            resp_channel: Option::Some(resp_receiver),
        }
    }
    
    pub fn take_resp_channel(&mut self) -> Option<Receiver<Resp>> {
        self.resp_channel.take()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn play(
        &mut self,
        seq: usize,
        play_start_loc: Option<PlayStartTick>,
        top_rhythm: Rhythm,
        top_key: Key,
        note_repo: &BagStore<u32, Rc<Note>, ModelChangeMetadata>,
        bar_repo: &Store<u32, Bar, ModelChangeMetadata>,
        tempo_repo: &Store<u32, Tempo, ModelChangeMetadata>,
        dumper_repo: &Store<u32, CtrlChg, ModelChangeMetadata>,
        soft_repo: &Store<u32, CtrlChg, ModelChangeMetadata>,
    ) -> Result<Vec<RenderRegionWarning>, Report<PlayError>> {
        let (events, warnings): (MidiEvents, Vec<RenderRegionWarning>) = create_midi_events(
            top_rhythm,
            top_key,
            note_repo,
            bar_repo,
            tempo_repo,
            dumper_repo,
            soft_repo,
        )
        .map_err(|e| Report::new(PlayError::RenderError(e.current_context().clone())))?;

        let cycles_by_tick: Store<u32, (TempoValue, u64), ()>
          = events.cycles_by_accum_tick(crate::SAMPLING_RATE_U32, self.tick_resolution);

        let start_accum_tick: u32 = match play_start_loc {
            None => 0,
            Some(loc) => {
                match loc.to_accum_tick(&events.chunks) {
                    Ok(tick) => tick,
                    Err(err) => Err(PlayError::PlayStartTickError(err))?,
                }
            }
        };
        let start_cycle: u64 = {
            match cycles_by_tick.just_before(start_accum_tick).next() {
                Some((t, (tempo, cycles))) =>
                    *cycles + MidiEvents::tick_to_cycle(start_accum_tick - *t, crate::SAMPLING_RATE_U32, tempo.as_u16(), Duration::TICK_RESOLUTION as u32),
                None =>
                    MidiEvents::tick_to_cycle(start_accum_tick, crate::SAMPLING_RATE_U32, TempoValue::default().as_u16(), Duration::TICK_RESOLUTION as u32),
            }
        };
        let play_data: PlayData = events.to_play_data(cycles_by_tick, crate::SAMPLING_RATE_U32, Duration::TICK_RESOLUTION as u32);
        
        // Send play command to player task
        let play_cmd = Cmd::Play(PlayCmdData {
            seq,
            play_data,
            start_cycle,
        });
        
        self.cmd_channel.send(play_cmd)
            .map_err(|e| Report::new(PlayError::SendCommandError(format!("{:?}", e))))?;
        
        Ok(warnings)
    }
    
    pub fn stop(&mut self, seq: usize) -> Result<(), Report<PlayError>> {
        let stop_cmd = Cmd::Stop { seq };
        
        self.cmd_channel.send(stop_cmd)
            .map_err(|e| Report::new(PlayError::SendCommandError(format!("{:?}", e))))?;
        
        Ok(())
    }
    
    /// Gracefully terminate the player task.
    /// This sends a Terminate command to cleanly shut down the player thread.
    pub fn terminate(&mut self) -> Result<(), Report<PlayError>> {
        let terminate_cmd = Cmd::Terminate;
        
        self.cmd_channel.send(terminate_cmd)
            .map_err(|e| Report::new(PlayError::SendCommandError(format!("{:?}", e))))?;
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::midi_output::MockMidiOutput;

    #[test]
    fn test_player_creation_with_mock() {
        let mock_output = MockMidiOutput::new();
        let tick_resolution = 480;
        let player = Player::new_with_output(tick_resolution, mock_output);
        
        // Verify player properties
        assert_eq!(player.tick_resolution, tick_resolution);
        
        // Verify channels are working by sending a command
        // This also verifies the thread was spawned successfully
        let result = player.cmd_channel.try_send(Cmd::Stop { seq: 0 });
        assert!(result.is_ok(), "Command channel should be functional");
    }
}
