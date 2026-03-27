use std::{rc::Rc, sync::{Arc, Mutex, mpsc}, thread};

use error_stack::Report;
use klavier_core::{bar::Bar, ctrl_chg::CtrlChg, global_repeat::RenderRegionWarning, key::Key, note::Note, play_start_tick::PlayStartTick, project::ModelChangeMetadata, rhythm::Rhythm, tempo::Tempo};
use klavier_helper::{bag_store::BagStore, store::Store};
use midir::MidiOutputConnection;
use tracing::error;
use crate::{play_error::PlayError, play_resp::{CmdInfo, Resp}, player::Player, player_status::PlayerStatus};

pub struct Tracker {
  seq: usize,
  player: Player,
  status: Arc<Mutex<PlayerStatus>>,
}

impl Tracker {
  pub fn new(tick_resolution: u32, midi_conn: MidiOutputConnection) -> Self {
    let mut player = Player::new(tick_resolution, midi_conn);
    let resp_channel: mpsc::Receiver<Resp> = player.take_resp_channel().unwrap();
    let status = Arc::new(Mutex::new(PlayerStatus::Stopped));
    let moved_status = status.clone();

    thread::spawn(move || {
      loop {
        match resp_channel.recv() {
          Ok(resp) => match resp {
            Resp::Err { seq: _seq, msg } => {
              *moved_status.lock().unwrap() = PlayerStatus::MidiOutputError(msg);
            }
            Resp::Info { seq: _seq, info } => match info {
                CmdInfo::PlayingEnded => {
                  *moved_status.lock().unwrap() = PlayerStatus::Stopped;
                },
                CmdInfo::CurrentLoc { seq, tick, accum_tick } => {
                  *moved_status.lock().unwrap() = PlayerStatus::Playing { seq, tick, accum_tick };
                },
            },
            Resp::Aborted => {
              *moved_status.lock().unwrap() = PlayerStatus::Disconnected;
              break;
            },
          },
          Err(err) => {
            println!("recv error: {:?}", err);
            error!("Player receiver disconnected {:?}", err);
            *moved_status.lock().unwrap() = PlayerStatus::Disconnected;
            break;
          },
        }
      }      
    });

    Self {
      seq: 0,
      player,
      status,
    }
  }

  pub fn status(&self) -> PlayerStatus {
    self.status.lock().unwrap().clone()
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
    let result = self.player.play(
      seq, play_start_loc, top_rhythm, top_key, note_repo, bar_repo, tempo_repo, dumper_repo, soft_repo,
    )?;
    *self.status.lock().unwrap() = PlayerStatus::StartingPlay { seq };
    self.seq += 1;
    
    Ok(result)
  }

  pub fn stop(&mut self, seq: usize) -> Result<(), Report<PlayError>> {
    self.player.stop(seq)
  }

  /// Gracefully terminate the tracker.
  /// This cleanly shuts down the player thread without generating error messages.
  pub fn terminate(&mut self) -> Result<(), Report<PlayError>> {
    self.player.terminate()
  }
}