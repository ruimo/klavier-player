use std::{mem::Discriminant, sync::{Arc, Mutex, mpsc}, thread};

use klavier_core::repeat::AccumTick;
use midir::MidiOutputConnection;
use tracing::error;
use crate::{play_resp::{CmdInfo, Resp}, player::Player, player_status::PlayerStatus};

pub struct Tracker {
  seq: usize,
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
                },
            },
            Resp::Ok { seq, status } => {
              *moved_status.lock().unwrap() = match status {
                crate::play_resp::Status::Stopped => PlayerStatus::Stopped,
                crate::play_resp::Status::Playing => todo!(),
              }
            },
            Resp::Aborted => {
              *moved_status.lock().unwrap() = PlayerStatus::Disconnected;
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
      status,
    }
  }
}