use std::{mem::Discriminant, sync::{Arc, Mutex, mpsc}, thread};

use klavier_core::repeat::AccumTick;
use midir::MidiOutputConnection;
use tracing::error;
use crate::{play_resp::{CmdInfo, Resp}, player::Player};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Status {
  Stopped,
  Playing { seq: usize, tick: u32, accum_tick: AccumTick },
  MidiOutputError(String),
  Disconnected,
}

pub struct Tracker {
  seq: usize,
  status: Arc<Mutex<Status>>,
}

impl Tracker {
  pub fn new(tick_resolution: u32, midi_conn: MidiOutputConnection) -> Self {
    let mut player = Player::new(tick_resolution, midi_conn);
    let resp_channel: mpsc::Receiver<Resp> = player.take_resp_channel().unwrap();
    let status = Arc::new(Mutex::new(Status::Stopped));
    let moved_status = status.clone();

    thread::spawn(move || {
      loop {
        match resp_channel.recv() {
          Ok(resp) => match resp {
            Resp::Err { seq: _seq, msg } => {
              *moved_status.lock().unwrap() = Status::MidiOutputError(msg);
            }
            Resp::Info { seq: _seq, info } => match info {
                CmdInfo::PlayingEnded => {
                  *moved_status.lock().unwrap() = Status::Stopped;
                },
                CmdInfo::CurrentLoc { seq, tick, accum_tick } => {
                },
            },
            Resp::Ok { seq, status } => {
              *moved_status.lock().unwrap() = match status {
                crate::play_resp::Status::Stopped => Status::Stopped,
                crate::play_resp::Status::Playing => todo!(),
              }
            },
            Resp::Aborted => {
              *moved_status.lock().unwrap() = Status::Disconnected;
            },
          },
          Err(err) => {
            println!("recv error: {:?}", err);
            error!("Player receiver disconnected {:?}", err);
            *moved_status.lock().unwrap() = Status::Disconnected;
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