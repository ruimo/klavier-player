use std::sync::mpsc::{Receiver, SyncSender, TryRecvError};
use std::time::{Duration, Instant};
use tracing::{error, info, warn};
use crate::play_resp::Status;
use crate::{play_cmd::{Cmd, PlayCmdData}, play_resp::{Resp, CmdInfo}, midi_output::MidiOutput};

const CYCLE_DURATION_NANOS: u64 = 1_000; // 1us = 1000ns

/// Trait for abstracting time operations to enable testing
trait Clock {
  fn now(&self) -> Instant;
  fn sleep(&self, duration: Duration);
}

/// System clock implementation using real time
struct SystemClock;

impl Clock for SystemClock {
  fn now(&self) -> Instant {
    Instant::now()
  }
  
  fn sleep(&self, duration: Duration) {
    spin_sleep::sleep(duration);
  }
}

struct PlayState {
  play_cmd_data: PlayCmdData,
  cycle_offset: u64,
  start_timestamp: Instant,
  current_idx: usize,
}

/// PlayerTask manages the player loop state and operations
struct PlayerTask<M: MidiOutput, C: Clock> {
  play_state: Option<PlayState>,
  midi_conn: M,
  cmd_receiver: Receiver<Cmd>,
  resp_sender: SyncSender<Resp>,
  clock: C,
}

impl<M: MidiOutput, C: Clock> PlayerTask<M, C> {
  /// Consumes the PlayerTask and returns its internal components.
  ///
  /// # Returns
  /// A tuple containing:
  /// - `play_state`: The current play state (if any)
  /// - `midi_conn`: The MIDI connection
  /// - `cmd_receiver`: The command receiver
  /// - `resp_sender`: The response sender
  /// - `clock`: The clock implementation
  fn take(self) -> (
    Option<PlayState>,
    M,
    Receiver<Cmd>,
    SyncSender<Resp>,
    C,
  ) {
    (
      self.play_state,
      self.midi_conn,
      self.cmd_receiver,
      self.resp_sender,
      self.clock,
    )
  }
}

impl<M: MidiOutput, C: Clock> PlayerTask<M, C> {
  fn new(
    midi_conn: M,
    cmd_receiver: Receiver<Cmd>,
    resp_sender: SyncSender<Resp>,
    clock: C,
  ) -> Self {
    Self {
      play_state: None,
      midi_conn,
      cmd_receiver,
      resp_sender,
      clock,
    }
  }

  /// Executes one iteration of the player task loop.
  /// Returns `true` to continue the loop, `false` to exit.
  fn run_iteration(&mut self) -> bool {
    if let Some(play_state) = self.play_state.take() {
      // If playing, perform playback
      self.handle_playing_state(play_state);
    } else {
      // If not playing, wait for a command (blocking)
      self.handle_idle_state();
    }
    self.play_state.is_some()
  }

  fn handle_idle_state(&mut self) {
    match self.cmd_receiver.recv() {
      Ok(Cmd::Play(play_cmd_data)) => {
        info!("Play command received: seq={}, start_cycle={}", play_cmd_data.seq, play_cmd_data.start_cycle);
        let cycle_offset = play_cmd_data.start_cycle;
        let start_idx = match play_cmd_data.play_data.midi_data.find(&cycle_offset) {
          Ok(idx) => idx,
          Err(idx) => idx,
        };
        self.play_state = Some(PlayState {
          play_cmd_data,
          cycle_offset,
          start_timestamp: self.clock.now(),
          current_idx: start_idx,
        });
        self.resp(Resp::Ok { seq: self.play_state.as_ref().unwrap().play_cmd_data.seq, status: Status::Playing });
      }
      Ok(Cmd::Stop { seq }) => {
        error!("Already stopped, just ignored: seq={}", seq);
        self.resp(Resp::Ok { seq, status: Status::Stopped });
      }
      Err(e) => {
        error!("Player task cannot receive command: {:?}", e);
        self.play_state.take();
      }
    }
  }

  fn handle_playing_state(&mut self, mut play_state: PlayState) {
    match self.play_cycle(&mut play_state) {
      PlayResult::Continue => {
        self.play_state = Some(play_state);
      }
      PlayResult::Finished => {
        info!("Playback finished: seq={}", play_state.play_cmd_data.seq);
        self.resp(Resp::Info {
          seq: play_state.play_cmd_data.seq,
          info: CmdInfo::PlayingEnded
        });
      }
      PlayResult::Stopped => {
        info!("Playback stopped: seq={}", play_state.play_cmd_data.seq);
        self.resp(Resp::Ok { seq: play_state.play_cmd_data.seq, status: Status::Stopped });
      }
      PlayResult::Interrupted(new_play_cmd_data) => {
        info!("Playback interrupted: old_seq={}, new_seq={}", play_state.play_cmd_data.seq, new_play_cmd_data.seq);
        let cycle_offset = new_play_cmd_data.start_cycle;
        let start_idx = match new_play_cmd_data.play_data.midi_data.find(&cycle_offset) {
          Ok(idx) => idx,
          Err(idx) => idx,
        };
        self.play_state = Some(PlayState {
          play_cmd_data: *new_play_cmd_data,
          cycle_offset,
          start_timestamp: self.clock.now(),
          current_idx: start_idx,
        });
        self.resp(Resp::Ok { seq: self.play_state.as_ref().unwrap().play_cmd_data.seq, status: Status::Playing });
      }
      PlayResult::Error(msg) => {
        let seq = play_state.play_cmd_data.seq;
        error!("Playback error(seq={}): {}", seq, msg);
        self.resp(Resp::Err { seq, msg });
      }
    }
  }

  fn play_cycle(&mut self, play_state: &mut PlayState) -> PlayResult {
    let seq = play_state.play_cmd_data.seq;
    
    if let Some(result) = self.process_command(seq) {
      return result;
    }
    
    // Calculate current playback position from elapsed time
    let current_position: u64 = self.calculate_current_position(play_state);
    
    // Find the index of events to process
    let next_idx = match play_state.play_cmd_data.play_data.midi_data.find(&current_position) {
      Ok(idx) => idx,
      Err(idx) => idx,
    };
    
    // Send MIDI events
    if let Err(result) = self.send_midi_events(play_state, next_idx) {
      return result;
    }
    
    // Wait for next event or finish
    self.wait_for_next_event(play_state, next_idx)
  }

  /// Process incoming commands (Stop or Play) during playback.
  /// Returns Some(PlayResult) if a command was received that should interrupt playback,
  /// or None if playback should continue normally.
  fn process_command(&self, current_seq: usize) -> Option<PlayResult> {
    match self.cmd_receiver.try_recv() {
      Ok(Cmd::Stop { seq: stop_seq }) => {
        info!("Stop command received: seq={}", stop_seq);
        if stop_seq == current_seq {
          Some(PlayResult::Stopped)
        } else {
          warn!("Stop command seq mismatch: expected={}, got={}", current_seq, stop_seq);
          None
        }
      }
      Ok(Cmd::Play(new_play_cmd_data)) => {
        info!("Play command received while already playing, interrupting current playback");
        Some(PlayResult::Interrupted(Box::new(new_play_cmd_data)))
      }
      Err(TryRecvError::Empty) => {
        // No command, continue playing
        None
      }
      Err(TryRecvError::Disconnected) => {
        let msg = "Command channel disconnected".to_owned();
        error!(msg);
        Some(PlayResult::Error(msg))
      }
    }
  }

  /// Calculate the current playback position based on elapsed time
  fn calculate_current_position(&self, play_state: &PlayState) -> u64 {
    let now = self.clock.now();
    let elapsed = now.duration_since(play_state.start_timestamp);
    let elapsed_cycles = elapsed.as_nanos() / CYCLE_DURATION_NANOS as u128;
    play_state.cycle_offset + elapsed_cycles as u64
  }

  /// Send MIDI events from current_idx up to (but not including) next_idx
  fn send_midi_events(
    &mut self,
    play_state: &mut PlayState,
    next_idx: usize,
  ) -> Result<(), PlayResult> {
    let mut idx = play_state.current_idx;
    
    while idx < next_idx {
      let (_cycle, midi_msgs) = &play_state.play_cmd_data.play_data.midi_data[idx];
      for message in midi_msgs {
        if let Err(e) = self.midi_conn.send(message) {
          let msg = format!("Failed to send MIDI message: {:?}", e);
          error!(msg);
          return Err(PlayResult::Error(msg));
        }
      }
      idx += 1;
    }
    
    play_state.current_idx = next_idx;
    Ok(())
  }

  /// Calculate and execute sleep until the next event
  fn wait_for_next_event(
    &self,
    play_state: &PlayState,
    next_idx: usize,
  ) -> PlayResult {
    if play_state.play_cmd_data.play_data.midi_data.len() <= next_idx {
      PlayResult::Finished
    } else {
      let now = self.clock.now();
      let (next_cycle, _) = play_state.play_cmd_data.play_data.midi_data[next_idx];
      let cycles_from_start = next_cycle.saturating_sub(play_state.cycle_offset);
      let target_time = play_state.start_timestamp + Duration::from_nanos(cycles_from_start * CYCLE_DURATION_NANOS);
      let sleep_duration = target_time - now;
      self.clock.sleep(sleep_duration);
      PlayResult::Continue
    }
  }

  fn resp(&self, resp: Resp) {
    if let Err(e) = self.resp_sender.send(resp) {
      println!("Cannot send response: {:?}.", e);
    }
  }
}

pub fn run(midi_conn: impl MidiOutput, cmd_receiver: Receiver<Cmd>, resp_sender: SyncSender<Resp>) {
  let clock = SystemClock;
  let mut player_task = PlayerTask::new(midi_conn, cmd_receiver, resp_sender, clock);
  
  while player_task.run_iteration() {
    // Eternal loop
  }

  let (_, _, _, resp_sender, _) = player_task.take();
  match resp_sender.send(Resp::Aborted) {
    Ok(_) => {},
    Err(e) => error!("Midi player aborted: {:?}", e),
  }
}

enum PlayResult {
  Continue,
  Finished,
  Stopped,
  Interrupted(Box<PlayCmdData>),
  Error(String),
}

#[cfg(test)]
mod tests {
    use klavier_core::duration::Dots;
    use klavier_core::octave::Octave;
    use klavier_core::pitch::Pitch;
    use klavier_core::sharp_flat::SharpFlat;
    use klavier_core::solfa::Solfa;
    use klavier_core::velocity::Velocity;

    use super::*;
    use crate::midi_output::MockMidiOutput;
    use crate::play_cmd::{Cmd, PlayCmdData};
    use crate::play_resp::Resp;
    use std::sync::mpsc;
    use std::cell::RefCell;

    /// Mock clock for testing that allows controlling time
    struct MockClock {
        times: RefCell<Vec<Instant>>,
        sleep_calls: RefCell<Vec<Duration>>,
    }

    impl MockClock {
        fn new(times: Vec<Instant>) -> Self {
            Self {
                times: RefCell::new(times),
                sleep_calls: RefCell::new(Vec::new()),
            }
        }

        fn sleep_calls(&self) -> Vec<Duration> {
            self.sleep_calls.borrow().clone()
        }
    }

    impl Clock for MockClock {
        fn now(&self) -> Instant {
            let mut times = self.times.borrow_mut();
            if times.is_empty() {
                panic!("MockClock: no more times available");
            }
            times.remove(0)
        }

        fn sleep(&self, duration: Duration) {
            self.sleep_calls.borrow_mut().push(duration);
        }
    }

    /// Helper function to create an empty PlayData for testing
    /// This uses the actual klavier_core API to create valid but empty test data
    fn create_empty_play_data() -> klavier_core::midi_events::PlayData {
        use klavier_core::midi_events::create_midi_events;
        use klavier_core::rhythm::Rhythm;
        use klavier_core::key::Key;
        use klavier_helper::bag_store::BagStore;
        use klavier_helper::store::Store;
        
        // Create minimal test data
        let rhythm = Rhythm::new(4, 4);
        let key = Key::default();
        let note_repo = BagStore::new(true);
        let bar_repo = Store::new(true);
        let tempo_repo = Store::new(true);
        let dumper_repo = Store::new(true);
        let soft_repo = Store::new(true);
        
        let (events, _warnings) = create_midi_events(
            rhythm,
            key,
            &note_repo,
            &bar_repo,
            &tempo_repo,
            &dumper_repo,
            &soft_repo,
        ).unwrap();
        
        let cycles_by_tick = events.cycles_by_accum_tick(
            1_000_000,
            klavier_core::duration::Duration::TICK_RESOLUTION as u32
        );
        events.to_play_data(
            cycles_by_tick,
            1_000_000,
            klavier_core::duration::Duration::TICK_RESOLUTION as u32
        )
    }

    /// Helper function to create PlayData with a single note for testing
    /// Creates a C4 note (MIDI 60) that plays for a quarter note duration
    fn create_single_note_play_data() -> klavier_core::midi_events::PlayData {
        use klavier_core::midi_events::create_midi_events;
        use klavier_core::rhythm::Rhythm;
        use klavier_core::key::Key;
        use klavier_core::note::Note;
        use klavier_helper::bag_store::BagStore;
        use klavier_helper::store::Store;
        use klavier_core::project::ModelChangeMetadata;
        use std::rc::Rc;
        
        // Create minimal test data with one note
        let rhythm = Rhythm::new(4, 4);
        let key = Key::default();
        
        // Create a note repository with one note
        let mut note_repo = BagStore::new(true);
        
        // Create a Note using Note::new() - C4 (pitch 60), quarter note duration (480 ticks)
        let note = Note {
           base_start_tick: 0,
           pitch: Pitch::new(Solfa::C, Octave::Oct4, SharpFlat::Null),
           duration: klavier_core::duration::Duration::new(klavier_core::duration::Numerator::Half, klavier_core::duration::Denominator::default(), Dots::ZERO),
           base_velocity: Velocity::new(100),
           ..Default::default()
        };
        
        note_repo.add(1, Rc::new(note), ModelChangeMetadata::default());
        
        let bar_repo = Store::new(true);
        let tempo_repo = Store::new(true);
        let dumper_repo = Store::new(true);
        let soft_repo = Store::new(true);
        
        let (events, _warnings) = create_midi_events(
            rhythm,
            key,
            &note_repo,
            &bar_repo,
            &tempo_repo,
            &dumper_repo,
            &soft_repo,
        ).unwrap();
        
        let cycles_by_tick = events.cycles_by_accum_tick(
            1_000_000,
            klavier_core::duration::Duration::TICK_RESOLUTION as u32
        );
        events.to_play_data(
            cycles_by_tick,
            1_000_000,
            klavier_core::duration::Duration::TICK_RESOLUTION as u32
        )
    }

    #[test]
    fn test_play_single_note() {
        // Setup
        let midi_output = MockMidiOutput::new();
        let (_cmd_sender, cmd_receiver) = mpsc::sync_channel::<Cmd>(1);
        let (_resp_sender, _resp_receiver) = mpsc::sync_channel::<Resp>(1);

        // Create test PlayData with a single note
        let play_data = create_single_note_play_data();
        let play_cmd_data = PlayCmdData {
            seq: 1,
            play_data,
            start_cycle: 0,
        };

        // Create PlayState
        let base_time = Instant::now();
        let mut play_state = PlayState {
            play_cmd_data,
            cycle_offset: 0,
            start_timestamp: base_time,
            current_idx: 0,
        };

        // Create mock clock with times for:
        // 1. Initial position calculation - advance time so current_position > 0
        // 2. Position after sending MIDI messages (for recalculation)
        // 3. For calculating sleep duration before next event
        let clock = MockClock::new(vec![
            base_time + Duration::from_micros(2),   // Initial now() call - advance past cycle 0
            base_time + Duration::from_micros(3),   // After sending MIDI for recalculation
            base_time + Duration::from_micros(4),   // For sleep duration calculation
        ]);

        // Execute play_cycle
        let mut player_task = PlayerTask::new(midi_output, cmd_receiver, _resp_sender, clock);
        let result = player_task.play_cycle(&mut play_state);
        
        // Take back the components for verification
        let (_, midi_output, _, _, clock) = player_task.take();

        // Verify result - should continue (waiting for note off event)
        match result {
            PlayResult::Continue => {
                // Expected - there should be more events (note off)
            }
            PlayResult::Finished => panic!("Got PlayResult::Finished, expected Continue"),
            PlayResult::Stopped => panic!("Got PlayResult::Stopped"),
            PlayResult::Interrupted(_) => panic!("Got PlayResult::Interrupted"),
            PlayResult::Error(_msg) => panic!("Got PlayResult::Error"),
        }

        // Verify MIDI messages were sent
        let sent_messages = &midi_output.sent_messages;
        assert!(!sent_messages.is_empty(), "Expected MIDI messages to be sent");
        
        // First message should be Note On (0x90 = note on channel 0)
        // MIDI message format: [status, pitch, velocity]
        assert_eq!(sent_messages[0][0] & 0xF0, 0x90, "First message should be Note On");
        assert_eq!(sent_messages[0][1], 72, "Pitch should be 72 (C5 in klavier-core)");
        assert_eq!(sent_messages[0][2], 100, "Velocity should be 100");
        
        // Verify sleep was called (waiting for next event)
        let sleep_calls = clock.sleep_calls();
        assert_eq!(sleep_calls.len(), 1, "Expected one sleep call");
    }

    #[test]
    fn test_run_iteration_play_command_when_idle() {
        // Setup
        let midi_output = MockMidiOutput::new();
        let (cmd_sender, cmd_receiver) = mpsc::sync_channel(1);
        let (resp_sender, resp_receiver) = mpsc::sync_channel(1);

        // Create test PlayData
        let play_data = create_empty_play_data();

        let play_cmd_data = PlayCmdData {
            seq: 1,
            play_data,
            start_cycle: 0,
        };

        // Send Play command
        cmd_sender.send(Cmd::Play(play_cmd_data)).unwrap();

        // Create mock clock with a single time point
        let base_time = Instant::now();
        let clock = MockClock::new(vec![base_time]);

        // Create PlayerTask and execute run_iteration
        let mut player_task = PlayerTask::new(midi_output, cmd_receiver, resp_sender, clock);
        let should_continue = player_task.run_iteration();

        // Verify results
        assert!(should_continue, "run_iteration should return true to continue");
        
        // Verify that play state was created
        assert!(player_task.play_state.is_some(), "PlayState should be created");
        let play_state = player_task.play_state.as_ref().unwrap();
        assert_eq!(play_state.play_cmd_data.seq, 1);
        assert_eq!(play_state.cycle_offset, 0);

        // Verify response was sent
        let resp = resp_receiver.try_recv().unwrap();
        assert_eq!(resp, Resp::Ok { seq: 1, status: Status::Playing });
    }

    #[test]
    fn test_play_cycle_with_mock_clock() {
        // Setup
        let midi_output = MockMidiOutput::new();
        let (_cmd_sender, cmd_receiver) = mpsc::sync_channel::<Cmd>(1);
        let (_resp_sender, _resp_receiver) = mpsc::sync_channel::<Resp>(1);

        // Create test PlayData (empty, so it will finish immediately)
        let play_data = create_empty_play_data();
        let play_cmd_data = PlayCmdData {
            seq: 1,
            play_data,
            start_cycle: 0,
        };

        // Create PlayState
        let base_time = Instant::now();
        let mut play_state = PlayState {
            play_cmd_data,
            cycle_offset: 0,
            start_timestamp: base_time,
            current_idx: 0,
        };

        // Create mock clock - need one time call for the initial position calculation
        let clock = MockClock::new(vec![base_time]);

        // Execute play_cycle
        let (_resp_sender, _resp_receiver) = mpsc::sync_channel::<Resp>(1);
        let mut player_task = PlayerTask::new(midi_output, cmd_receiver, _resp_sender, clock);
        let result = player_task.play_cycle(&mut play_state);
        
        // Take back the components for verification
        let (_, _, _, _, clock) = player_task.take();

        // Verify result - empty PlayData should finish immediately
        match result {
            PlayResult::Finished => {
                // Expected - empty PlayData has no events
            }
            PlayResult::Continue => panic!("Got PlayResult::Continue"),
            PlayResult::Stopped => panic!("Got PlayResult::Stopped"),
            PlayResult::Interrupted(_) => panic!("Got PlayResult::Interrupted"),
            PlayResult::Error(_msg) => panic!("Got PlayResult::Error"),
        }

        // Verify no sleep was called (finished before sleep)
        let sleep_calls = clock.sleep_calls();
        assert_eq!(sleep_calls.len(), 0, "Expected no sleep calls for empty PlayData");
    }

    #[test]
    fn test_play_cycle_stop_command() {
        // Setup
        let midi_output = MockMidiOutput::new();
        let (cmd_sender, cmd_receiver) = mpsc::sync_channel::<Cmd>(1);

        // Create test PlayData
        let play_data = create_empty_play_data();
        let play_cmd_data = PlayCmdData {
            seq: 1,
            play_data,
            start_cycle: 0,
        };

        // Create PlayState
        let base_time = Instant::now();
        let mut play_state = PlayState {
            play_cmd_data,
            cycle_offset: 0,
            start_timestamp: base_time,
            current_idx: 0,
        };

        // Send stop command
        cmd_sender.send(Cmd::Stop { seq: 1 }).unwrap();

        // Create mock clock (won't be used since we stop immediately)
        let clock = MockClock::new(vec![]);

        // Execute play_cycle
        let (_resp_sender, _resp_receiver) = mpsc::sync_channel::<Resp>(1);
        let mut player_task = PlayerTask::new(midi_output, cmd_receiver, _resp_sender, clock);
        let result = player_task.play_cycle(&mut play_state);

        // Verify result - play_cycle should return Stopped
        // Note: Response sending is handled by handle_playing_state, not play_cycle
        match result {
            PlayResult::Stopped => {
                // Expected
            }
            _ => panic!("Expected PlayResult::Stopped"),
        }
    }

    #[test]
    fn test_play_cycle_note_on_and_off_boundary() {
        let midi_output = MockMidiOutput::new();
        let (_cmd_sender, cmd_receiver) = mpsc::sync_channel::<Cmd>(1);
        let (_resp_sender, _resp_receiver) = mpsc::sync_channel::<Resp>(1);

        let play_data = create_single_note_play_data();
        let note_off_cycle = play_data.midi_data[1].0;
        
        let play_cmd_data = PlayCmdData {
            seq: 1,
            play_data,
            start_cycle: 0,
        };

        // Create PlayState
        let base_time = Instant::now();
        let mut play_state = PlayState {
            play_cmd_data,
            cycle_offset: 0,
            start_timestamp: base_time,
            current_idx: 0,
        };

        // Test 1: At cycle 1 (just past cycle 0) - should send Note On
        let clock1 = MockClock::new(vec![
            base_time + Duration::from_nanos(CYCLE_DURATION_NANOS + 1),  // current_position = 1
            base_time + Duration::from_nanos(CYCLE_DURATION_NANOS + 2),
        ]);

        let (_resp_sender, _resp_receiver) = mpsc::sync_channel::<Resp>(1);
        let mut player_task1 = PlayerTask::new(midi_output, cmd_receiver, _resp_sender, clock1);
        let result1 = player_task1.play_cycle(&mut play_state);
        
        // Take back the components for next test
        let (_, midi_output, cmd_receiver, _resp_sender, clock1) = player_task1.take();

        assert!(matches!(result1, PlayResult::Continue), "Should continue after Note On");
        assert_eq!(midi_output.sent_messages.len(), 1, "Should have sent Note On");
        assert_eq!(midi_output.sent_messages[0][0] & 0xF0, 0x90, "Should be Note On");
        assert_eq!(midi_output.sent_messages[0][2], 100, "Velocity should be 100 (Note On)");
        
        // Verify sleep was called with correct duration (until note_off_cycle)
        let sleep_calls = clock1.sleep_calls();
        assert_eq!(sleep_calls.len(), 1, "Should have called sleep once");
        let expected_sleep_nanos = note_off_cycle * CYCLE_DURATION_NANOS - (CYCLE_DURATION_NANOS + 2);
        assert_eq!(sleep_calls[0].as_nanos(), expected_sleep_nanos as u128,
            "Sleep duration should be {} nanoseconds", expected_sleep_nanos);

        // Test 2: At exactly note_off_cycle - should NOT send Note Off yet
        let clock2 = MockClock::new(vec![
            base_time + Duration::from_nanos(note_off_cycle * CYCLE_DURATION_NANOS),
            base_time + Duration::from_nanos(note_off_cycle * CYCLE_DURATION_NANOS + 1),
        ]);

        let mut player_task2 = PlayerTask::new(midi_output, cmd_receiver, _resp_sender, clock2);
        let result2 = player_task2.play_cycle(&mut play_state);
        
        // Take back the components for next test
        let (_, midi_output, cmd_receiver, _resp_sender, clock2) = player_task2.take();

        assert!(matches!(result2, PlayResult::Continue), "Should continue at exact boundary");
        assert_eq!(midi_output.sent_messages.len(), 1, "Should NOT have sent Note Off at exact boundary");
        
        // Verify sleep was called (waiting for next event which is at note_off_cycle)
        let sleep_calls = clock2.sleep_calls();
        assert_eq!(sleep_calls.len(), 1, "Should have called sleep once");
        // At exact boundary, sleep should be 0 or very small (already at the event time)
        assert_eq!(sleep_calls[0].as_nanos(), 0,
            "Sleep duration should be 0 at exact boundary");

        // Test 3: At note_off_cycle + 1 - should send Note Off
        let clock3 = MockClock::new(vec![
            base_time + Duration::from_nanos((note_off_cycle + 1) * CYCLE_DURATION_NANOS),
            base_time + Duration::from_nanos((note_off_cycle + 1) * CYCLE_DURATION_NANOS + 1),
        ]);

        let mut player_task3 = PlayerTask::new(midi_output, cmd_receiver, _resp_sender, clock3);
        let result3 = player_task3.play_cycle(&mut play_state);
        
        // Take back the components for verification
        let (_, midi_output, _, _, clock3) = player_task3.take();

        assert!(matches!(result3, PlayResult::Finished), "Should finish after Note Off");
        assert_eq!(midi_output.sent_messages.len(), 2, "Should have sent Note Off");
        assert_eq!(midi_output.sent_messages[1][0] & 0xF0, 0x90, "Should be Note Off (0x90)");
        assert_eq!(midi_output.sent_messages[1][2], 0, "Velocity should be 0 (Note Off)");
        
        // Verify no sleep was called (no more events)
        let sleep_calls = clock3.sleep_calls();
        assert_eq!(sleep_calls.len(), 0, "Should not call sleep when finished");
    }

    #[test]
    fn test_play_cycle_with_non_zero_start_cycle() {
        // Setup
        let midi_output = MockMidiOutput::new();
        let (_cmd_sender, cmd_receiver) = mpsc::sync_channel::<Cmd>(1);
        let (_resp_sender, _resp_receiver) = mpsc::sync_channel::<Resp>(1);

        // Create test PlayData with a single note
        let play_data = create_single_note_play_data();
        
        // Get the actual cycles from the MIDI data
        let _note_on_cycle = play_data.midi_data[0].0;
        let note_off_cycle = play_data.midi_data[1].0;
        
        // Set start_cycle to 500 (start playback from middle of the note)
        let start_cycle = 500u64;
        let play_cmd_data = PlayCmdData {
            seq: 1,
            play_data,
            start_cycle,
        };

        // Create PlayState with non-zero cycle_offset
        // Calculate start_idx from start_cycle
        let start_idx = match play_cmd_data.play_data.midi_data.find(&start_cycle) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
        let base_time = Instant::now();
        let mut play_state = PlayState {
            play_cmd_data,
            cycle_offset: start_cycle,
            start_timestamp: base_time,
            current_idx: start_idx,
        };

        // Test 1: Start from cycle 500, advance to cycle 501
        // Since note_on is at cycle 0, which is < start_cycle (500), it should already be processed
        // We should be waiting for note_off at cycle 1000000
        let clock1 = MockClock::new(vec![
            base_time + Duration::from_nanos(CYCLE_DURATION_NANOS + 1),  // elapsed = 1001ns, current_position = 500 + 1 = 501
            base_time + Duration::from_nanos(CYCLE_DURATION_NANOS + 2),
        ]);

        let (_resp_sender, _resp_receiver) = mpsc::sync_channel::<Resp>(1);
        let mut player_task1 = PlayerTask::new(midi_output, cmd_receiver, _resp_sender, clock1);
        let result1 = player_task1.play_cycle(&mut play_state);
        
        // Take back the components for next test
        let (_, midi_output, cmd_receiver, _resp_sender, clock1) = player_task1.take();

        // Should continue (waiting for note_off)
        assert!(matches!(result1, PlayResult::Continue), "Should continue, waiting for note_off");
        
        // Note On at cycle 0 is BEFORE start_cycle (500), so it should NOT be sent
        assert_eq!(midi_output.sent_messages.len(), 0, "No messages should be sent (note_on is before start_cycle)");
        
        // Verify sleep was called (waiting for note_off at cycle 1000000)
        let sleep_calls = clock1.sleep_calls();
        assert_eq!(sleep_calls.len(), 1, "Should have called sleep once");
        // target_time = base_time + (note_off_cycle - start_cycle) * CYCLE_DURATION_NANOS
        //             = base_time + (1000000 - 500) * 1000
        // now = base_time + 1002ns
        // sleep_duration = target_time - now = (1000000 - 500) * 1000 - 1002
        let expected_sleep = (note_off_cycle - start_cycle) * CYCLE_DURATION_NANOS - (CYCLE_DURATION_NANOS + 2);
        assert_eq!(sleep_calls[0].as_nanos(), expected_sleep as u128,
            "Sleep duration should be {} nanoseconds", expected_sleep);

        // Test 2: Advance past note_off_cycle to send Note Off
        let clock2 = MockClock::new(vec![
            base_time + Duration::from_nanos((note_off_cycle - start_cycle + 1) * CYCLE_DURATION_NANOS),
            base_time + Duration::from_nanos((note_off_cycle - start_cycle + 1) * CYCLE_DURATION_NANOS + 1),
        ]);

        let mut player_task2 = PlayerTask::new(midi_output, cmd_receiver, _resp_sender, clock2);
        let result2 = player_task2.play_cycle(&mut play_state);
        
        // Take back the components for verification
        let (_, midi_output, _, _, clock2) = player_task2.take();

        // Should finish after sending note_off
        assert!(matches!(result2, PlayResult::Finished), "Should finish after note_off");
        
        // Only Note Off should be sent (Note On at cycle 0 is before start_cycle)
        assert_eq!(midi_output.sent_messages.len(), 1, "Only Note Off should be sent");
        assert_eq!(midi_output.sent_messages[0][0] & 0xF0, 0x90, "Should be Note Off (0x90)");
        assert_eq!(midi_output.sent_messages[0][2], 0, "Velocity should be 0 (Note Off)");
        
        // No sleep should be called (finished)
        let sleep_calls = clock2.sleep_calls();
        assert_eq!(sleep_calls.len(), 0, "Should not call sleep when finished");
    }
}
