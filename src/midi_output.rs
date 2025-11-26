/// Trait for abstracting MIDI output operations
/// This allows for testing without requiring actual MIDI hardware
pub trait MidiOutput: Send {
    /// Send a MIDI message
    fn send(&mut self, message: &[u8]) -> Result<(), String>;
}

/// Real MIDI output implementation using midir
pub struct RealMidiOutput {
    connection: midir::MidiOutputConnection,
}

impl RealMidiOutput {
    pub fn new(connection: midir::MidiOutputConnection) -> Self {
        Self { connection }
    }
}

impl MidiOutput for RealMidiOutput {
    fn send(&mut self, message: &[u8]) -> Result<(), String> {
        self.connection.send(message)
            .map_err(|e| format!("MIDI send error: {:?}", e))
    }
}

/// Mock MIDI output for testing
#[cfg(test)]
pub struct MockMidiOutput {
    pub sent_messages: Vec<Vec<u8>>,
}

#[cfg(test)]
impl MockMidiOutput {
    pub fn new() -> Self {
        Self {
            sent_messages: Vec::new(),
        }
    }
}

#[cfg(test)]
impl MidiOutput for MockMidiOutput {
    fn send(&mut self, message: &[u8]) -> Result<(), String> {
        self.sent_messages.push(message.to_vec());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_midi_output() {
        let mut mock = MockMidiOutput::new();
        
        // Send some test messages
        mock.send(&[0x90, 0x3C, 0x7F]).unwrap();
        mock.send(&[0x80, 0x3C, 0x00]).unwrap();
        
        // Verify messages were recorded
        assert_eq!(mock.sent_messages.len(), 2);
        assert_eq!(mock.sent_messages[0], vec![0x90, 0x3C, 0x7F]);
        assert_eq!(mock.sent_messages[1], vec![0x80, 0x3C, 0x00]);
    }
}

// Made with Bob
