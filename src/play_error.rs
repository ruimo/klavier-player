use klavier_core::{play_start_tick::ToAccumTickError, repeat::RenderRegionError};

#[derive(Debug, Clone, PartialEq)]
pub enum PlayError {
    RenderError(RenderRegionError),
    PlayStartTickError(ToAccumTickError),
    SendCommandError(String),
}

impl std::fmt::Display for PlayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl core::error::Error for PlayError {}

// Made with Bob
