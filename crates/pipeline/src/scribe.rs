//! ElevenLabs Scribe v2 Realtime streaming-STT client.
//!
//! One persistent WebSocket connection per track. Audio in (i16 LE @
//! 16 kHz mono, base64 JSON chunks), JSON events out. The server runs
//! VAD-based commit: `partial_transcript` maps to `PipelineEvent::Partial`
//! and `committed_transcript` maps to `PipelineEvent::Flushed`, so each
//! committed utterance goes straight to the translation layer.
//!
//! Language is auto-detected server-side when `language_code` is `None`
//! (90+ languages, incl. Mandarin). Pin `language_code` (ISO 639-1/639-3)
//! to force a single language.

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use futures_util::{FutureExt, SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, http::HeaderValue, Message},
};

use crate::events::{PipelineEvent, TrackId};
use crate::PipelineError;

/// VAD tuning for Scribe's server-side auto-commit. Pick one of the const
/// presets; all four fields travel together so a preset is a single
/// substitution, not scattered conditionals.
#[derive(Debug, Clone, Copy)]
pub struct VadPreset {
    /// Seconds of silence before the VAD commits an utterance (0.3–3.0).
    pub silence_threshold_secs: f64,
    /// Speech-detection sensitivity (0.1–0.9). Lower = more sensitive.
    pub threshold: f64,
    /// Speech bursts shorter than this are ignored (50–2000 ms).
    pub min_speech_duration_ms: u32,
    /// Silence gaps shorter than this are ignored (50–2000 ms).
    pub min_silence_duration_ms: u32,
}

impl VadPreset {
    /// ElevenLabs defaults — complete sentences, higher latency.
    pub const DEFAULT: Self = Self {
        silence_threshold_secs: 1.5,
        threshold: 0.4,
        min_speech_duration_ms: 100,
        min_silence_duration_ms: 100,
    };

    /// Regular conversation: commits on natural sentence pauses.
    pub const SLOW_SPEECH: Self = Self {
        silence_threshold_secs: 1.0,
        ..Self::DEFAULT
    };

    /// Rapid/news speech: short pauses count as silence so fast speakers
    /// don't accumulate multi-sentence chunks.
    pub const FAST_SPEECH: Self = Self {
        silence_threshold_secs: 0.5,
        min_speech_duration_ms: 50,
        min_silence_duration_ms: 50,
        ..Self::DEFAULT
    };
}

impl Default for VadPreset {
    fn default() -> Self { Self::DEFAULT }
}

#[derive(Debug, Clone)]
pub struct ScribeConfig {
    pub api_key: String,
    pub model:   String,
    /// ISO 639-1/639-3 language code (e.g. `"en"`, `"zho"`). `None` =
    /// server-side automatic language detection.
    pub language_code: Option<String>,
    /// Server-side VAD auto-commit tuning.
    pub vad: VadPreset,
    pub endpoint: String,
}

impl ScribeConfig {
    pub fn new(api_key: String) -> Self {
        Self {
            api_key,
            model: "scribe_v2_realtime".into(),
            language_code: None,
            vad: VadPreset::DEFAULT,
            endpoint: "wss://api.elevenlabs.io/v1/speech-to-text/realtime".into(),
        }
    }

    pub fn with_language(api_key: String, language: impl Into<String>) -> Self {
        Self {
            language_code: Some(language.into()),
            ..Self::new(api_key)
        }
    }

    fn build_url(&self) -> String {
        let mut pairs: Vec<(&str, String)> = vec![
            ("model_id", self.model.clone()),
            ("audio_format", "pcm_16000".into()),
            ("commit_strategy", "vad".into()),
            // Suppress music/noise so only speech is transcribed.
            ("filter_background_audio", "true".into()),
            (
                "vad_silence_threshold_secs",
                format!("{}", self.vad.silence_threshold_secs),
            ),
            ("vad_threshold", format!("{}", self.vad.threshold)),
            (
                "min_speech_duration_ms",
                format!("{}", self.vad.min_speech_duration_ms),
            ),
            (
                "min_silence_duration_ms",
                format!("{}", self.vad.min_silence_duration_ms),
            ),
        ];
        if let Some(ref lang) = self.language_code {
            pairs.push(("language_code", lang.clone()));
        }
        format!(
            "{}?{}",
            self.endpoint,
            pairs
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("&")
        )
    }
}

/// Wall-clock timestamp as `HH:MM:SS.mmm` for latency logging.
fn ts() -> String {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let s  = (ms / 1000) % 86400;
    let h  = s / 3600;
    let m  = (s % 3600) / 60;
    let sc = s % 60;
    let ms = ms % 1000;
    format!("{h:02}:{m:02}:{sc:02}.{ms:03}")
}

/// Handle the caller uses to feed audio in + receive events out.
/// Cloneable — dropping the *last* clone closes the audio channel,
/// at which point the WS task commits and ends.
#[derive(Clone)]
pub struct ScribeHandle {
    audio_tx: mpsc::Sender<Vec<i16>>,
    closed_logged: Arc<AtomicBool>,
}

impl ScribeHandle {
    /// Push 16-bit PCM samples (16 kHz mono) into the stream. Non-
    /// blocking: drops on backpressure with a warning.
    pub fn push_pcm(&self, samples: Vec<i16>) {
        if samples.is_empty() {
            return;
        }
        match self.audio_tx.try_send(samples) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                log::warn!("Scribe audio queue full — dropping samples");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                if !self.closed_logged.swap(true, Ordering::Relaxed) {
                    log::error!(
                        "Scribe audio channel closed (client task ended) \
                         — subsequent push_pcm calls will be silently dropped"
                    );
                }
            }
        }
    }

    /// Whether the WS task has ended (audio channel closed).
    pub fn is_closed(&self) -> bool {
        self.audio_tx.is_closed()
    }
}

pub struct ScribeClient;

impl ScribeClient {
    /// Spawn the client. Returns the handle + the event receiver.
    pub fn spawn(
        cfg:   ScribeConfig,
        track: TrackId,
    ) -> (ScribeHandle, mpsc::Receiver<PipelineEvent>) {
        crate::ensure_crypto_provider();

        let (audio_tx, audio_rx) = mpsc::channel::<Vec<i16>>(512);
        let (event_tx, event_rx) = mpsc::channel::<PipelineEvent>(256);

        tokio::spawn(async move {
            let result = AssertUnwindSafe(run_client(
                cfg, track, audio_rx, event_tx.clone(),
            ))
            .catch_unwind()
            .await;

            let err_msg = match result {
                Ok(Ok(())) => return,
                Ok(Err(e)) => format!("{e}"),
                Err(panic_payload) => {
                    let s = panic_payload
                        .downcast_ref::<&'static str>()
                        .copied()
                        .or_else(|| {
                            panic_payload
                                .downcast_ref::<String>()
                                .map(String::as_str)
                        })
                        .unwrap_or("<non-string panic>");
                    format!("client task panicked: {s}")
                }
            };
            log::error!("Scribe client task ended: {err_msg}");
            let _ = event_tx
                .send(PipelineEvent::Error { track, error: err_msg })
                .await;
        });

        (
            ScribeHandle {
                audio_tx,
                closed_logged: Arc::new(AtomicBool::new(false)),
            },
            event_rx,
        )
    }
}

async fn run_client(
    cfg:        ScribeConfig,
    track:      TrackId,
    mut audio_rx: mpsc::Receiver<Vec<i16>>,
    event_tx:   mpsc::Sender<PipelineEvent>,
) -> Result<(), PipelineError> {
    let url = cfg.build_url();
    log::info!(
        "Scribe: connecting — params: {}",
        url.split_once('?').map(|(_, q)| q).unwrap_or("(none)")
    );

    let mut req = url
        .as_str()
        .into_client_request()
        .map_err(PipelineError::WebSocket)?;
    req.headers_mut().insert(
        "xi-api-key",
        HeaderValue::from_str(&cfg.api_key)
            .map_err(|_| PipelineError::Scribe("invalid API key header".into()))?,
    );

    let (ws, _resp) = connect_async(req).await.map_err(PipelineError::WebSocket)?;
    log::info!("Scribe: connected");

    let (mut ws_sink, mut ws_stream) = ws.split();

    loop {
        tokio::select! {
            maybe_samples = audio_rx.recv() => {
                match maybe_samples {
                    Some(samples) => {
                        let msg = audio_chunk_msg(&samples, false);
                        if let Err(e) = ws_sink.send(Message::Text(msg.into())).await {
                            log::error!("Scribe: ws send failed: {e}");
                            return Err(PipelineError::WebSocket(e));
                        }
                    }
                    None => {
                        // Audio channel closed — commit any trailing audio
                        // and end the stream.
                        let commit = audio_chunk_msg(&[], true);
                        let _ = ws_sink.send(Message::Text(commit.into())).await;
                        break;
                    }
                }
            }

            maybe_msg = ws_stream.next() => {
                match maybe_msg {
                    Some(Ok(msg)) => {
                        if let Err(e) = handle_ws_message(msg, &event_tx, track).await {
                            log::warn!("Scribe: bad ws message: {e}");
                        }
                    }
                    Some(Err(e)) => {
                        log::error!("Scribe: ws error: {e}");
                        return Err(PipelineError::WebSocket(e));
                    }
                    None => {
                        log::info!("Scribe: ws closed");
                        break;
                    }
                }
            }
        }
    }

    // Drain tail messages after the final commit.
    while let Some(msg) = ws_stream.next().await {
        match msg {
            Ok(m) => { let _ = handle_ws_message(m, &event_tx, track).await; }
            Err(_) => break,
        }
    }

    Ok(())
}

fn audio_chunk_msg(samples: &[i16], commit: bool) -> String {
    let mut bytes = Vec::with_capacity(samples.len() * 2);
    for &s in samples {
        bytes.extend_from_slice(&s.to_le_bytes());
    }
    serde_json::json!({
        "message_type": "input_audio_chunk",
        "audio_base_64": base64::engine::general_purpose::STANDARD.encode(bytes),
        "commit": commit,
        "sample_rate": 16_000,
    })
    .to_string()
}

async fn handle_ws_message(
    msg:      Message,
    event_tx: &mpsc::Sender<PipelineEvent>,
    track:    TrackId,
) -> Result<(), PipelineError> {
    let text = match msg {
        Message::Text(t) => t,
        _ => return Ok(()),
    };

    let raw: serde_json::Value = serde_json::from_str(&text)?;
    let kind = raw
        .get("message_type")
        .and_then(|t| t.as_str())
        .unwrap_or("");

    match kind {
        "partial_transcript" => {
            let t = raw.get("text").and_then(|t| t.as_str()).unwrap_or("");
            if !t.trim().is_empty() {
                log::info!("[{}] scribe partial={:?}", ts(), t);
                let _ = event_tx.send(PipelineEvent::Partial {
                    track,
                    text: t.to_string(),
                }).await;
            }
        }
        "committed_transcript" | "committed_transcript_with_timestamps" => {
            let t = raw.get("text").and_then(|t| t.as_str()).unwrap_or("");
            if !t.trim().is_empty() {
                log::info!("[{}] scribe committed → flush: {:?}", ts(), t);
                let _ = event_tx.send(PipelineEvent::Flushed {
                    track,
                    text: t.trim().to_string(),
                    reason: "committed",
                }).await;
            }
        }
        "session_started" => log::info!("Scribe: session started"),
        "input_error" => log::warn!("Scribe input error: {text}"),
        other => log::trace!("Scribe: unknown message type '{other}'"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_defaults_to_vad_auto_detect() {
        let cfg = ScribeConfig::new("dummy".into());
        let url = cfg.build_url();
        assert!(url.starts_with("wss://api.elevenlabs.io/v1/speech-to-text/realtime?"));
        for needle in [
            "model_id=scribe_v2_realtime",
            "audio_format=pcm_16000",
            "commit_strategy=vad",
            "vad_silence_threshold_secs=1.5",
            "vad_threshold=0.4",
            "min_speech_duration_ms=100",
            "min_silence_duration_ms=100",
            "filter_background_audio=true",
        ] {
            assert!(url.contains(needle), "missing {needle} in {url}");
        }
        assert!(!url.contains("language_code"), "language_code must be omitted for auto-detect: {url}");
    }

    #[test]
    fn url_explicit_language() {
        let cfg = ScribeConfig::with_language("dummy".into(), "zh");
        let url = cfg.build_url();
        assert!(url.contains("language_code=zh"), "missing language_code=zh in {url}");
    }

    #[test]
    fn audio_chunk_is_base64_pcm() {
        // [0x0001, -1] → LE bytes [0x01, 0x00, 0xFF, 0xFF] → "AQD//w=="
        let msg = audio_chunk_msg(&[1, -1], false);
        let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(v["message_type"], "input_audio_chunk");
        assert_eq!(v["audio_base_64"], "AQD//w==");
        assert_eq!(v["commit"], false);
        assert_eq!(v["sample_rate"], 16_000);
    }

    #[test]
    fn parse_partial_and_committed() {
        let p: serde_json::Value = serde_json::from_str(
            r#"{"message_type":"partial_transcript","text":"hello"}"#,
        ).unwrap();
        assert_eq!(p["message_type"], "partial_transcript");
        assert_eq!(p["text"], "hello");

        let c: serde_json::Value = serde_json::from_str(
            r#"{"message_type":"committed_transcript","text":"hello world"}"#,
        ).unwrap();
        assert_eq!(c["message_type"], "committed_transcript");
        assert_eq!(c["text"], "hello world");
    }
}
