//! Gemini Live Translate API client.
//!
//! Replaces the STT → translate → TTS chain for Track 1 with a single
//! WebSocket that streams raw PCM in and receives translated PCM out,
//! plus input/output transcripts.
//!
//! Protocol: wss://generativelanguage.googleapis.com/ws/...
//!   - Send setup with model, responseModalities=["AUDIO"], translationConfig.
//!   - Stream audio chunks as realtimeInput with mimeType audio/pcm;rate=16000.
//!   - Receive serverContent with inputTranscription, outputTranscription,
//!     and modelTurn.parts[].inlineData (base64 PCM 24 kHz mono LE).
//!
//! This module mirrors the shape of the Deepgram client: a handle with a
//! `push_pcm` method, a spawned async task, and a channel of `PipelineEvent`s.
//! Audio output is returned on a separate channel so the caller can feed it
//! straight into the PipeWire playback path.

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use base64::Engine as _;
use futures_util::{FutureExt, SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
};

use crate::events::{PipelineEvent, TrackId};
use crate::PipelineError;

/// Gemini Live Translate model recommended by Google.
pub const DEFAULT_MODEL: &str = "gemini-3.5-live-translate-preview";

/// Input audio format required by the API: 16-bit PCM, 16 kHz, mono, LE.
pub const INPUT_SAMPLE_RATE: u32 = 16_000;

/// Output audio format returned by the API: 16-bit PCM, 24 kHz, mono, LE.
pub const OUTPUT_SAMPLE_RATE: u32 = 24_000;

/// Map DeepL-style language codes used elsewhere in the app to the
/// BCP-47 codes required by Gemini Live Translate.
///
/// Gemini supports codes like `"de"`, `"pt-PT"`, `"zh-Hans"`, etc. We
/// keep the mapping close to the Gemini client so callers don't need to
/// know the wire format.
pub fn bcp47_from_deepl(code: &str) -> String {
    match code.to_lowercase().as_str() {
        "en" => "en".into(),
        "de" => "de".into(),
        "nl" => "nl".into(),
        "et" => "et".into(),
        "it" => "it".into(),
        "es" => "es".into(),
        "fr" => "fr".into(),
        "pl" => "pl".into(),
        "pt" | "pt-pt" => "pt-PT".into(),
        "pt-br" => "pt-BR".into(),
        "ru" => "ru".into(),
        "zh" | "zh-hans" => "zh-Hans".into(),
        "zh-hant" => "zh-Hant".into(),
        "ja" => "ja".into(),
        "ko" => "ko".into(),
        other => other.to_string(),
    }
}

/// Approximate chunk size the API recommends sending (100 ms of input).
const INPUT_CHUNK_SAMPLES: usize = INPUT_SAMPLE_RATE as usize / 10;

/// Configuration for the Gemini Live Translate session.
#[derive(Debug, Clone)]
pub struct GeminiConfig {
    pub api_key: String,
    pub model: String,
    /// BCP-47 target language code, e.g. `"de"`, `"pl"`.
    pub target_language_code: String,
    /// If true, echo input that is already in the target language.
    /// Default false, but useful for meetings where both languages may appear.
    pub echo_target_language: bool,
}

impl GeminiConfig {
    pub fn new(api_key: impl Into<String>, target_language_code: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            model: DEFAULT_MODEL.into(),
            target_language_code: target_language_code.into(),
            echo_target_language: false,
        }
    }

    fn ws_url(&self) -> String {
        format!(
            "wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key={}",
            self.api_key
        )
    }
}

/// Caller handle used to feed microphone PCM into the Gemini session.
/// Cloneable; dropping the last clone closes the audio channel and the
/// background task ends gracefully.
#[derive(Clone)]
pub struct GeminiHandle {
    audio_tx: mpsc::Sender<Vec<i16>>,
    closed_logged: Arc<AtomicBool>,
}

impl GeminiHandle {
    /// Push 16-bit PCM mono samples (16 kHz) into the stream. Non-blocking;
    /// drops on backpressure with a warning.
    pub fn push_pcm(&self, samples: Vec<i16>) {
        if samples.is_empty() {
            return;
        }
        match self.audio_tx.try_send(samples) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                log::warn!("Gemini audio queue full — dropping samples");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                if !self.closed_logged.swap(true, Ordering::Relaxed) {
                    log::error!(
                        "Gemini audio channel closed (client task ended) — \
                         subsequent push_pcm calls will be silently dropped"
                    );
                }
            }
        }
    }

    pub fn is_closed(&self) -> bool {
        self.audio_tx.is_closed()
    }
}

pub struct GeminiClient;

impl GeminiClient {
    /// Spawn the Gemini Live Translate task.
    ///
    /// Returns:
    ///   - a handle to push microphone audio into,
    ///   - a receiver for transcript/error events (`PipelineEvent`),
    ///   - a receiver for translated audio chunks as f32 samples at 24 kHz.
    pub fn spawn(
        cfg: GeminiConfig,
        track: TrackId,
    ) -> (
        GeminiHandle,
        mpsc::Receiver<PipelineEvent>,
        mpsc::Receiver<Option<Vec<f32>>>,
    ) {
        crate::ensure_crypto_provider();

        let (audio_tx, audio_rx) = mpsc::channel::<Vec<i16>>(512);
        let (event_tx, event_rx) = mpsc::channel::<PipelineEvent>(256);
        let (pcm_tx, pcm_rx) = mpsc::channel::<Option<Vec<f32>>>(256);

        tokio::spawn(async move {
            let result = AssertUnwindSafe(run_client(cfg, track, audio_rx, event_tx.clone(), pcm_tx))
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
                            panic_payload.downcast_ref::<String>().map(String::as_str)
                        })
                        .unwrap_or("<non-string panic>");
                    format!("Gemini client task panicked: {s}")
                }
            };
            log::error!("Gemini client task ended: {err_msg}");
            let _ = event_tx
                .send(PipelineEvent::Error {
                    track,
                    error: err_msg,
                })
                .await;
        });

        (
            GeminiHandle {
                audio_tx,
                closed_logged: Arc::new(AtomicBool::new(false)),
            },
            event_rx,
            pcm_rx,
        )
    }
}

// ── Wire protocol types ──────────────────────────────────────────────────────

#[derive(Serialize, Debug)]
struct SetupMessage {
    setup: Setup,
}

#[derive(Serialize, Debug)]
struct Setup {
    model: String,
    #[serde(rename = "generationConfig")]
    generation_config: GenerationConfig,
    #[serde(rename = "inputAudioTranscription")]
    input_audio_transcription: Empty,
    #[serde(rename = "outputAudioTranscription")]
    output_audio_transcription: Empty,
}

#[derive(Serialize, Debug)]
struct GenerationConfig {
    #[serde(rename = "responseModalities")]
    response_modalities: Vec<String>,
    #[serde(rename = "translationConfig")]
    translation_config: TranslationConfig,
}

#[derive(Serialize, Debug)]
struct Empty {}

#[derive(Serialize, Debug)]
struct TranslationConfig {
    #[serde(rename = "targetLanguageCode")]
    target_language_code: String,
    #[serde(rename = "echoTargetLanguage")]
    echo_target_language: bool,
}

#[derive(Serialize, Debug)]
struct RealtimeInputMessage {
    #[serde(rename = "realtimeInput")]
    realtime_input: RealtimeInput,
}

#[derive(Serialize, Debug)]
struct RealtimeInput {
    audio: AudioBlob,
}

#[derive(Serialize, Debug)]
struct AudioBlob {
    data: String,
    #[serde(rename = "mimeType")]
    mime_type: String,
}

#[derive(Deserialize, Debug)]
struct GeminiResponse {
    #[serde(rename = "setupComplete")]
    _setup_complete: Option<SetupComplete>,
    #[serde(rename = "serverContent")]
    server_content: Option<ServerContent>,
    #[serde(rename = "error")]
    error: Option<GeminiError>,
}

#[derive(Deserialize, Debug)]
struct SetupComplete {}

#[derive(Deserialize, Debug)]
struct ServerContent {
    #[serde(rename = "inputTranscription")]
    input_transcription: Option<Transcription>,
    #[serde(rename = "outputTranscription")]
    output_transcription: Option<Transcription>,
    #[serde(rename = "modelTurn")]
    model_turn: Option<ModelTurn>,
}

#[derive(Deserialize, Debug, Clone)]
struct Transcription {
    text: String,
    #[serde(rename = "languageCode")]
    _language_code: Option<String>,
}

#[derive(Deserialize, Debug)]
struct ModelTurn {
    parts: Vec<Part>,
}

#[derive(Deserialize, Debug)]
struct Part {
    #[serde(rename = "inlineData")]
    inline_data: Option<InlineData>,
}

#[derive(Deserialize, Debug)]
struct InlineData {
    #[serde(rename = "mimeType")]
    mime_type: String,
    data: String,
}

#[derive(Deserialize, Debug)]
struct GeminiError {
    message: String,
    code: Option<i32>,
}

// ── Runner ───────────────────────────────────────────────────────────────────

async fn run_client(
    cfg: GeminiConfig,
    track: TrackId,
    mut audio_rx: mpsc::Receiver<Vec<i16>>,
    event_tx: mpsc::Sender<PipelineEvent>,
    pcm_tx: mpsc::Sender<Option<Vec<f32>>>,
) -> Result<(), PipelineError> {
    let url = cfg.ws_url();
    log::info!(
        "Gemini: connecting to Live Translate (model={} key={}...)",
        cfg.model,
        cfg.api_key.chars().take(4).collect::<String>()
    );

    let req = url
        .as_str()
        .into_client_request()
        .map_err(PipelineError::WebSocket)?;

    let (ws, resp) = connect_async(req).await.map_err(PipelineError::WebSocket)?;
    log::info!("Gemini: connected (status={})", resp.status());

    let setup = SetupMessage {
        setup: Setup {
            model: format!("models/{}", cfg.model),
            generation_config: GenerationConfig {
                response_modalities: vec!["AUDIO".into()],
                translation_config: TranslationConfig {
                    target_language_code: cfg.target_language_code.clone(),
                    echo_target_language: cfg.echo_target_language,
                },
            },
            input_audio_transcription: Empty {},
            output_audio_transcription: Empty {},
        },
    };
    let setup_json = serde_json::to_string(&setup)?;
    log::debug!("Gemini setup message: {}", setup_json);

    let (mut ws_sink, mut ws_stream) = ws.split();
    ws_sink
        .send(Message::Text(setup_json.into()))
        .await
        .map_err(PipelineError::WebSocket)?;
    log::info!("Gemini: setup sent, waiting for setupComplete");

    // Wait for setupComplete before streaming audio, mirroring the Python SDK.
    let mut setup_seen = false;
    let setup_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !setup_seen {
        match tokio::time::timeout_at(setup_deadline, ws_stream.next()).await {
            Ok(Some(Ok(msg))) => {
                if msg.is_close() {
                    log::error!("Gemini: server closed WebSocket before setupComplete");
                    return Err(PipelineError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "server closed WebSocket before setupComplete",
                    )));
                }
                let text = match msg {
                    Message::Text(t) => t.to_string(),
                    // Gemini sends JSON payloads over Binary-opcode frames, not Text.
                    Message::Binary(b) => match String::from_utf8(b.to_vec()) {
                        Ok(s) => s,
                        Err(e) => {
                            log::warn!("Gemini: non-UTF8 binary frame during setup: {e}");
                            continue;
                        }
                    },
                    Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {
                        continue;
                    }
                    Message::Close(_) => {
                        log::error!("Gemini: server closed WebSocket before setupComplete");
                        return Err(PipelineError::Io(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "server closed WebSocket before setupComplete",
                        )));
                    }
                };
                log::debug!("Gemini setup response raw: {}", text);
                match serde_json::from_str::<serde_json::Value>(&text) {
                    Ok(v) => {
                        if v.get("setupComplete").is_some() {
                            setup_seen = true;
                            log::info!("Gemini: setup complete");
                        }
                        if let Some(err) = v.get("error") {
                            log::error!("Gemini setup error: {}", err);
                            return Err(PipelineError::Io(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                format!("Gemini setup error: {}", err).as_str(),
                            )));
                        }
                    }
                    Err(e) => {
                        log::warn!("Gemini: bad setup response JSON: {e}");
                    }
                }
            }
            Ok(Some(Err(e))) => {
                log::error!("Gemini: ws error during setup: {e}");
                return Err(PipelineError::WebSocket(e));
            }
            Ok(None) => {
                log::error!("Gemini: ws stream ended before setupComplete");
                return Err(PipelineError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Gemini: ws stream ended before setupComplete",
                )));
            }
            Err(_) => {
                log::error!("Gemini: timeout waiting for setupComplete");
                return Err(PipelineError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Gemini: timeout waiting for setupComplete",
                )));
            }
        }
    }

    // Buffer input audio so we send exactly 100 ms chunks (1600 samples) as recommended.
    let mut input_buffer: Vec<i16> = Vec::with_capacity(INPUT_CHUNK_SAMPLES * 2);
    let mut flush_tick = interval(Duration::from_millis(100));
    flush_tick.tick().await; // consume immediate first tick

    let mut last_input_text: String = String::new();
    loop {
        tokio::select! {
            maybe_samples = audio_rx.recv() => {
                match maybe_samples {
                    Some(samples) => {
                        input_buffer.extend_from_slice(&samples);
                        while input_buffer.len() >= INPUT_CHUNK_SAMPLES {
                            let chunk: Vec<i16> = input_buffer.drain(..INPUT_CHUNK_SAMPLES).collect();
                            send_audio_chunk(&mut ws_sink, &chunk).await?;
                        }
                    }
                    None => {
                        // Audio channel closed — send any final buffered audio.
                        if !input_buffer.is_empty() {
                            let _ = send_audio_chunk(&mut ws_sink, &input_buffer).await;
                            input_buffer.clear();
                        }
                        log::info!("Gemini: audio input closed, ending session");
                        break;
                    }
                }
            }

            _ = flush_tick.tick() => {
                if !input_buffer.is_empty() {
                    send_audio_chunk(&mut ws_sink, &input_buffer).await?;
                    input_buffer.clear();
                }
            }

            maybe_msg = ws_stream.next() => {
                match maybe_msg {
                    Some(Ok(msg)) => {
                        if msg.is_close() {
                            if let Message::Close(Some(frame)) = msg {
                                log::info!(
                                    "Gemini: server closed the WebSocket (code={} reason={})",
                                    frame.code,
                                    frame.reason
                                );
                            } else {
                                log::info!("Gemini: server closed the WebSocket (no reason)");
                            }
                            break;
                        }
                        match handle_message(
                            msg,
                            &event_tx,
                            &pcm_tx,
                            track,
                            &cfg.target_language_code,
                            &mut last_input_text,
                            &mut setup_seen,
                        ).await {
                            Ok(true) => {}
                            Ok(false) => {
                                log::info!("Gemini: ending session after server error");
                                break;
                            }
                            Err(e) => {
                                log::warn!("Gemini: bad message: {e}");
                            }
                        }
                    }
                    Some(Err(e)) => {
                        log::error!("Gemini: ws error: {e}");
                        return Err(PipelineError::WebSocket(e));
                    }
                    None => {
                        log::info!("Gemini: ws stream ended");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

async fn send_audio_chunk(
    ws_sink: &mut futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >,
    samples: &[i16],
) -> Result<(), PipelineError> {
    let bytes: Vec<u8> = samples.iter().flat_map(|s| s.to_le_bytes()).collect();
    let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
    let msg = RealtimeInputMessage {
        realtime_input: RealtimeInput {
            audio: AudioBlob {
                data: b64,
                mime_type: format!("audio/pcm;rate={}", INPUT_SAMPLE_RATE),
            },
        },
    };
    let json = serde_json::to_string(&msg)?;
    ws_sink
        .send(Message::Text(json.into()))
        .await
        .map_err(PipelineError::WebSocket)
}

/// Returns Ok(true) to keep the session alive, Ok(false) to shut down cleanly.
async fn handle_message(
    msg: Message,
    event_tx: &mpsc::Sender<PipelineEvent>,
    pcm_tx: &mpsc::Sender<Option<Vec<f32>>>,
    track: TrackId,
    _target_lang: &str,
    last_input_text: &mut String,
    setup_seen: &mut bool,
) -> Result<bool, PipelineError> {
    let text = match msg {
        Message::Text(t) => t.to_string(),
        // Gemini sends JSON payloads over Binary-opcode frames, not Text.
        Message::Binary(b) => match String::from_utf8(b.to_vec()) {
            Ok(s) => s,
            Err(e) => {
                log::warn!("Gemini: non-UTF8 binary frame: {e}");
                return Ok(true);
            }
        },
        Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => return Ok(true),
        Message::Close(_) => return Ok(false),
    };

    let resp: GeminiResponse = serde_json::from_str(&text)?;

    if let Some(err) = resp.error {
        log::error!("Gemini API error: {} (code {:?})", err.message, err.code);
        let _ = event_tx
            .send(PipelineEvent::Error {
                track,
                error: format!("Gemini API error: {} (code {:?})", err.message, err.code),
            })
            .await;
        return Ok(false);
    }

    if resp._setup_complete.is_some() {
        *setup_seen = true;
        log::info!("Gemini: setup complete");
    }

    let Some(content) = resp.server_content else { return Ok(true) };

    if let Some(tx) = content.input_transcription {
        let t = tx.text.trim();
        if !t.is_empty() {
            log::info!("Gemini input transcription: {}", t);
            *last_input_text = t.to_string();
            let _ = event_tx
                .send(PipelineEvent::Partial {
                    track,
                    text: t.to_string(),
                })
                .await;
        }
    }

    if let Some(tx) = content.output_transcription {
        let t = tx.text.trim();
        if !t.is_empty() {
            log::info!("Gemini output transcription: {}", t);
            let source = if last_input_text.is_empty() {
                "...".to_string()
            } else {
                last_input_text.clone()
            };
            let _ = event_tx
                .send(PipelineEvent::Translated {
                    track,
                    source_text: source,
                    translated: t.to_string(),
                })
                .await;
        }
    }

    if let Some(turn) = content.model_turn {
        for part in turn.parts {
            if let Some(data) = part.inline_data {
                if data.mime_type.starts_with("audio/") {
                    if let Some(samples) = decode_pcm(&data.data) {
                        let _ = pcm_tx.send(Some(samples)).await;
                    }
                }
            }
        }
    }

    Ok(true)
}

fn decode_pcm(b64: &str) -> Option<Vec<f32>> {
    if b64.is_empty() {
        return None;
    }
    let bytes = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
    if bytes.len() < 2 {
        return None;
    }
    Some(
        bytes
            .chunks_exact(2)
            .map(|c| i16::from_le_bytes([c[0], c[1]]) as f32 / 32768.0)
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_pcm_silence() {
        let raw = vec![0u8, 0, 0, 0, 0, 0];
        let b64 = base64::engine::general_purpose::STANDARD.encode(&raw);
        let samples = decode_pcm(&b64).unwrap();
        assert_eq!(samples.len(), 3);
        assert!(samples.iter().all(|&s| s.abs() < 0.001));
    }

    #[test]
    fn decode_pcm_max() {
        let raw = vec![0xFF, 0x7F]; // i16 max
        let b64 = base64::engine::general_purpose::STANDARD.encode(&raw);
        let samples = decode_pcm(&b64).unwrap();
        assert_eq!(samples.len(), 1);
        assert!((samples[0] - 1.0).abs() < 0.001);
    }

    #[test]
    fn setup_message_serializes() {
        let cfg = GeminiConfig::new("key", "de");
        let setup = SetupMessage {
            setup: Setup {
                model: format!("models/{}", cfg.model),
                generation_config: GenerationConfig {
                    response_modalities: vec!["AUDIO".into()],
                    translation_config: TranslationConfig {
                        target_language_code: cfg.target_language_code,
                        echo_target_language: cfg.echo_target_language,
                    },
                },
                input_audio_transcription: Empty {},
                output_audio_transcription: Empty {},
            },
        };
        let json = serde_json::to_string(&setup).unwrap();
        assert!(json.contains("setup"));
        assert!(json.contains("AUDIO"));
        assert!(json.contains("translationConfig"));
        assert!(json.contains("de"));
    }

    #[test]
    fn bcp47_mapping_from_deepl_codes() {
        assert_eq!(bcp47_from_deepl("DE"), "de");
        assert_eq!(bcp47_from_deepl("EN"), "en");
        assert_eq!(bcp47_from_deepl("PT-PT"), "pt-PT");
        assert_eq!(bcp47_from_deepl("ZH"), "zh-Hans");
        assert_eq!(bcp47_from_deepl("et"), "et");
    }
}
