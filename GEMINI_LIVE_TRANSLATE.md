# Gemini Live Translate — Protocol & Limitations

Sources inspected:
- `https://github.com/googleapis/python-genai` (SDK source, especially `google/genai/_live_converters.py` and `tests/live/test_live.py`)
- `https://github.com/google-gemini/gemini-live-api-examples` (official examples, including `command-line/python/translate.py`)
- `https://ai.google.dev/gemini-api/docs/live-api/live-translate` (official docs)

## 1. Endpoint

```text
wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=API_KEY
```

Authentication is `?key=YOUR_AI_STUDIO_KEY` (not a Google Cloud API key). Other auth variants (OAuth, ephemeral tokens) use different endpoints.

## 2. Setup message (verified from SDK source)

The SDK serializes `LiveConnectConfig` to this exact wire shape for the ML Dev API:

```json
{
  "setup": {
    "model": "models/gemini-3.5-live-translate-preview",
    "generationConfig": {
      "responseModalities": ["AUDIO"],
      "translationConfig": {
        "targetLanguageCode": "de",
        "echoTargetLanguage": true
      }
    },
    "inputAudioTranscription": {},
    "outputAudioTranscription": {}
  }
}
```

Important: the official docs page shows `inputAudioTranscription` and `outputAudioTranscription` nested inside `generationConfig`. The SDK source and the live server reject that placement. They must be at the `setup` level, as shown above. The `translationConfig` must be inside `generationConfig`.

## 3. Sending audio

- Format: raw 16-bit little-endian PCM, mono, 16 kHz.
- Wire message:

```json
{
  "realtimeInput": {
    "audio": {
      "data": "BASE64_OF_LITTLE_ENDIAN_I16_PCM",
      "mimeType": "audio/pcm;rate=16000"
    }
  }
}
```

- Chunk size: 100 ms (1600 samples / 3200 bytes) is the recommended chunk. Sending continuously is expected — the model does its own speech detection.
- The client does **not** need to send `turnComplete` for Live Translate. It is a continuous-stream interpreter, not a turn-based assistant.

## 4. Receiving responses

Server messages contain `serverContent` with:

- `inputTranscription` — source-language transcript.
- `outputTranscription` — translated transcript.
- `modelTurn.parts[].inlineData` — translated audio, base64-encoded 16-bit PCM at 24 kHz, mono.

There is no separate `setupComplete` requirement for the client to start sending audio; after the setup message you can stream immediately.

## 5. Why the current build may stay silent

The most likely cause is `echoTargetLanguage: false` with the target language set to `de`.

From the docs:

> `echoTargetLanguage`: A boolean indicating how input audio that is already in the target language should be handled. If set to `true`, the model will echo (parrot) input audio that is already in the target language. If set to `false`, the model will stay silent when the input speech is already in the target language.

If you speak German while the target language is German and `echoTargetLanguage` is `false`, the model intentionally produces no output. For testing, set it to `true` or speak a language other than the target language.

Other possible causes for no output:

- Wrong capture source or muted microphone.
- API key not enabled for the `gemini-3.5-live-translate-preview` model.
- Speaking too briefly for the model to commit a translation.
- Network / region latency delaying the first response by several seconds.

## 6. Limitations

- Translation only: no tools, no system instructions, no function calling.
- Input is audio-only for this model.
- Output audio is 24 kHz mono PCM; the client must resample if the playback sink expects a different rate.
- The model does its own speech/silence handling, so continuous streaming is the intended mode. A client-side VAD is optional for bandwidth saving, not required for correctness.
- Supported language list is fixed; see the official docs for the 70+ supported BCP-47 codes.
- There is no documented "wake word" or push-to-talk API; the model decides when to speak based on the audio it receives.
