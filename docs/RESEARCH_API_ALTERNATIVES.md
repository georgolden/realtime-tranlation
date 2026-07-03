# API Alternatives Research (May 2026)

Competitive analysis for the three services in the realtime translation pipeline.
Research conducted across 15+ independent benchmarks, vendor comparisons, and
production deployment reports from Q4 2025–Q2 2026.

---

## 1. Speech-to-Text

**Current: Deepgram Nova-3 → Keep.**

Deepgram remains the latency leader for real-time streaming STT. No competitor
beats it on the latency × accuracy combination for English.

| Provider | Model | Streaming Latency | WER (English) | Price/min |
|----------|-------|-------------------|---------------|-----------|
| **Deepgram** | Nova-3/4 | **150–280ms** | **4.1–5.3%** | $0.0077 |
| AssemblyAI | Universal-3 Pro | 280–400ms | 4.5–5.0% | $0.0075 |
| OpenAI | Whisper V4 (realtime) | ~280ms | 8.1% | $0.006 |
| Groq | Whisper Large v3 Turbo | ~200ms | 8.5% | $0.02/hr |
| Google | Chirp 3 | 350–700ms | — | $0.016 |

**Key trade-offs:**
- Deepgram: best latency, mature WebSocket SDK, native turn detection (Flux)
- AssemblyAI: lower hallucination rate (30% below Whisper), bundled audio intelligence (sentiment, PII, diarization), cheaper at scale ($0.15/hr vs $0.46/hr streaming)
- Whisper V4: best multilingual (99 languages), real-time API is new and immature
- Groq: fastest Whisper variant but WER gap of ~4 points vs Deepgram

**Verdict:** Deepgram is the right choice. AssemblyAI worth evaluating if
accuracy/diarization matters more than sub-300ms latency.

---

## 2. Translation

**Current: DeepL → Consider Google Cloud Translation v3.**

DeepL has the best European-language quality but **~500ms–1s latency per segment**
is a problem for real-time pipelines. Google closed the quality gap significantly
with December 2025 Gemini integration.

| Provider | COMET Score | Latency/segment | Price/M chars | Languages |
|----------|-------------|-----------------|---------------|-----------|
| **DeepL** | **0.884** | **500–1000ms** | $25 | 33→100+ |
| Google Cloud v3 | 0.871 | **50–150ms** | $20 | 130+ |
| Microsoft Translator | 0.850 | **90ms** | $10 | 100+ |
| Amazon Translate | 0.870 | 200ms | $15 | 75 |
| GPT-4o | 0.879 | 500–2000ms | expensive | 90+ |

**Key findings:**
- Google Translate + Gemini: state-of-the-art on WMT25 benchmark, specifically
  targeting idioms/slang/contextual meaning (historical DeepL strengths)
- Microsoft: fastest per-segment translation (median 0.09s), budget-friendly
- DeepL: still wins pure quality for European pairs (5–7 BLEU points above
  Google on EN→DE/FR/ES in some benchmarks), but latency is 5–10× worse
- For real-time: Google or Microsoft are the pragmatic choices

**Verdict:** For a real-time pipeline, Google Cloud Translation v3 cuts
translation latency from ~1s to ~100ms with quality now competitive.
Worth A/B testing against DeepL on your actual language pairs.

---

## 3. Text-to-Speech

**Current: ElevenLabs → Consider Cartesia Sonic or Deepgram Aura.**

ElevenLabs has industry-best voice quality (MOS 4.7+) and cloning, but
TTFA (time-to-first-audio) of 200–500ms. Faster alternatives exist for
real-time conversation.

| Provider | Model | TTFA | Quality | Price | Notes |
|----------|-------|------|---------|-------|-------|
| **ElevenLabs** | Flash v2.5 | 200–500ms | ⭐⭐⭐⭐⭐ | ~$0.30/1K chars | Best quality, voice cloning |
| **Cartesia** | Sonic | **<90ms** | ⭐⭐⭐½ | pay-per-use | Purpose-built for real-time voice agents |
| Deepgram | Aura-2 | ~200ms | ⭐⭐⭐ | $0.015/1K chars | Single-vendor STT+TTS |
| Smallest.ai | Lightning | <300ms | ⭐⭐⭐ | 3× cheaper | Built for real-time voice agents |
| OpenAI | TTS | ~400ms | ⭐⭐⭐½ | $15/1M chars | Simple, cheap, no voice cloning |

**Key findings:**
- Cartesia Sonic at sub-90ms TTFA is purpose-built for real-time conversational
  AI. Streaming-first WebSocket architecture. The latency leader.
- Deepgram Aura: single-vendor STT+TTS simplifies architecture (one API key,
  one SDK, single WS connection possible). Saves ~200ms vs ElevenLabs.
- Smallest.ai: cost leader for real-time at scale with competitive latency.
- ElevenLabs: still best for quality/branded voice. Use when naturalness
  matters more than latency.

**Verdict:** Cartesia Sonic could shave ~300ms off total pipeline latency.
Deepgram Aura is the pragmatic choice if you want one provider for both
STT and TTS.

---

## 4. Alternative Architectures

### OpenAI Realtime API (GPT-4o-realtime)
End-to-end speech-to-speech in one bidirectional WebSocket. The model handles
STT + translation + TTS in a single stream. Sub-second total latency. One API
key, one billing relationship. Trade-off: no custom voice cloning, English-first,
more expensive at scale.

### Kyutai Hibiki / Hibiki-Zero
Open-source simultaneous speech translation (CC-BY 4.0). Direct speech→speech
without intermediate text. FR→EN currently. 12.5Hz framerate, sub-second
latency. Hibiki-Zero uses RL to optimize latency × quality. Tracks as it
expands language pairs.

### SeamlessM4T v2 (Meta)
Open-source, 100+ languages, speech+text in/out. Self-hostable. Competitive
quality with commercial APIs. For data sovereignty or cost at scale.

### StreamSpeech
Academic SOTA for simultaneous speech translation — unified model handling
streaming ASR, simultaneous S2TT, and real-time TTS. Demonstrates the
single-model approach is viable.

---

## 5. Summary

| Layer | Current | Best Alternative | Impact |
|-------|---------|------------------|--------|
| STT | Deepgram | — | No change needed. Latency leader. |
| Translation | DeepL | Google Cloud v3 | Saves **~500–900ms** per call, quality now competitive |
| TTS | ElevenLabs | Cartesia Sonic or Deepgram Aura | Saves **~200–300ms** TTFA, simplifies architecture |

**Biggest single win:** Replacing DeepL with Google Translate for translation.
