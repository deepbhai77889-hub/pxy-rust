# pxy-rust: Ultra-Fast NVIDIA AI Gateway

🚀 **High-Performance Rust Proxy with Dual API-Key Auto-Rotation, 1M Context Streaming, and Complete OpenAI $\leftrightarrow$ Anthropic Translation.**

---

## ⚡ Features

1. **Dual NVIDIA API Key Pool & Instant Auto-Failover**:
   - `Key 1`: `nvapi-2gc6jRc4...`
   - `Key 2`: `nvapi-Vu0NYNNX...`
   - Agar kisi bhi key pe **Rate-Limit (429), 403, 401, ya 503** aata hai, ye instant next working key par switch ho jata hai.

2. **Dummy API Key Acceptance**:
   - Client request me `Authorization: Bearer <ANYTHING>` pass kar sakta hai. Proxy automatically authenticated NVIDIA keys inject karegi.

3. **Full Bidirectional Protocol Translation**:
   - **OpenAI Format**: `/v1/chat/completions` (Supports standard tools, functions, streaming SSE, and images).
   - **Anthropic Claude Format**: `/v1/messages` (Converts Claude messages, system prompts, `tool_use`, and base64 vision images into NVIDIA/OpenAI schema transparently).

4. **Zero-Copy & 1 Million Token Streaming**:
   - Built on `Axum`, `Tokio`, and `Reqwest` without GC pauses or memory bloat.

---

## 🛠️ Usage Examples

### 1. OpenAI Chat Completions (Any Client / cURL / 9router / LangChain)
```bash
curl -N -X POST "https://<YOUR-WORKFLOW-URL>.trycloudflare.com/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer dummy-key" \
  -d '{
    "model": "moonshotai/kimi-k3",
    "messages": [{"role": "user", "content": "Hello!"}],
    "stream": true
  }'
```

### 2. Anthropic Messages (Claude Desktop / Claude Code / Cline / Roo Code)
```bash
curl -N -X POST "https://<YOUR-WORKFLOW-URL>.trycloudflare.com/v1/messages" \
  -H "Content-Type: application/json" \
  -H "x-api-key: dummy-key" \
  -H "anthropic-version: 2023-06-01" \
  -d '{
    "model": "claude-3-5-sonnet-20241022",
    "messages": [{"role": "user", "content": "Hello Claude!"}],
    "stream": true
  }'
```
