export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // API Key Rotation Pool
    const KEYS = [
      "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",
      "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"
    ];

    // CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
          "Access-Control-Allow-Headers": "*"
        }
      });
    }

    if (url.pathname === "/v1/models" || url.pathname === "/models") {
      return new Response(JSON.stringify({
        object: "list",
        data: [
          { id: "moonshotai/kimi-k3", object: "model", owned_by: "nvidia" },
          { id: "deepseek-ai/deepseek-v3", object: "model", owned_by: "nvidia" },
          { id: "meta/llama-3.3-70b-instruct", object: "model", owned_by: "nvidia" },
          { id: "mistralai/mistral-large-2-instruct", object: "model", owned_by: "nvidia" },
          { id: "claude-3-5-sonnet-20241022", object: "model", owned_by: "anthropic-translated" }
        ]
      }), {
        headers: { "content-type": "application/json", "Access-Control-Allow-Origin": "*" }
      });
    }

    if (request.method !== "POST") {
      return new Response(JSON.stringify({ error: "Method not allowed" }), { status: 405 });
    }

    const path = url.pathname;
    const isAnthropic = path.includes("/messages");
    const isChat = path.includes("/chat/completions");

    if (!isAnthropic && !isChat) {
      return new Response(JSON.stringify({ error: "Invalid endpoint. Use /v1/chat/completions or /v1/messages" }), { status: 404 });
    }

    let reqBody;
    try {
      reqBody = await request.json();
    } catch {
      return new Response(JSON.stringify({ error: "Invalid JSON body" }), { status: 400 });
    }

    let targetPayload;
    let actualModel = "moonshotai/kimi-k3";

    if (isAnthropic) {
      actualModel = reqBody.model?.replace(/^anthropic\//, "").replace(/^claude-/, "") || "moonshotai/kimi-k3";
      if (!actualModel || actualModel.startsWith("3")) actualModel = "moonshotai/kimi-k3";
      targetPayload = translateAnthropicToOpenAI(reqBody, actualModel);
    } else {
      reqBody.model = reqBody.model?.replace(/^nvidia\//, "").replace(/^openai\//, "") || "moonshotai/kimi-k3";
      actualModel = reqBody.model;
      targetPayload = reqBody;
    }

    // Call NVIDIA with auto key failover
    let response;
    for (let i = 0; i < KEYS.length; i++) {
      const apiKey = KEYS[i];
      try {
        response = await fetch("https://integrate.api.nvidia.com/v1/chat/completions", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Authorization": `Bearer ${apiKey}`
          },
          body: JSON.stringify(targetPayload)
        });

        if (response.status === 429 || response.status === 403 || response.status === 503) {
          continue; // Try next key immediately
        }
        break;
      } catch (err) {
        if (i === KEYS.length - 1) {
          return new Response(JSON.stringify({ error: err.message }), { status: 502 });
        }
      }
    }

    if (!response) {
      return new Response(JSON.stringify({ error: "All NVIDIA keys rate-limited" }), { status: 429 });
    }

    if (!isAnthropic) {
      // Direct stream or JSON forward for OpenAI
      return new Response(response.body, {
        status: response.status,
        headers: {
          "Content-Type": response.headers.get("content-type") || "application/json",
          "Access-Control-Allow-Origin": "*",
          "Cache-Control": "no-cache"
        }
      });
    }

    // Translate OpenAI -> Anthropic for Claude format
    if (reqBody.stream) {
      const { readable, writable } = new TransformStream();
      const writer = writable.getWriter();
      const encoder = new TextEncoder();

      (async () => {
        const msgId = "msg_" + crypto.randomUUID().replace(/-/g, "");
        await writer.write(encoder.encode(`event: message_start\ndata: ${JSON.stringify({
          type: "message_start",
          message: { id: msgId, type: "message", role: "assistant", content: [], model: actualModel, usage: { input_tokens: 10, output_tokens: 1 } }
        })}\n\n`));

        await writer.write(encoder.encode(`event: content_block_start\ndata: ${JSON.stringify({
          type: "content_block_start", index: 0, content_block: { type: "text", text: "" }
        })}\n\n`));

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split("\n");
          buffer = lines.pop() || "";

          for (const line of lines) {
            if (line.startsWith("data: ")) {
              const dataStr = line.slice(6).trim();
              if (dataStr === "[DONE]") continue;
              try {
                const parsed = JSON.parse(dataStr);
                const delta = parsed.choices?.[0]?.delta?.content;
                if (delta) {
                  await writer.write(encoder.encode(`event: content_block_delta\ndata: ${JSON.stringify({
                    type: "content_block_delta", index: 0, delta: { type: "text_delta", text: delta }
                  })}\n\n`));
                }
              } catch {}
            }
          }
        }

        await writer.write(encoder.encode(`event: content_block_stop\ndata: {"type":"content_block_stop","index":0}\n\n`));
        await writer.write(encoder.encode(`event: message_delta\ndata: ${JSON.stringify({ type: "message_delta", delta: { stop_reason: "end_turn" }, usage: { output_tokens: 20 } })}\n\n`));
        await writer.write(encoder.encode(`event: message_stop\ndata: {"type":"message_stop"}\n\n`));
        await writer.close();
      })();

      return new Response(readable, {
        headers: {
          "Content-Type": "text/event-stream; charset=utf-8",
          "Access-Control-Allow-Origin": "*",
          "Cache-Control": "no-cache"
        }
      });
    } else {
      const openAiData = await response.json();
      return new Response(JSON.stringify(translateOpenAIToAnthropic(openAiData, actualModel)), {
        headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
      });
    }
  }
};

function translateAnthropicToOpenAI(req, model) {
  const messages = [];
  if (req.system) {
    messages.push({ role: "system", content: typeof req.system === "string" ? req.system : req.system.map(s => s.text).join("\n") });
  }

  if (Array.isArray(req.messages)) {
    for (const m of req.messages) {
      if (typeof m.content === "string") {
        messages.push({ role: m.role, content: m.content });
      } else if (Array.isArray(m.content)) {
        const parts = [];
        for (const block of m.content) {
          if (block.type === "text") parts.push({ type: "text", text: block.text });
          else if (block.type === "image") {
            parts.push({
              type: "image_url",
              image_url: { url: `data:${block.source?.media_type || "image/jpeg"};base64,${block.source?.data || ""}` }
            });
          } else if (block.type === "tool_use") {
            messages.push({
              role: "assistant",
              tool_calls: [{ id: block.id, type: "function", function: { name: block.name, arguments: JSON.stringify(block.input || {}) } }]
            });
          } else if (block.type === "tool_result") {
            messages.push({ role: "tool", tool_call_id: block.tool_use_id, content: typeof block.content === "string" ? block.content : JSON.stringify(block.content) });
          }
        }
        if (parts.length > 0) messages.push({ role: m.role, content: parts });
      }
    }
  }

  const payload = {
    model: model,
    messages: messages,
    stream: !!req.stream,
    temperature: req.temperature ?? 0.7,
    max_tokens: req.max_tokens ?? 4096
  };

  if (Array.isArray(req.tools)) {
    payload.tools = req.tools.map(t => ({
      type: "function",
      function: { name: t.name, description: t.description, parameters: t.input_schema || { type: "object" } }
    }));
  }

  return payload;
}

function translateOpenAIToAnthropic(openai, model) {
  const choice = openai.choices?.[0]?.message || {};
  const content = [];
  if (choice.content) content.push({ type: "text", text: choice.content });
  if (Array.isArray(choice.tool_calls)) {
    for (const tc of choice.tool_calls) {
      content.push({
        type: "tool_use",
        id: tc.id || "call_1",
        name: tc.function?.name,
        input: JSON.parse(tc.function?.arguments || "{}")
      });
    }
  }
  return {
    id: "msg_" + crypto.randomUUID().replace(/-/g, ""),
    type: "message",
    role: "assistant",
    content: content,
    model: model,
    stop_reason: "end_turn",
    usage: {
      input_tokens: openai.usage?.prompt_tokens || 0,
      output_tokens: openai.usage?.completion_tokens || 0
    }
  };
}
