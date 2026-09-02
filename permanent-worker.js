export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // 1. Root / Health / Info Endpoint
    if (url.pathname === "/" || url.pathname === "/health") {
      return new Response(JSON.stringify({
        status: "active",
        service: "24/7 Permanent AI Gateway Relay",
        upstream_tunnel: env.UPSTREAM_URL || "auto"
      }), {
        headers: { "content-type": "application/json", "Access-Control-Allow-Origin": "*" }
      });
    }

    // 2. Secret Update Endpoint: Workflow updates live tunnel URL here automatically
    if (url.pathname === "/_update_tunnel" && request.method === "POST") {
      const authKey = request.headers.get("x-update-key");
      if (authKey !== (env.SYNC_SECRET || "pxy-rust-sync-key-77889")) {
        return new Response(JSON.stringify({ error: "Unauthorized" }), { status: 401 });
      }

      const body = await request.json();
      const newUrl = body.url?.replace(/\/$/, "");
      if (!newUrl) return new Response(JSON.stringify({ error: "Missing url" }), { status: 400 });

      if (env.KV_PROXY) {
        await env.KV_PROXY.put("LATEST_TUNNEL_URL", newUrl);
      }
      return new Response(JSON.stringify({ success: true, updated_to: newUrl }), {
        headers: { "content-type": "application/json" }
      });
    }

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

    // 3. Dynamic Forwarding to Latest GitHub Actions Runner Tunnel
    let targetBase = env.UPSTREAM_URL;
    if (env.KV_PROXY) {
      const kvUrl = await env.KV_PROXY.get("LATEST_TUNNEL_URL");
      if (kvUrl) targetBase = kvUrl;
    }

    if (!targetBase) {
      targetBase = "https://fires-abc-observed-motorcycle.trycloudflare.com";
    }

    const targetUrl = new URL(url.pathname + url.search, targetBase);
    const forwardHeaders = new Headers(request.headers);
    forwardHeaders.delete("host");

    const response = await fetch(targetUrl.toString(), {
      method: request.method,
      headers: forwardHeaders,
      body: request.method !== "GET" && request.method !== "HEAD" ? request.body : undefined,
      duplex: "half"
    });

    const responseHeaders = new Headers(response.headers);
    responseHeaders.set("Access-Control-Allow-Origin", "*");
    responseHeaders.set("x-accel-buffering", "no");

    return new Response(response.body, {
      status: response.status,
      headers: responseHeaders
    });
  }
};
