const { createTcpClient } = require('../../htcp');
const { createHttpServer, sendHttpRequest } = require('../../httpm');

const TCP_BACKEND_PORT = 8011;
const HTTP_PROXY_PORT = 9013;
const SHARED_CREDS = 'tcp-proxy-secret-token';

async function startHttpTcpProxy() {
  console.log('================================================================');
  console.log('  HTTP Reverse Proxy Gateway (Forwarding to TCP Backend)');
  console.log('================================================================\n');

  // 1. Initialize TCP Client targeting the TCP Backend Server
  const tcpProxyClient = createTcpClient({
    port: TCP_BACKEND_PORT,
    credentials: SHARED_CREDS
  });

  // 2. Define Proxy Handler to translate HTTP to TCP Frames
  const tcpProxyHandler = async (httpRequestDetails) => {
    console.log(`[HTTP Proxy -> TCP Client] Forwarding ${httpRequestDetails.method} ${httpRequestDetails.url}`);

    const tcpResponse = await tcpProxyClient.sendHttpRequestPayload(httpRequestDetails);

    return {
      protocolClient: tcpProxyClient,
      response: tcpResponse
    };
  };

  // 3. Start HTTP Reverse Proxy Gateway
  const httpServer = createHttpServer(
    { httpPort: HTTP_PROXY_PORT },
    tcpProxyHandler
  );

  console.log(`[HTTP Proxy] Gateway listening on http://127.0.0.1:${HTTP_PROXY_PORT}`);

  // Wait briefly for setup
  await new Promise((r) => setTimeout(r, 300));

  // 4. Send Test Request via HTTP Client
  console.log('\n--- Sending Request from Client to HTTP Reverse Proxy ---');
  try {
    const response = await sendHttpRequest({
      targetUrl: `http://127.0.0.1:${HTTP_PROXY_PORT}/api/tcp/gateway`,
      method: 'POST',
      body: { metric: 'temperature', val: 24.8 }
    });

    console.log('[Client] Proxy Status Code:', response.statusCode);
    console.log('[Client] Proxy Response Body:', response.body);
  } catch (err) {
    console.error('[Client] Request Error:', err.message);
  }

  // Teardown
  setTimeout(() => {
    httpServer.server.close();
    tcpProxyClient.close();
    console.log('\n================================================================\n');
  }, 500);
}

startHttpTcpProxy();