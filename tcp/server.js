const { createTcpServer } = require('../../htcp');

const TCP_PORT = 8011;
const SHARED_CREDS = 'tcp-proxy-secret-token';

// 1. TCP Request Handler
const tcpRequestHandler = async (reqPayload) => {
  console.log(`[TCP Backend] Handled Request: ${reqPayload.method} ${reqPayload.url}`);
  console.log('[TCP Backend] Received Body:', reqPayload.body);

  return {
    status: 200,
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      message: 'Hello from Standalone TCP Backend Service',
      path: reqPayload.url,
      method: reqPayload.method,
      receivedBody: reqPayload.body
    })
  };
};

// 2. Authentication Validator
const authValidator = async (incomingToken, configuredCreds) => {
  return incomingToken === configuredCreds;
};

// 3. Start TCP Backend Service
const tcpBackendServer = createTcpServer(
  { port: TCP_PORT, credentials: SHARED_CREDS },
  tcpRequestHandler,
  authValidator
);

console.log(`================================================================`);
console.log(`  TCP Backend Server running on port ${TCP_PORT}`);
console.log(`================================================================`);

// Graceful Shutdown Handler
process.on('SIGINT', () => {
  console.log('\n[TCP Backend] Shutting down...');
  tcpBackendServer.close();
  process.exit(0);
});