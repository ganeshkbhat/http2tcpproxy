const net = require('net');
const tls = require('tls');

// ============================================================================
// LENGTH-PREFIXED STREAM PARSER UTILITIES
// ============================================================================

/**
 * Encapsulates a payload buffer with a 4-byte big-endian length prefix.
 *
 * @param {Buffer} buffer - Raw payload buffer to frame.
 * @returns {Buffer} Formatted buffer [ 4-byte Length | Payload Data ].
 */
function frameMessage(buffer) {
  const lengthBuffer = Buffer.alloc(4);
  lengthBuffer.writeUInt32BE(buffer.length, 0);
  return Buffer.concat([lengthBuffer, buffer]);
}

/**
 * Parses length-prefixed incoming binary chunk streams from a stream socket.
 *
 * @param {Buffer} bufferStore - Accumulated stream buffer.
 * @param {Function} onMessage - Callback triggered for each fully decoded message buffer.
 * @returns {Buffer} Remaining unparsed chunk buffer.
 */
function parseStreamFrames(bufferStore, onMessage) {
  while (bufferStore.length >= 4) {
    const messageLength = bufferStore.readUInt32BE(0);
    const totalFrameLength = 4 + messageLength;

    if (bufferStore.length < totalFrameLength) {
      break; // Complete frame hasn't arrived yet
    }

    const messageBuffer = bufferStore.slice(4, totalFrameLength);
    bufferStore = bufferStore.slice(totalFrameLength);

    onMessage(messageBuffer);
  }
  return bufferStore;
}

/**
 * Common handler generator for decoding length-prefixed incoming frames on a socket.
 *
 * @param {Object} socket - Active Net/TLS Socket.
 * @param {Function} requestHandler - Processing logic for valid payload requests.
 * @param {Function} [authenticateHook] - Optional authentication callback.
 * @param {Object|null} expectedCredentials - Credentials configured on the server.
 */
function handleSocketDataStream(socket, requestHandler, authenticateHook, expectedCredentials) {
  let accumulatedBuffer = Buffer.alloc(0);

  socket.on('data', (chunk) => {
    accumulatedBuffer = Buffer.concat([accumulatedBuffer, chunk]);

    accumulatedBuffer = parseStreamFrames(accumulatedBuffer, async (messageBuffer) => {
      try {
        const rawMessage = messageBuffer.toString('utf-8');
        const packet = JSON.parse(rawMessage);

        const { credentials, requestId, payload } = packet;

        // Step 1: Authenticate incoming packet credentials if hook is provided
        if (typeof authenticateHook === 'function') {
          const isAuthorized = await authenticateHook(credentials, expectedCredentials);
          if (!isAuthorized) {
            const authErrorResponse = frameMessage(Buffer.from(JSON.stringify({
              requestId: requestId,
              status: 401,
              headers: { 'content-type': 'application/json' },
              body: JSON.stringify({ error: 'Unauthorized protocol access' })
            })));
            socket.write(authErrorResponse);
            return;
          }
        }

        // Step 2: Invoke server application request handler
        const responsePayload = await requestHandler(payload);

        // Step 3: Format and transmit framed response packet back over socket
        const responsePacket = {
          requestId: requestId,
          status: responsePayload.status || 200,
          headers: responsePayload.headers || { 'content-type': 'application/json' },
          body: responsePayload.body || ''
        };

        const framedResponse = frameMessage(Buffer.from(JSON.stringify(responsePacket)));
        socket.write(framedResponse);
      } catch (err) {
        const errorPacket = frameMessage(Buffer.from(JSON.stringify({
          status: 500,
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ error: 'Stream Processing Error', details: err.message })
        })));
        socket.write(errorPacket);
      }
    });
  });

  socket.on('error', (err) => {
    if (err.code !== 'ECONNRESET') {
      console.error('[Socket Error]:', err.message);
    }
  });
}

// ============================================================================
// TCP SERVER MODULE
// ============================================================================

/**
 * Creates and starts a length-prefixed TCP RPC/Proxy Server.
 *
 * @param {Object} options - Options containing port, host, and optional credentials.
 * @param {Function} requestHandler - Asynchronous processor function `async (requestPayload) => responsePayload`.
 * @param {Function} [authenticateHook] - Optional authentication callback `async (incomingCreds, expectedCreds) => boolean`.
 * @returns {net.Server} Node.js `net.Server` instance.
 */
function createTcpServer(options = {}, requestHandler, authenticateHook) {
  const port = options.tcpPort || options.port || 7000;
  const host = options.tcpHost || options.host || '0.0.0.0';
  const expectedCredentials = options.tcpCredentials || options.credentials || null;

  const server = net.createServer((socket) => {
    handleSocketDataStream(socket, requestHandler, authenticateHook, expectedCredentials);
  });

  server.listen(port, host, () => {
    console.log(`[TCP Server] Listening on ${host}:${port}`);
  });

  return server;
}

// ============================================================================
// TLS SERVER MODULE
// ============================================================================

/**
 * Creates and starts a length-prefixed secure TLS/mTLS RPC/Proxy Server.
 *
 * @param {Object} options - Options containing cert, key, ca, port, host, requestCert, rejectUnauthorized.
 * @param {Function} requestHandler - Asynchronous processor function `async (requestPayload) => responsePayload`.
 * @param {Function} [authenticateHook] - Optional authentication callback.
 * @returns {tls.Server} Node.js `tls.Server` instance.
 */
function createTlsServer(options = {}, requestHandler, authenticateHook) {
  const port = options.tlsPort || options.port || 7001;
  const host = options.tlsHost || options.host || '0.0.0.0';
  const expectedCredentials = options.tlsCredentials || options.credentials || null;

  const tlsOptions = {
    key: options.key,
    cert: options.cert,
    ca: options.ca,
    requestCert: options.requestCert !== undefined ? options.requestCert : false,
    rejectUnauthorized: options.rejectUnauthorized !== undefined ? options.rejectUnauthorized : false
  };

  const server = tls.createServer(tlsOptions, (socket) => {
    handleSocketDataStream(socket, requestHandler, authenticateHook, expectedCredentials);
  });

  server.listen(port, host, () => {
    console.log(`[TLS Server] Listening on ${host}:${port}`);
  });

  return server;
}

// ============================================================================
// ABSTRACT SOCKET CLIENT FACTORY (Function / Closure-Based)
// ============================================================================

/**
 * Base stream client factory function that manages framed JSON RPC requests over stream connections.
 *
 * @param {Function} connectFn - Connection initialization function `(onConnect) => socket`.
 * @param {Object} options - Common options `{ credentials }`.
 * @returns {Object} `{ sendHttpRequestPayload: Function, close: Function }`
 */
function createBaseStreamClient(connectFn, options = {}) {
  const credentials = options.protocolCredentials || options.tcpCredentials || options.tlsCredentials || options.credentials || null;

  let socket = null;
  const pendingRequests = new Map();
  let requestCounter = 0;
  let accumulatedBuffer = Buffer.alloc(0);

  function ensureConnection() {
    return new Promise((resolve, reject) => {
      if (socket && !socket.destroyed) {
        return resolve(socket);
      }

      try {
        socket = connectFn(() => {
          resolve(socket);
        });
      } catch (err) {
        return reject(err);
      }

      socket.on('data', (chunk) => {
        accumulatedBuffer = Buffer.concat([accumulatedBuffer, chunk]);

        accumulatedBuffer = parseStreamFrames(accumulatedBuffer, (messageBuffer) => {
          try {
            const responsePacket = JSON.parse(messageBuffer.toString('utf-8'));
            const { requestId } = responsePacket;

            if (requestId && pendingRequests.has(requestId)) {
              const { resolve: resolvePending } = pendingRequests.get(requestId);
              pendingRequests.delete(requestId);
              resolvePending(responsePacket);
            }
          } catch (e) {
            console.error('[Stream Client] Frame parse error:', e.message);
          }
        });
      });

      socket.on('error', (err) => {
        for (const [id, { reject: rejectPending }] of pendingRequests.entries()) {
          rejectPending(err);
        }
        pendingRequests.clear();
      });

      socket.on('close', () => {
        socket = null;
      });
    });
  }

  function sendHttpRequestPayload(httpRequestDetails, timeout = 5000) {
    return ensureConnection().then(() => {
      requestCounter = (requestCounter + 1) % 1000000;
      const requestId = `req_${Date.now()}_${requestCounter}`;

      const packet = {
        credentials: credentials,
        requestId: requestId,
        payload: httpRequestDetails
      };

      const framedPayload = frameMessage(Buffer.from(JSON.stringify(packet)));

      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          if (pendingRequests.has(requestId)) {
            pendingRequests.delete(requestId);
            reject(new Error(`Stream RPC Request timed out after ${timeout}ms`));
          }
        }, timeout);

        pendingRequests.set(requestId, {
          resolve: (response) => {
            clearTimeout(timer);
            resolve(response);
          },
          reject: (err) => {
            clearTimeout(timer);
            reject(err);
          }
        });

        socket.write(framedPayload, (err) => {
          if (err) {
            clearTimeout(timer);
            pendingRequests.delete(requestId);
            reject(err);
          }
        });
      });
    });
  }

  function close() {
    if (socket && !socket.destroyed) {
      socket.destroy();
      socket = null;
    }
  }

  return {
    sendHttpRequestPayload: sendHttpRequestPayload,
    close: close
  };
}

// ============================================================================
// TCP CLIENT MODULE
// ============================================================================

/**
 * Creates a TCP Client instance leveraging closure state management.
 *
 * @param {Object} options - Client configuration `{ host, port, credentials }`.
 * @returns {Object} Object containing `sendHttpRequestPayload` and `close` functions.
 */
function createTcpClient(options = {}) {
  const host = options.tcpHost || options.protocolHost || options.host || '127.0.0.1';
  const port = options.tcpPort || options.protocolPort || options.port || 7000;

  const connectFn = (onConnect) => net.createConnection({ host: host, port: port }, onConnect);
  return createBaseStreamClient(connectFn, options);
}

// ============================================================================
// TLS CLIENT MODULE
// ============================================================================

/**
 * Creates a TLS/mTLS Client instance leveraging closure state management.
 *
 * @param {Object} options - Client configuration `{ host, port, cert, key, ca, rejectUnauthorized }`.
 * @returns {Object} Object containing `sendHttpRequestPayload` and `close` functions.
 */
function createTlsClient(options = {}) {
  const host = options.tlsHost || options.protocolHost || options.host || '127.0.0.1';
  const port = options.tlsPort || options.protocolPort || options.port || 7001;

  const connectFn = (onConnect) => {
    return tls.connect({
      host: host,
      port: port,
      key: options.key,
      cert: options.cert,
      ca: options.ca,
      rejectUnauthorized: options.rejectUnauthorized !== undefined ? options.rejectUnauthorized : false
    }, onConnect);
  };

  return createBaseStreamClient(connectFn, options);
}

module.exports = {
  createTcpServer: createTcpServer,
  createTcpClient: createTcpClient,
  createTlsServer: createTlsServer,
  createTlsClient: createTlsClient
};