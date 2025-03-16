using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DistributedSimulation.Networking
{
    public class WebSocketServer
    {
        private HttpListener _httpListener;
        private ConcurrentDictionary<int, WebSocket> _clients = new ConcurrentDictionary<int, WebSocket>();

        public async Task StartAsync(int port)
        {
            _httpListener = new HttpListener();
            _httpListener.Prefixes.Add($"http://localhost:{port}/");
            _httpListener.Start();
            Console.WriteLine($"WebSocket Server started at ws://localhost:{port}/");

            while (true)
            {
                var context = await _httpListener.GetContextAsync();
                if (context.Request.IsWebSocketRequest)
                {
                    var wsContext = await context.AcceptWebSocketAsync(null);
                    int clientId = _clients.Count + 1;
                    _clients.TryAdd(clientId, wsContext.WebSocket);

                    Console.WriteLine($"Client {clientId} connected.");
                    _ = HandleClientAsync(clientId, wsContext.WebSocket);
                }
                else
                {
                    context.Response.StatusCode = 400;
                    context.Response.Close();
                }
            }
        }

        private async Task HandleClientAsync(int clientId, WebSocket socket)
        {
            var buffer = new byte[1024];

            while (socket.State == WebSocketState.Open)
            {
                var result = await socket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Connection closed", CancellationToken.None);
                    _clients.TryRemove(clientId, out _);
                    Console.WriteLine($"Client {clientId} disconnected.");
                    break;
                }

                string message = Encoding.UTF8.GetString(buffer, 0, result.Count);
                Console.WriteLine($"Received from Client {clientId}: {message}");

                // 🔹 Propagar mensaje a todos los clientes conectados
                foreach (var client in _clients.Values)
                {
                    await client.SendAsync(new ArraySegment<byte>(buffer, 0, result.Count), WebSocketMessageType.Text, true, CancellationToken.None);
                }
            }
        }

    }
}
