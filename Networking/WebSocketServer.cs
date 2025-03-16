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
        private CancellationTokenSource _cancellationTokenSource;
        private bool _isRunning = false;

        public event Action<string> OnServerMessageReceived;

        public async Task StartAsync(int port)
        {
            if (_isRunning)
                return;

            _isRunning = true;
            _cancellationTokenSource = new CancellationTokenSource();

            try
            {
                _httpListener = new HttpListener();
                _httpListener.Prefixes.Add($"http://localhost:{port}/");
                _httpListener.Start();

                LogMessage($"Servidor WebSocket iniciado en ws://localhost:{port}/");

                while (!_cancellationTokenSource.Token.IsCancellationRequested)
                {
                    HttpListenerContext context;

                    try
                    {
                        context = await _httpListener.GetContextAsync().WaitAsync(_cancellationTokenSource.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }

                    if (context.Request.IsWebSocketRequest)
                    {
                        ProcessWebSocketRequest(context);
                    }
                    else
                    {
                        context.Response.StatusCode = 400;
                        context.Response.Close();
                    }
                }
            }
            catch (HttpListenerException ex)
            {
                LogMessage($"Error en HttpListener: {ex.Message}");
            }
            catch (Exception ex)
            {
                LogMessage($"Error inesperado en el servidor: {ex.Message}");
            }
            finally
            {
                StopServer();
            }
        }

        private void ProcessWebSocketRequest(HttpListenerContext context)
        {
            _ = Task.Run(async () =>
            {
                WebSocketContext webSocketContext = null;
                WebSocket webSocket = null;

                try
                {
                    webSocketContext = await context.AcceptWebSocketAsync(null);
                    webSocket = webSocketContext.WebSocket;

                    int clientId = _clients.Count + 1;
                    _clients.TryAdd(clientId, webSocket);

                    LogMessage($"Cliente {clientId} conectado. Total de clientes: {_clients.Count}");

                    await HandleClientAsync(clientId, webSocket);
                }
                catch (Exception ex)
                {
                    LogMessage($"Error al manejar conexión WebSocket: {ex.Message}");

                    if (webSocket != null && webSocket.State == WebSocketState.Open)
                    {
                        await webSocket.CloseAsync(
                            WebSocketCloseStatus.InternalServerError,
                            "Error interno del servidor",
                            CancellationToken.None);
                    }
                }
            });
        }

        private async Task HandleClientAsync(int clientId, WebSocket socket)
        {
            var buffer = new byte[4096];

            try
            {
                while (socket.State == WebSocketState.Open && !_cancellationTokenSource.Token.IsCancellationRequested)
                {
                    WebSocketReceiveResult result;

                    try
                    {
                        result = await socket.ReceiveAsync(
                            new ArraySegment<byte>(buffer),
                            _cancellationTokenSource.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (WebSocketException wse)
                    {
                        LogMessage($"Error de WebSocket para cliente {clientId}: {wse.Message}");
                        break;
                    }

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        await socket.CloseAsync(
                            WebSocketCloseStatus.NormalClosure,
                            "Cierre normal",
                            CancellationToken.None);

                        _clients.TryRemove(clientId, out _);
                        LogMessage($"Cliente {clientId} desconectado. Total de clientes: {_clients.Count}");
                        break;
                    }

                    string message = Encoding.UTF8.GetString(buffer, 0, result.Count);
                    LogMessage($"Recibido de Cliente {clientId}: {message}");

                    // Propagar mensaje a todos los clientes conectados
                    await BroadcastMessageAsync(buffer, result.Count);
                }
            }
            catch (Exception ex)
            {
                LogMessage($"Error al manejar cliente {clientId}: {ex.Message}");
            }
            finally
            {
                if (socket.State != WebSocketState.Closed)
                {
                    try
                    {
                        await socket.CloseAsync(
                            WebSocketCloseStatus.NormalClosure,
                            "Finalización de la conexión",
                            CancellationToken.None);
                    }
                    catch (Exception ex)
                    {
                        LogMessage($"Error al cerrar WebSocket para cliente {clientId}: {ex.Message}");
                    }
                }

                _clients.TryRemove(clientId, out _);
                LogMessage($"Cliente {clientId} desconectado. Total de clientes: {_clients.Count}");
            }
        }

        private async Task BroadcastMessageAsync(byte[] buffer, int count)
        {
            var tasks = new List<Task>();
            var failedClients = new List<int>();

            foreach (var client in _clients)
            {
                if (client.Value.State == WebSocketState.Open)
                {
                    try
                    {
                        tasks.Add(client.Value.SendAsync(
                            new ArraySegment<byte>(buffer, 0, count),
                            WebSocketMessageType.Text,
                            true,
                            CancellationToken.None));
                    }
                    catch (Exception)
                    {
                        failedClients.Add(client.Key);
                    }
                }
                else
                {
                    failedClients.Add(client.Key);
                }
            }

            try
            {
                await Task.WhenAll(tasks);
            }
            catch (Exception ex)
            {
                LogMessage($"Error al difundir mensaje: {ex.Message}");
            }

            // Limpiar clientes con error
            foreach (var clientId in failedClients)
            {
                _clients.TryRemove(clientId, out _);
                LogMessage($"Cliente {clientId} eliminado por error en envío.");
            }
        }

        public void StopServer()
        {
            if (!_isRunning)
                return;

            _isRunning = false;

            try
            {
                _cancellationTokenSource?.Cancel();

                // Cerrar todas las conexiones de clientes
                foreach (var client in _clients)
                {
                    try
                    {
                        if (client.Value.State == WebSocketState.Open)
                        {
                            client.Value.CloseAsync(
                                WebSocketCloseStatus.NormalClosure,
                                "Servidor cerrando",
                                CancellationToken.None).Wait(1000);
                        }
                    }
                    catch
                    {
                        // Ignorar errores al cerrar conexiones
                    }
                }

                _clients.Clear();
                _httpListener?.Stop();
                _httpListener?.Close();

                LogMessage("Servidor WebSocket detenido.");
            }
            catch (Exception ex)
            {
                LogMessage($"Error al detener servidor: {ex.Message}");
            }
        }

        private void LogMessage(string message)
        {
            Console.WriteLine($"[WebSocketServer] {message}");
            OnServerMessageReceived?.Invoke(message);
        }
    }
}