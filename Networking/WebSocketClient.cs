using System;
using System.Collections.Generic;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DistributedSimulation.Networking
{
    public class WebSocketClient
    {
        private ClientWebSocket _clientWebSocket;
        private int _lamportClock = 0;
        private HashSet<int> _okResponses = new HashSet<int>();
        private bool _inCriticalSection = false;
        private bool _waitingForAccess = false;
        private CancellationTokenSource _cancellationTokenSource;
        private bool _isReconnecting = false;
        private Queue<string> _pendingResponses = new Queue<string>();

        public event Action<string> OnMessageReceived;
        public int ClientId { get; private set; }
        public int TotalClients { get; set; }
        public int LamportClock => _lamportClock;
        public bool IsInCriticalSection => _inCriticalSection;
        public bool IsWaitingForAccess => _waitingForAccess;

        private string _serverUrl;

        public async Task ConnectAsync(string url, int clientId, int totalClients)
        {
            _serverUrl = url;
            ClientId = clientId;
            TotalClients = totalClients;

            await EstablishConnectionAsync();
        }

        private async Task EstablishConnectionAsync()
        {
            try
            {
                _clientWebSocket = new ClientWebSocket();
                _clientWebSocket.Options.KeepAliveInterval = TimeSpan.FromSeconds(30);

                _cancellationTokenSource = new CancellationTokenSource();

                await _clientWebSocket.ConnectAsync(new Uri(_serverUrl), CancellationToken.None);

                OnMessageReceived?.Invoke($"Conectado al servidor WebSocket. Estado: {_clientWebSocket.State}");
            }
            catch (Exception ex)
            {
                OnMessageReceived?.Invoke($"Error al establecer conexión: {ex.Message}");
                throw;
            }
        }

        public async Task SendRequestAsync()
        {
            if (_waitingForAccess || _inCriticalSection)
            {
                OnMessageReceived?.Invoke($"Cliente {ClientId} ya está esperando o en sección crítica");
                return;
            }

            _lamportClock++;
            _okResponses.Clear();
            _waitingForAccess = true;
            string message = $"REQUEST {ClientId} {_lamportClock}";
            await SendMessageAsync(message);

            // Si somos el único cliente, podemos entrar directamente en la sección crítica
            if (TotalClients == 1)
            {
                await EnterCriticalSectionAsync();
            }
        }

        private async Task EnterCriticalSectionAsync()
        {
            _inCriticalSection = true;
            _waitingForAccess = false;
            OnMessageReceived?.Invoke($"ENTERED Critical Section. Accediendo al recurso compartido...");

            // Simular trabajo en la sección crítica
            await Task.Delay(3000);

            // Liberar la sección crítica
            await SendReleaseAsync();
        }

        public async Task SendReleaseAsync()
        {
            _lamportClock++;
            _inCriticalSection = false;
            _waitingForAccess = false;
            string message = $"RELEASE {ClientId} {_lamportClock}";
            await SendMessageAsync(message);
        }

        private async Task SendMessageAsync(string message)
        {
            if (_clientWebSocket == null || _clientWebSocket.State != WebSocketState.Open)
            {
                if (!_isReconnecting)
                {
                    _isReconnecting = true;
                    OnMessageReceived?.Invoke($"WebSocket no está abierto. Intentando reconectar...");

                    try
                    {
                        await EstablishConnectionAsync();
                        _isReconnecting = false;

                        // Procesar mensajes pendientes
                        while (_pendingResponses.Count > 0)
                        {
                            string pendingMessage = _pendingResponses.Dequeue();
                            await SendMessageAsync(pendingMessage);
                        }
                    }
                    catch (Exception ex)
                    {
                        _isReconnecting = false;
                        OnMessageReceived?.Invoke($"Error al reconectar: {ex.Message}");
                        _pendingResponses.Enqueue(message); // Guardar mensaje para reenviar después
                        return;
                    }
                }
                else
                {
                    _pendingResponses.Enqueue(message);
                    return;
                }
            }

            try
            {
                var bytes = Encoding.UTF8.GetBytes(message);
                await _clientWebSocket.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, CancellationToken.None);
                OnMessageReceived?.Invoke($"Enviado: {message} (Reloj: {_lamportClock})");
            }
            catch (Exception ex)
            {
                OnMessageReceived?.Invoke($"Error al enviar mensaje: {ex.Message}");
                _pendingResponses.Enqueue(message);
            }
        }

        public async Task ReceiveMessagesAsync()
        {
            while (true)
            {
                try
                {
                    if (_clientWebSocket == null || _clientWebSocket.State != WebSocketState.Open)
                    {
                        if (!_isReconnecting)
                        {
                            await Task.Delay(1000); // Esperar antes de intentar reconectar
                            continue;
                        }
                        else
                        {
                            await Task.Delay(5000); // Esperar más tiempo si ya está intentando reconectar
                            continue;
                        }
                    }

                    var buffer = new byte[4096]; // Buffer más grande para mensajes
                    var result = await _clientWebSocket.ReceiveAsync(
                        new ArraySegment<byte>(buffer),
                        CancellationToken.None); // Usar un token que no se cancele

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        await _clientWebSocket.CloseAsync(
                            WebSocketCloseStatus.NormalClosure,
                            "Conexión cerrada por el servidor",
                            CancellationToken.None);

                        OnMessageReceived?.Invoke("Conexión cerrada por el servidor. Intentando reconectar...");
                        await Task.Delay(1000);

                        if (!_isReconnecting)
                        {
                            _isReconnecting = true;
                            await EstablishConnectionAsync();
                            _isReconnecting = false;
                        }

                        continue;
                    }

                    string message = Encoding.UTF8.GetString(buffer, 0, result.Count);

                    // Solo procesar mensajes válidos (evitar ruido)
                    if (!string.IsNullOrWhiteSpace(message))
                    {
                        OnMessageReceived?.Invoke($"Recibido: {message}");
                        await ProcessMessageAsync(message);
                    }
                }
                catch (OperationCanceledException)
                {
                    // Normal cuando se cancela, reintentar
                    await Task.Delay(1000);
                }
                catch (WebSocketException wse)
                {
                    OnMessageReceived?.Invoke($"Error WebSocket: {wse.Message}. Intentando reconectar...");

                    if (!_isReconnecting)
                    {
                        _isReconnecting = true;
                        await Task.Delay(2000); // Esperar antes de reconectar
                        await EstablishConnectionAsync();
                        _isReconnecting = false;
                    }
                }
                catch (Exception ex)
                {
                    OnMessageReceived?.Invoke($"Error al recibir mensajes: {ex.Message}");
                    await Task.Delay(1000);
                }
            }
        }

        private async Task ProcessMessageAsync(string message)
        {
            try
            {
                string[] parts = message.Split(' ');
                if (parts.Length < 3) return; // Mensaje inválido

                string messageType = parts[0];
                int senderId = int.Parse(parts[1]);
                int timestamp = int.Parse(parts[2]);

                // Actualizar reloj de Lamport
                _lamportClock = Math.Max(_lamportClock, timestamp) + 1;

                // Procesar mensajes según su tipo
                if (messageType == "REQUEST" && senderId != ClientId)
                {
                    // Si recibimos un REQUEST, decidimos si responder OK inmediatamente o esperar
                    bool shouldSendOK = !_waitingForAccess ||
                                        (_waitingForAccess && (
                                            timestamp < _lamportClock ||
                                            (timestamp == _lamportClock && senderId < ClientId)
                                        ));

                    if (shouldSendOK)
                    {
                        string response = $"OK {ClientId} {senderId} {_lamportClock}";
                        await SendMessageAsync(response);
                    }
                    else
                    {
                        OnMessageReceived?.Invoke($"Retrasando respuesta a {senderId} - tenemos mayor prioridad (nuestro reloj: {_lamportClock}, su reloj: {timestamp})");
                    }
                }
                else if (messageType == "OK")
                {
                    // Solo si el ok es para nosotros y estamos esperando
                    if (_waitingForAccess && parts.Length >= 3)
                    {
                        int okSender = int.Parse(parts[1]);
                        int okTarget = int.Parse(parts[2]);

                        if (okTarget == ClientId) // El OK es para nosotros
                        {
                            if (!_okResponses.Contains(okSender))
                            {
                                _okResponses.Add(okSender);
                                OnMessageReceived?.Invoke($"Recibido OK #{_okResponses.Count} de {okSender}. Se necesitan {TotalClients - 1} para entrar en sección crítica.");

                                // Si recibimos todos los OKs necesarios, entramos en sección crítica
                                if (_okResponses.Count >= TotalClients - 1)
                                {
                                    await EnterCriticalSectionAsync();
                                }
                            }
                        }
                    }
                }
                else if (messageType == "RELEASE" && senderId != ClientId)
                {
                    // Si estábamos esperando responder a una solicitud, ahora podemos hacerlo
                    if (_waitingForAccess)
                    {
                        // Tendremos mensajes REQUEST pendientes a los que ahora podemos responder
                        foreach (var pendingRequest in _pendingResponses)
                        {
                            if (pendingRequest.StartsWith("REQUEST"))
                            {
                                string[] reqParts = pendingRequest.Split(' ');
                                int reqSender = int.Parse(reqParts[1]);

                                string response = $"OK {ClientId} {reqSender} {_lamportClock}";
                                await SendMessageAsync(response);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                OnMessageReceived?.Invoke($"Error al procesar mensaje: {ex.Message}");
            }
        }

        public async Task DisconnectAsync()
        {
            if (_clientWebSocket?.State == WebSocketState.Open)
            {
                _cancellationTokenSource?.Cancel();

                try
                {
                    await _clientWebSocket.CloseAsync(
                        WebSocketCloseStatus.NormalClosure,
                        "Desconexión solicitada por el cliente",
                        CancellationToken.None);

                    OnMessageReceived?.Invoke("Desconectado del servidor WebSocket.");
                }
                catch (Exception ex)
                {
                    OnMessageReceived?.Invoke($"Error al desconectar: {ex.Message}");
                }
            }
        }
    }
}