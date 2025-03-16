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

        public event Action<string> OnMessageReceived;
        public int ClientId { get; private set; }
        public int TotalClients { get; set; }
        public int LamportClock => _lamportClock;
        public bool IsInCriticalSection => _inCriticalSection;
        public bool IsWaitingForAccess => _waitingForAccess;

        public async Task ConnectAsync(string url, int clientId, int totalClients)
        {
            _clientWebSocket = new ClientWebSocket();
            _cancellationTokenSource = new CancellationTokenSource();
            await _clientWebSocket.ConnectAsync(new Uri(url), CancellationToken.None);
            ClientId = clientId;
            TotalClients = totalClients;
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
            if (_clientWebSocket.State != WebSocketState.Open)
            {
                OnMessageReceived?.Invoke($"Error: WebSocket no está abierto. Estado: {_clientWebSocket.State}");
                return;
            }

            var bytes = Encoding.UTF8.GetBytes(message);
            await _clientWebSocket.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, CancellationToken.None);
            OnMessageReceived?.Invoke($"Enviado: {message} (Reloj: {_lamportClock})");
        }

        public async Task ReceiveMessagesAsync()
        {
            var buffer = new byte[1024];

            try
            {
                while (_clientWebSocket.State == WebSocketState.Open && !_cancellationTokenSource.Token.IsCancellationRequested)
                {
                    var result = await _clientWebSocket.ReceiveAsync(
                        new ArraySegment<byte>(buffer),
                        _cancellationTokenSource.Token);

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        await _clientWebSocket.CloseAsync(
                            WebSocketCloseStatus.NormalClosure,
                            "Conexión cerrada",
                            CancellationToken.None);
                        break;
                    }

                    string message = Encoding.UTF8.GetString(buffer, 0, result.Count);
                    OnMessageReceived?.Invoke($"Recibido: {message}");

                    // Procesamos el mensaje de forma asíncrona pero esperamos la finalización
                    await ProcessMessageAsync(message);
                }
            }
            catch (OperationCanceledException)
            {
                // Normal cuando se cancela
                OnMessageReceived?.Invoke("Recepción de mensajes cancelada");
            }
            catch (Exception ex)
            {
                OnMessageReceived?.Invoke($"Error al recibir mensajes: {ex.Message}");
            }
        }

        private async Task ProcessMessageAsync(string message)
        {
            string[] parts = message.Split(' ');
            if (parts.Length < 3) return; // Mensaje inválido

            string messageType = parts[0];
            int senderId = int.Parse(parts[1]);
            int timestamp = int.Parse(parts[2]);

            // Actualizar reloj de Lamport
            _lamportClock = Math.Max(_lamportClock, timestamp) + 1;

            if (messageType == "REQUEST" && senderId != ClientId)
            {
                // Decide si responder OK inmediatamente o esperar
                if (!_waitingForAccess ||
                    (_waitingForAccess && (
                        _lamportClock > timestamp ||
                        (_lamportClock == timestamp && ClientId > senderId))))
                {
                    string response = $"OK {ClientId} {_lamportClock}";
                    await SendMessageAsync(response);
                }
                else
                {
                    OnMessageReceived?.Invoke($"Retrasando OK a {senderId} porque estamos esperando con mayor prioridad");
                }
            }
            else if (messageType == "OK" && _waitingForAccess)
            {
                int targetId = int.Parse(parts[2]); // A quién va dirigido el OK

                if (targetId == ClientId)
                {
                    _okResponses.Add(senderId);
                    OnMessageReceived?.Invoke($"Recibido OK #{_okResponses.Count} de {senderId}. Se necesitan {TotalClients - 1} para entrar en sección crítica.");

                    if (_okResponses.Count == TotalClients - 1)
                    {
                        _inCriticalSection = true;
                        _waitingForAccess = false;
                        OnMessageReceived?.Invoke($"Cliente {ClientId} ENTERED Critical Section.");

                        // Simular trabajo en la sección crítica
                        await Task.Delay(3000);

                        // Liberar la sección crítica
                        await SendReleaseAsync();
                    }
                }
            }
            else if (messageType == "RELEASE")
            {
                // Si estábamos esperando responder a una solicitud, ahora podemos
                if (_waitingForAccess && senderId != ClientId)
                {
                    string response = $"OK {ClientId} {senderId}";
                    await SendMessageAsync(response);
                }
            }
        }

        public async Task DisconnectAsync()
        {
            if (_clientWebSocket.State == WebSocketState.Open)
            {
                _cancellationTokenSource?.Cancel();

                try
                {
                    await _clientWebSocket.CloseAsync(
                        WebSocketCloseStatus.NormalClosure,
                        "Desconexión solicitada por el cliente",
                        CancellationToken.None);
                }
                catch (Exception ex)
                {
                    OnMessageReceived?.Invoke($"Error al desconectar: {ex.Message}");
                }
            }
        }
    }
}