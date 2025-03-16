using System;
using System.Collections.ObjectModel;
using System.Threading.Tasks;
using System.Windows.Input;
using DistributedSimulation.Helpers;
using DistributedSimulation.Networking;

namespace DistributedSimulation.ViewModel
{
    public class MainViewModel : ViewModelBase
    {
        private readonly WebSocketClient[] _clients;
        private readonly int _numClients = 3;
        private readonly string _serverUrl = "ws://localhost:8080";
        private int _activeClientIndex = 0;
        private string _sharedResourceState = "Libre";
        private ObservableCollection<string> _messages = new ObservableCollection<string>();

        public ObservableCollection<string> Messages
        {
            get => _messages;
            set
            {
                _messages = value;
                OnPropertyChanged(nameof(Messages));
            }
        }

        public string SharedResourceState
        {
            get => _sharedResourceState;
            set
            {
                _sharedResourceState = value;
                OnPropertyChanged(nameof(SharedResourceState));
            }
        }

        public int ActiveClientIndex
        {
            get => _activeClientIndex;
            set
            {
                _activeClientIndex = value;
                OnPropertyChanged(nameof(ActiveClientIndex));
                OnPropertyChanged(nameof(ActiveClientId));
            }
        }

        public int ActiveClientId => _activeClientIndex + 1;

        public ICommand SendRequestCommand { get; }
        public ICommand ChangeClientCommand { get; }

        public MainViewModel()
        {
            _clients = new WebSocketClient[_numClients];
            SendRequestCommand = new RelayCommand(SendRequestAsync, () => true);
            ChangeClientCommand = new RelayCommand(ChangeActiveClientAsync, () => true);

            InitializeClientsAsync();
        }

        private async Task InitializeClientsAsync()
        {
            AddMessage("Inicializando clientes...");

            // Crear e inicializar los clientes
            for (int i = 0; i < _numClients; i++)
            {
                // Asegúrate de que el índice está dentro de los límites
                if (i >= _clients.Length)
                {
                    AddMessage($"Error: Índice {i} fuera de los límites del array de clientes (tamaño: {_clients.Length})");
                    continue;
                }

                _clients[i] = new WebSocketClient();
                int clientId = i + 1;

                // Configurar handler de mensajes antes de conectar
                _clients[i].OnMessageReceived += (message) =>
                {
                    App.Current.Dispatcher.Invoke(() =>
                    {
                        AddMessage($"Cliente {clientId}: {message}");

                        // Actualizar estado del recurso cuando un cliente entra o sale de la sección crítica
                        if (message.Contains("ENTERED Critical Section"))
                        {
                            SharedResourceState = $"En uso por Cliente {clientId}";
                        }
                        else if (message.Contains("RELEASE") && SharedResourceState.Contains($"Cliente {clientId}"))
                        {
                            SharedResourceState = "Libre";
                        }
                    });
                };

                try
                {
                    await _clients[i].ConnectAsync(_serverUrl, clientId, _numClients);
                    AddMessage($"Cliente {clientId} conectado al servidor WebSocket.");

                    // Iniciar recepción de mensajes en segundo plano - CORRECCIÓN AQUÍ
                    int index = i; // Captura el índice en una variable local
                    _ = Task.Run(() => _clients[index].ReceiveMessagesAsync());
                }
                catch (Exception ex)
                {
                    AddMessage($"Error al conectar cliente {clientId}: {ex.Message}");
                }
            }

            AddMessage("Todos los clientes inicializados. Sistema listo.");
        }


        private async Task SendRequestAsync()
        {
            try
            {
                AddMessage($"Cliente {ActiveClientId} solicitando acceso al recurso compartido...");
                await _clients[_activeClientIndex].SendRequestAsync();
            }
            catch (Exception ex)
            {
                AddMessage($"Error al enviar solicitud: {ex.Message}");
            }
        }

        private async Task ChangeActiveClientAsync()
        {
            ActiveClientIndex = (ActiveClientIndex + 1) % _numClients;
            AddMessage($"Cliente activo cambiado a Cliente {ActiveClientId}");
            await Task.CompletedTask;
        }

        private void AddMessage(string message)
        {
            App.Current.Dispatcher.Invoke(() =>
            {
                Messages.Add($"[{DateTime.Now:HH:mm:ss.fff}] {message}");
            });
        }

        // Agrega este método al final de la clase MainViewModel

        public async Task CleanupAsync()
        {
            // Desconectar todos los clientes WebSocket
            if (_clients != null)
            {
                foreach (var client in _clients)
                {
                    if (client != null)
                    {
                        try
                        {
                            await client.DisconnectAsync();
                        }
                        catch (Exception ex)
                        {
                            // Registrar errores pero continuar con la limpieza
                            Console.WriteLine($"Error al desconectar cliente: {ex.Message}");
                        }
                    }
                }
            }

            // Limpiar otros recursos si es necesario
            AddMessage("Cerrando aplicación y liberando recursos...");
        }
    }
}