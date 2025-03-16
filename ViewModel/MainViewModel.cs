using System;
using System.Collections.ObjectModel;
using System.Threading.Tasks;
using System.Windows.Input;
using System.Windows;
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
        private bool _isInitialized = false;
        private bool _isInitializing = false;

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

        public bool CanSendRequest => _isInitialized && !(_clients[_activeClientIndex]?.IsWaitingForAccess ?? false)
                                   && !(_clients[_activeClientIndex]?.IsInCriticalSection ?? false);

        public ICommand SendRequestCommand { get; }
        public ICommand ChangeClientCommand { get; }
        public ICommand ResetCommand { get; }

        public MainViewModel()
        {
            _clients = new WebSocketClient[_numClients];

            SendRequestCommand = new RelayCommand(SendRequestAsync, () => CanSendRequest);
            ChangeClientCommand = new RelayCommand(ChangeActiveClientAsync, () => _isInitialized);
            ResetCommand = new RelayCommand(ResetClientsAsync, () => _isInitialized);

            // Iniciar los clientes de forma asíncrona
            _ = InitializeClientsAsync();
        }

        private async Task InitializeClientsAsync()
        {
            if (_isInitializing || _isInitialized)
                return;

            _isInitializing = true;

            try
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
                    int clientId = i + 1; // Capturar para el closure

                    // Configurar handler de mensajes antes de conectar
                    _clients[i].OnMessageReceived += (message) =>
                    {
                        Application.Current.Dispatcher.Invoke(() =>
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

                            // Actualizar estado de los comandos
                            (SendRequestCommand as RelayCommand)?.RaiseCanExecuteChanged();
                        });
                    };

                    try
                    {
                        // Asignar un índice local para el task que inicia la recepción de mensajes
                        int clientIndex = i;

                        await _clients[clientIndex].ConnectAsync(_serverUrl, clientId, _numClients);
                        AddMessage($"Cliente {clientId} conectado al servidor WebSocket.");

                        // Iniciar recepción de mensajes en segundo plano
                        _ = Task.Run(() => _clients[clientIndex].ReceiveMessagesAsync());
                    }
                    catch (Exception ex)
                    {
                        AddMessage($"Error al conectar cliente {clientId}: {ex.Message}");
                    }

                    // Pequeña pausa entre conexiones para evitar sobrecarga
                    await Task.Delay(300);
                }

                AddMessage("Todos los clientes inicializados. Sistema listo.");
                _isInitialized = true;
            }
            catch (Exception ex)
            {
                AddMessage($"Error durante la inicialización: {ex.Message}");
            }
            finally
            {
                _isInitializing = false;
                (SendRequestCommand as RelayCommand)?.RaiseCanExecuteChanged();
                (ChangeClientCommand as RelayCommand)?.RaiseCanExecuteChanged();
                (ResetCommand as RelayCommand)?.RaiseCanExecuteChanged();
            }
        }

        private async Task SendRequestAsync()
        {
            try
            {
                if (!CanSendRequest)
                {
                    AddMessage($"No se puede enviar solicitud ahora: Cliente {ActiveClientId} ya está en proceso de solicitud o sección crítica");
                    return;
                }

                AddMessage($"Cliente {ActiveClientId} solicitando acceso al recurso compartido...");
                await _clients[_activeClientIndex].SendRequestAsync();

                // Actualizar estado de comandos
                (SendRequestCommand as RelayCommand)?.RaiseCanExecuteChanged();
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

            // Actualizar estados de comandos
            (SendRequestCommand as RelayCommand)?.RaiseCanExecuteChanged();

            await Task.CompletedTask;
        }

        private async Task ResetClientsAsync()
        {
            try
            {
                // Desconectar todos los clientes existentes
                await CleanupAsync();

                // Reiniciar variables
                _isInitialized = false;
                _isInitializing = false;
                SharedResourceState = "Libre";
                ActiveClientIndex = 0;

                // Limpiar mensajes
                Messages.Clear();

                // Inicializar clientes nuevamente
                AddMessage("Reiniciando sistema...");
                await InitializeClientsAsync();
            }
            catch (Exception ex)
            {
                AddMessage($"Error al reiniciar sistema: {ex.Message}");
            }
        }

        private void AddMessage(string message)
        {
            Application.Current.Dispatcher.Invoke(() =>
            {
                Messages.Add($"[{DateTime.Now:HH:mm:ss.fff}] {message}");

                // Limitar el número de mensajes para evitar problemas de memoria
                while (Messages.Count > 500)
                {
                    Messages.RemoveAt(0);
                }
            });
        }

        public async Task CleanupAsync()
        {
            // Desconectar todos los clientes WebSocket
            if (_clients != null)
            {
                for (int i = 0; i < _clients.Length; i++)
                {
                    if (_clients[i] != null)
                    {
                        try
                        {
                            await _clients[i].DisconnectAsync();
                            _clients[i] = null;
                        }
                        catch (Exception ex)
                        {
                            // Registrar errores pero continuar con la limpieza
                            AddMessage($"Error al desconectar cliente {i + 1}: {ex.Message}");
                        }
                    }
                }
            }

            // Limpiar otros recursos si es necesario
            AddMessage("Cerrando conexiones de clientes...");
        }
    }
}