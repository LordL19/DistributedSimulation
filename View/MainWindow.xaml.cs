using System;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using DistributedSimulation.ViewModel;
using DistributedSimulation.Networking;

namespace DistributedSimulation.View
{
    public partial class MainWindow : Window
    {
        private WebSocketServer _webSocketServer;
        private MainViewModel _viewModel;
        private bool _serverStarted = false;

        public MainWindow()
        {
            InitializeComponent();

            // Iniciar el servidor en segundo plano
            StartWebSocketServerAsync();

            // Crear y asignar el ViewModel
            _viewModel = new MainViewModel();
            DataContext = _viewModel;

            // Configurar el auto-scroll para el log de mensajes
            if (scrollViewer != null)
            {
                _viewModel.Messages.CollectionChanged += (s, e) =>
                {
                    scrollViewer.ScrollToBottom();
                };
            }

            // Gestionar el cierre de la aplicación
            Closing += MainWindow_Closing;
        }

        private async void StartWebSocketServerAsync()
        {
            try
            {
                _webSocketServer = new WebSocketServer();

                // Iniciar servidor en hilo separado
                await Task.Run(async () =>
                {
                    try
                    {
                        _serverStarted = true;
                        await _webSocketServer.StartAsync(8080);
                    }
                    catch (Exception ex)
                    {
                        _serverStarted = false;
                        MessageBox.Show($"Error al iniciar el servidor WebSocket: {ex.Message}",
                                      "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                    }
                });
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Error al crear el servidor WebSocket: {ex.Message}",
                              "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private void MainWindow_Closing(object sender, System.ComponentModel.CancelEventArgs e)
        {
            // Limpiar recursos y cerrar conexiones
            if (_viewModel != null)
            {
                _viewModel.CleanupAsync();
            }
        }
    }

    // Extensión para hacer auto-scroll
    public static class ScrollViewerExtensions
    {
        public static void ScrollToBottom(this ScrollViewer scrollViewer)
        {
            if (scrollViewer != null)
            {
                scrollViewer.ScrollToVerticalOffset(double.MaxValue);
            }
        }
    }
}