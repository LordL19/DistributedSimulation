using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace DistributedSimulation.Model
{
    public class Process
    {
        public int Id { get; set; }
        public int LamportClock { get; set; } = 0;
        public bool InCriticalSection { get; set; } = false;
        private List<int> okResponses = new List<int>();

        public event Action<string> OnMessageReceived;

        public Process(int id)
        {
            Id = id;
        }

        public void SendRequest()
        {
            LamportClock++;
            string message = $"REQUEST {Id} {LamportClock}";
            OnMessageReceived?.Invoke(message);
        }

        public void ReceiveMessage(string message)
        {
            string[] parts = message.Split(' ');
            string type = parts[0];
            int senderId = int.Parse(parts[1]);
            int timestamp = int.Parse(parts[2]);

            if (type == "REQUEST")
            {
                if (!InCriticalSection && (LamportClock < timestamp || (LamportClock == timestamp && Id < senderId)))
                {
                    OnMessageReceived?.Invoke($"OK {Id} {senderId}");
                }
            }
            else if (type == "OK")
            {
                okResponses.Add(senderId);
                if (okResponses.Count == 2) // 3 procesos en total
                {
                    EnterCriticalSection();
                }
            }
            else if (type == "RELEASE")
            {
                InCriticalSection = false;
            }
        }

        private void EnterCriticalSection()
        {
            InCriticalSection = true;
            Task.Delay(3000).Wait();
            OnMessageReceived?.Invoke($"RELEASE {Id}");
        }
    }
}
