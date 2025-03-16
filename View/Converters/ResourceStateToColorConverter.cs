using System;
using System.Globalization;
using System.Windows.Data;
using System.Windows.Media;

namespace DistributedSimulation.View.Converters
{
    public class ResourceStateToColorConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            string state = value as string;

            if (state == null) return Brushes.Black;

            if (state.StartsWith("Libre"))
                return Brushes.Green;

            if (state.StartsWith("En uso"))
                return Brushes.Red;

            return Brushes.Orange; // Estado pendiente o desconocido
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }
}