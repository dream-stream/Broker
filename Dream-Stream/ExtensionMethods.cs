using System;
using System.IO;
using System.Threading.Tasks;

namespace Dream_Stream
{
    public static class ExtensionMethods
    {
        public static T[] SubArray<T>(this T[] data, int index, int length)
        {
            var result = new T[length];
            Array.Copy(data, index, result, 0, length);
            return result;
        }

        public static async Task MyCopyToAsync(this Stream sourceStream, Stream destinationStream, int amount)
        {
            var buffer = new byte[81920];
            int read;
            while (amount > 0 &&
                   (read = await sourceStream.ReadAsync(buffer, 0, Math.Min(buffer.Length, amount))) > 0)
            {
                await destinationStream.WriteAsync(buffer, 0, read);
                amount -= read;
            }
        }
    }
}