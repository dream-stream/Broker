using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;

namespace Dream_Stream.Services
{
    public static class FileStreamHandler
    {
        private static readonly ConcurrentDictionary<string, (SemaphoreSlim _lock, FileStream stream)> FileStreams = new ConcurrentDictionary<string, (SemaphoreSlim, FileStream)>();
        private static readonly SemaphoreSlim Lock = new SemaphoreSlim(1, 1);

        public static (SemaphoreSlim _lock, FileStream stream) GetFileStream(string key, string filePath)
        {
            if (!File.Exists(filePath)) CreateFile(filePath);

            var fileStreamWithLock = FileStreams.GetOrAdd(key,
                (new SemaphoreSlim(1, 1), new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite)));

            return fileStreamWithLock;
        }

        private static void CreateFile(string path)
        {
            Lock.Wait();
            if (File.Exists(path)) return;

            var directories = path.Substring(0, path.LastIndexOf("/", StringComparison.Ordinal));
            Directory.CreateDirectory(directories);
            var stream = File.Create(path);
            stream.Close();
            stream.Dispose();

            Lock.Release();
        }
    }
}
