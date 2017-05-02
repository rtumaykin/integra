using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Integra
{
    public class Buffer<T>
    {
        private readonly BlockingCollection<T> _head;
        private readonly BlockingCollection<T> _tail;

        private readonly Task[] _backgroundProcesses;

        private long _readFileIndex;
        private long _writeFileIndex;
        private int _runningWriteItemsCount;

        private readonly string _fileBasePath;
        private StreamWriter _writeStreamWriter;
        private StreamReader _readStreamReader;
        private FileStream _writeStream;

        private readonly CancellationTokenSource _cancellationTokenSource;

        private long _isInitialized;

        private long _enqueueingComplete;
        private long _enqueueingCompletionRequested;
        private long _enqueueIntents;

        private readonly ConcurrentDictionary<long, StreamReader> _readers;


        private long _itemsOnDisk;

        private long _totalItems;

        private readonly ManualResetEventSlim _tailBackfillerCoordinator;

        public bool EnqueueingComplete => Interlocked.Read(ref _enqueueingComplete) == 1;

        public bool EndOfBuffer => EnqueueingComplete && Interlocked.Read(ref _totalItems) == 0;


        public void CompleteEnqueueing()
        {
            Interlocked.CompareExchange(ref _enqueueingCompletionRequested, 1, 0);
            var spinWait = new SpinWait();

            while (Interlocked.Read(ref _enqueueIntents) > 0)
                spinWait.SpinOnce();

            Interlocked.CompareExchange(ref _enqueueingComplete, 1, 0);

            // It is possible that the newly added item may have already been fetched by the dequeue request and now another dequeue request is waiting.
            if (Interlocked.Read(ref _totalItems) == 0)
            {
                // Cancel all background threads and also a waiting dequeue thread
                _cancellationTokenSource.Cancel(false);
                Task.WaitAll(_backgroundProcesses);
            }
        }

        public Buffer(int maxItemsInQueue)
        {
            _readFileIndex = -1;
            _writeFileIndex = -1;
            var tailStorage = new ConcurrentQueue<T>();
            _tail = new BlockingCollection<T>(tailStorage, maxItemsInQueue / 2);
            var headStorage = new ConcurrentQueue<T>();
            _head = new BlockingCollection<T>(headStorage, maxItemsInQueue / 2);
            _isInitialized = 0;
            _enqueueingComplete = 0;
            _enqueueingCompletionRequested = 0;
            _enqueueIntents = 0;
            _itemsOnDisk = 0;
            _totalItems = 0;
            _runningWriteItemsCount = 0;
            _cancellationTokenSource = new CancellationTokenSource();
            _tailBackfillerCoordinator = new ManualResetEventSlim();
            _readers = new ConcurrentDictionary<long, StreamReader>();
            _backgroundProcesses = new Task[2];

            _fileBasePath = Path.Combine(Path.GetTempPath(), $"BQ-{Guid.NewGuid():N}");
        }

        public void Enqueue(T item)
        {
            Interlocked.Increment(ref _enqueueIntents);

            if (Interlocked.CompareExchange(ref _isInitialized, 1, 0) == 0)
                StartBackgroundThreads();

            if (Interlocked.Read(ref _enqueueingComplete) == 1 || Interlocked.Read(ref _enqueueingCompletionRequested) == 1)
            {
                Interlocked.Decrement(ref _enqueueIntents);
                throw new Exception("Not accepting new items");
            }

            _head.Add(item);

            Interlocked.Increment(ref _totalItems);
            Interlocked.Decrement(ref _enqueueIntents);
        }

        private void StartBackgroundThreads()
        {
            _backgroundProcesses[0] = Task.Run(() => HeadOffloader());
            _backgroundProcesses[1] = Task.Run(() => TailBackfiller());
        }

        #region Head Offloading

        private void HeadOffloader()
        {
            while (!_cancellationTokenSource.Token.IsCancellationRequested)
            {
                try
                {
                    var item = _head.Take(_cancellationTokenSource.Token);

                    if (!TryDirectTransfer(item))
                        OffloadToFile(item);
                }
                catch (OperationCanceledException)
                {
                    // Ignore
                }
            }
        }

        private bool TryDirectTransfer(T item)
        {
            if (Interlocked.Read(ref _itemsOnDisk) == 0)
                return _tail.TryAdd(item);

            return false;
        }

        private void OffloadToFile(T item)
        {
            if (_writeFileIndex == -1 || _runningWriteItemsCount >= 10000)
            {
                CreateNextFile();
                _runningWriteItemsCount = 0;
            }

            var stringToWrite = JsonConvert.SerializeObject(item);

            var retry = true;
            do
            {
                try
                {
                    _writeStreamWriter.WriteLine(stringToWrite);
                    retry = false;
                }
                catch (IOException)
                {
                    CreateNextFile();
                    _runningWriteItemsCount = 0;
                }
            } while (retry);

            Interlocked.Increment(ref _itemsOnDisk);

            // allow tailBackfiller to continue
            _tailBackfillerCoordinator.Set();

            _runningWriteItemsCount++;
        }


        private void CreateNextFile()
        {
            //var currentWriter = _writeStreamWriter;
            //ReleaseFileWriters(currentWriter);
            var currentFileIndex = Interlocked.Increment(ref _writeFileIndex);

            var fileName = $"{_fileBasePath}.{currentFileIndex}.tmp";

            _writeStream = new FileStream(fileName, FileMode.Append, FileAccess.Write, FileShare.Read | FileShare.Delete);
            _writeStreamWriter = new StreamWriter(_writeStream) { AutoFlush = true };
            var readStream = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 512, FileOptions.DeleteOnClose);
            _readers.TryAdd(currentFileIndex, new StreamReader(readStream));
        }

        #endregion

        private void CancelAndWaitForBackgroundProcessesToComplete()
        {
            _cancellationTokenSource.Cancel(false);

            try
            {
                Task.WaitAll(_backgroundProcesses);
            }
            catch
            {
                // Ignore
            }
        }

        public BufferTakeResult<T> Dequeue()
        {
            if (EndOfBuffer)
            {
                CancelAndWaitForBackgroundProcessesToComplete();

                return new BufferTakeResult<T>()
                {
                    Item = default(T),
                    Status = BufferTakeStatus.EndOfQueue
                };
            }

            try
            {
                var item = _tail.Take(_cancellationTokenSource.Token);
                Interlocked.Decrement(ref _totalItems);

                return new BufferTakeResult<T>()
                {
                    Item = item,
                    Status = BufferTakeStatus.Success
                };
            }
            catch (OperationCanceledException)
            {
                CancelAndWaitForBackgroundProcessesToComplete();

                return new BufferTakeResult<T>()
                {
                    Item = default(T),
                    Status = BufferTakeStatus.EndOfQueue
                };
            }
        }

        public void TailBackfiller()
        {
            while (!EndOfBuffer && !_cancellationTokenSource.Token.IsCancellationRequested)
            {
                try
                {
                    _tailBackfillerCoordinator.Wait(_cancellationTokenSource.Token);
                }
                catch (OperationCanceledException)
                {
                    // Ignore
                }

                if (_cancellationTokenSource.Token.IsCancellationRequested || EndOfBuffer)
                    break;

                if (Interlocked.Read(ref _itemsOnDisk) > 0)
                {
                    EnsureFileReader();

                    var line = _readStreamReader.ReadLine();
                    var item = JsonConvert.DeserializeObject<T>(line);
                    _tail.Add(item);
                    Interlocked.Decrement(ref _itemsOnDisk);
                }
                else
                {
                    _tailBackfillerCoordinator.Reset();

                    // just in case check if there was any new data inserted in between
                    if (Interlocked.Read(ref _itemsOnDisk) > 0)
                        _tailBackfillerCoordinator.Set();
                }
            }

        }

        private void EnsureFileReader()
        {
            // read from file
            if (_readStreamReader == null || _readStreamReader.EndOfStream)
            {
                var currentReadFileIndex = Interlocked.Read(ref _readFileIndex);

                if (currentReadFileIndex < Interlocked.Read(ref _writeFileIndex))
                {
                    var newReadIndex = Interlocked.Increment(ref _readFileIndex);

                    _readStreamReader = _readers[newReadIndex];

                    if (newReadIndex != 0)
                        // let the garbage collector to take case of the rest
                        _readers[newReadIndex - 1] = null;
                }
            }
        }
    }
}
