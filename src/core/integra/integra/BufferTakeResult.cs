namespace Integra
{
    public class BufferTakeResult <T>
    {
        public T Item { get; internal set; }

        public BufferTakeStatus Status { get; internal set; }
    }
}
