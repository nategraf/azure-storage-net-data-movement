using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    internal class StreamSlice : Stream
    {
        public override bool CanRead { get => Source.CanRead; }
        public override bool CanSeek { get => Source.CanSeek; }
        public override bool CanTimeout { get => Source.CanTimeout; }
        public override bool CanWrite { get => Source.CanWrite; }
        public override long Position { get; set; }

        public Stream Source { get; private set; }
        public long Offset { get; private set; }

        private List<Task> tasks = new List<Task>();

        public StreamSlice(Stream stream, long offset)
        {
            Source = stream;
            Offset = offset;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if(Monitor.TryEnter(Source)) {
                try {
                    Source.Position = this.Offset + this.Position;
                    Source.Write(buffer, offset, count);
                }
                finally {
                    Monitor.Exit(Source);
                }
            }
            else {
                var copy = new byte[count];
                Array.Copy(buffer, offset, copy, 0, count);

                tasks.Add(DeferedWrite(copy, 0, count, this.Position));
            }

            this.Position += count;
        }

        private async Task DeferedWrite(byte[] buffer, int offset, int count, long position) {
            await Task.Run(() => {
                Monitor.Enter(Source);
                try {
                    Source.Position = this.Offset + position;
                    Source.Write(buffer, offset, count);
                }
                finally {
                    Monitor.Exit(Source);
                }
            });
        }

        public override int Read(byte[] buffer, int offset, int count) {
            Flush(); // Ensure no writes are waiting

            Monitor.Enter(Source);
            try {
                Source.Position = this.Offset + this.Position;
                return Source.Read(buffer, offset, count);
            }
            finally {
                Monitor.Exit(Source);
            }
        }

        public override void Flush() {
            Task.WhenAll(tasks).Wait();
            tasks = new List<Task>();
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing){ }
            base.Dispose(disposing);
        }

        public override void SetLength(long len) { }
        public override long Seek(long pos, SeekOrigin origin) { throw new NotSupportedException(); }
        public override long Length { get { throw new NotSupportedException(); } }
    }
}
