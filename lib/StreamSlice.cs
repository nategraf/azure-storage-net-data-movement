using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    internal class StreamSlice : Stream
    {
        public override bool CanRead { get => Source.CanRead; }
        public override bool CanSeek { get => false; }
        public override bool CanTimeout { get => Source.CanTimeout; }
        public override bool CanWrite { get => Source.CanWrite; }
        public override long Position { get => this.position; set { throw new NotSupportedException(); } }

        public Stream Source { get; private set; }
        public long Offset { get; private set; }

        private long position;
        private MemoryStream uncommited;
        private long commitedPosition;

        public StreamSlice(Stream stream, long offset)
        {
            Source = stream;
            Offset = offset;
            uncommited = new MemoryStream(32 * 1024);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (Monitor.TryEnter(Source)) {
                try {
                    Commit();
                    Source.Position = Offset + position;
                    Source.Write(buffer, offset, count);
                }
                finally {
                    Monitor.Exit(Source);
                }

                position += count;
                commitedPosition = position;
            }
            else {
                uncommited.Write(buffer, offset, count);
                position += count;
            }

        }

        private void Commit()
        {
            if (uncommited.Length > 0) {
                uncommited.Position = 0;
                Source.Position = Offset + commitedPosition;
                uncommited.CopyTo(Source);

                Debug.Assert(commitedPosition + uncommited.Length == position);
                commitedPosition = position;
                uncommited.Position = 0;
                uncommited.SetLength(0);
            }
        }

        public override int Read(byte[] buffer, int offset, int count) {
            Flush(); // Ensure no writes are waiting

            Monitor.Enter(Source);
            try {
                Source.Position = Offset + Position;
                return Source.Read(buffer, offset, count);
            }
            finally {
                Monitor.Exit(Source);
            }
        }

        public override void Flush() {
            Monitor.Enter(Source);
            try {
                Commit();
            }
            finally {
                Monitor.Exit(Source);
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing) {
                uncommited.Dispose();
            }
            base.Dispose(disposing);
        }

        public override void SetLength(long len) { }
        public override long Seek(long pos, SeekOrigin origin) { throw new NotSupportedException(); }
        public override long Length { get { throw new NotSupportedException(); } }
    }
}
