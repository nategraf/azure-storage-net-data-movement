//------------------------------------------------------------------------------
// <copyright file="PipelinedChunkedMemoryStream.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Collections.Generic;

    // The following class is designed under the assumption that writes much smaller on average than the buffer-size
    // It is still correct without this assumption, but it's performance could be improved
    // This is almost certainly a fair assumption for buffer-sizes >= 4 MB (the default)

    // Note: There is a class to be released in .NET Core 2.1 called Pipeline, which can be used to improve or replace this implementation
    internal class PipelineMemoryStream : Stream
    {
        private byte[] buffer;   // Our one and only working buffer
        private int length;      // Including data which has been returned
        private int offset;      // Within the buffer

        private IMemoryManager manager;
        private Action<byte[], int, int> callback;

        public PipelineMemoryStream(IMemoryManager manager, Action<byte[], int, int> callback)
        {
            this.buffer = manager.RequireBuffer();
            Debug.Assert(this.buffer != null); // TODO: Handle null return
            this.length = buffer.Length;
            this.manager = manager;
            this.callback = callback;
        }

        public override void Flush()
        {
            this.callback(this.buffer, 0, this.offset);
            this.length += this.offset;
            this.offset = 0;

            // The callback receiver now owns the sent buffer
            this.buffer = this.manager.RequireBuffer();
            Debug.Assert(this.buffer != null); // TODO: Handle null return
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (buffer == null)
            {
                throw new ArgumentNullException(nameof(buffer));
            }

            if (offset < 0 || count < 0 || buffer.Length - offset < count)
            {
                throw new ArgumentOutOfRangeException(nameof(count));
            }


            int remaining = count;
            int availible = this.buffer.Length - this.offset;

            while (remaining > 0)
            {
                // Find n: The bytes to be written on this iteration
                int n = remaining;
                if (n > availible) {
                    n = availible;
                }

                //Write n bytes to the current chunk
                Array.Copy(buffer, offset, this.buffer, this.offset, n);
                offset += n;
                this.offset += n;
                remaining -= n;

                // If we have just filled our internal buffer, flush
                // Otherwise there should be no more remaining data
                availible = this.buffer.Length - this.offset;
                if (availible == 0) {
                    this.Flush();
                    availible = this.buffer.Length;
                }
                else {
                    Debug.Assert(remaining > 0);
                }
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                this.manager.ReleaseBuffer(this.buffer);
                this.buffer = null;
            }

            base.Dispose(disposing);
        }

        public override bool CanRead
        {
            get { return false; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return true; }
        }

        public override long Length
        {
            get { return this.length; }
        }

        public override long Position
        {
            get { return this.length - this.buffer.Length + this.offset; }
            set { throw new NotSupportedException(); }
        }

        // Unsupported methods
        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }
    }
}
