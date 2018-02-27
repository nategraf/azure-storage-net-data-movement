//------------------------------------------------------------------------------
// <copyright file="PipelinedSyncTransferController.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Collections.Concurrent;
    using System.Globalization;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Blob;

    internal class PipelinedSyncTransferController : TransferControllerBase, ISyncTransferController
    {
        private readonly TransferReaderWriterBase reader;
        private readonly TransferReaderWriterBase writer;

        public PipelinedSyncTransferController(
            TransferScheduler transferScheduler, 
            TransferJob transferJob, 
            CancellationToken userCancellationToken)
            : base(transferScheduler, transferJob, userCancellationToken)
        {
            if (null == transferScheduler)
            {
                throw new ArgumentNullException(nameof(transferScheduler));
            }

            if (null == transferJob)
            {
                throw new ArgumentNullException(nameof(transferJob));
            }

            this.SharedTransferData = new SharedTransferData()
            {
                TransferJob = this.TransferJob, 
                AvailableData = new ConcurrentDictionary<long, TransferData>(), 
            };

            if (null == transferJob.CheckPoint)
            {
                transferJob.CheckPoint = new SingleObjectCheckpoint();
            }

            this.reader = this.GetReader(transferJob.Source);
            this.writer = this.GetWriter(transferJob.Destination);

            this.SharedTransferData.OnTotalLengthChanged += (sender, args) =>
            {
                // For large block blob uploading, we need to re-calculate the BlockSize according to the total size
                // The formula: Ceiling(TotalSize / (50000 * DefaultBlockSize)) * DefaultBlockSize. This will make sure the 
                // new block size will be mutiple of DefaultBlockSize(aka MemoryManager's chunk size)
                var normalMaxBlockBlobSize = (long)50000 * Constants.DefaultBlockSize;

                // Calculate the min block size according to the blob total length
                var memoryChunksRequiredEachTime = (int)Math.Ceiling((double)this.SharedTransferData.TotalLength / normalMaxBlockBlobSize);
                var blockSize = memoryChunksRequiredEachTime * TransferManager.Configurations.MemoryChunkSize;

                if (Constants.PipelinedBlockSize > blockSize)
                {
                    // Take the block size user specified when it's greater than the calculated value
                    blockSize = Constants.PipelinedBlockSize;
                    memoryChunksRequiredEachTime = (int)Math.Ceiling((double)blockSize / TransferManager.Configurations.MemoryChunkSize);
                }

                // Try to increase the memory pool size
                this.Scheduler.TransferOptions.UpdateMaximumCacheSize(blockSize);

                this.SharedTransferData.BlockSize = blockSize;
                this.SharedTransferData.MemoryChunksRequiredEachTime = memoryChunksRequiredEachTime;
            };
        }

        public SharedTransferData SharedTransferData
        {
            get;
            private set;
        }

        public bool ErrorOccurred
        {
            get;
            private set;
        }

        public override bool HasWork
        {
            get
            {
                var hasWork = (!this.reader.PreProcessed && this.reader.HasWork)
                    || (this.reader.PreProcessed && this.writer.HasWork)
                    || (this.writer.PreProcessed && this.reader.HasWork);

                return !this.ErrorOccurred && hasWork;
            }
        }

        public override bool HasWriterWork
        {
            get
            {
                var hasWork = this.reader.PreProcessed && this.writer.HasWork;

                return !this.ErrorOccurred && hasWork;
            }
        }

        protected override async Task<bool> DoWorkInternalAsync()
        {
            if (this.reader.PreProcessed && this.writer.HasWork)
            {
                await this.writer.DoWorkInternalAsync();
            }
            else if (!this.reader.PreProcessed && this.reader.HasWork)
            {
                await this.reader.DoWorkInternalAsync();
            }
            else if (this.writer.PreProcessed && this.reader.HasWork)
            {
                await this.reader.DoWorkInternalAsync();
            }

            return this.ErrorOccurred || this.writer.IsFinished;
        }

        protected override void SetErrorState(Exception ex)
        {
            this.ErrorOccurred = true;
        }

        private TransferReaderWriterBase GetReader(TransferLocation sourceLocation)
        {
            switch (sourceLocation.Type)
            {
                case TransferLocationType.AzureBlob:
                    CloudBlob sourceBlob = (sourceLocation as AzureBlobLocation).Blob;
                    if (sourceBlob is CloudBlockBlob)
                    {
                        return new PipelinedBlockBasedBlobReader(this.Scheduler, this, this.CancellationToken);
                    }
                    else if (sourceBlob is CloudAppendBlob)
                    {
                        return new PipelinedBlockBasedBlobReader(this.Scheduler, this, this.CancellationToken);
                    }
                    else 
                    {
                        throw new InvalidOperationException(
                            string.Format(
                            CultureInfo.CurrentCulture, 
                            Resources.UnsupportedBlobTypeException, 
                            sourceBlob.BlobType));
                    }
                default:
                    throw new InvalidOperationException(
                        string.Format(
                        CultureInfo.CurrentCulture, 
                        Resources.UnsupportedTransferLocationException, 
                        sourceLocation.Type));
            }
        }

        private TransferReaderWriterBase GetWriter(TransferLocation destLocation)
        {
            switch (destLocation.Type)
            {
                case TransferLocationType.Stream:
                    return new StreamedWriter(this.Scheduler, this, this.CancellationToken);
                default:
                    throw new InvalidOperationException(
                        string.Format(
                        CultureInfo.CurrentCulture, 
                        Resources.UnsupportedTransferLocationException, 
                        destLocation.Type));
            }
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            if (disposing)
            {
                this.reader?.Dispose();

                this.writer?.Dispose();

                foreach(var transferData in this.SharedTransferData.AvailableData.Values)
                {
                    transferData.Dispose();
                }

                this.SharedTransferData.AvailableData.Clear();
            }
        }
    }
}
