//------------------------------------------------------------------------------
// <copyright file="SlimSyncTransferController.cs" company="Microsoft">
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

    internal class SlimSyncTransferController : TransferControllerBase, ISyncTransferController
    {
        private readonly TransferReaderWriterBase reader;

        public SlimSyncTransferController(
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

            this.SharedTransferData.OnTotalLengthChanged += (sender, args) =>
            {
                // For normal directions, we'll use default block size 4MB for transfer.
                this.SharedTransferData.BlockSize = Constants.DefaultBlockSize;
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
                var hasWork = this.reader.HasWork;

                return !this.ErrorOccurred && hasWork;
            }
        }

        protected override async Task<bool> DoWorkInternalAsync()
        {
            if (this.reader.HasWork) {
                await this.reader.DoWorkInternalAsync();
            }

            return this.ErrorOccurred || this.reader.IsFinished;
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
                        return new SlimBlockBasedBlobReader(this.Scheduler, this, this.CancellationToken);
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

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            if (disposing)
            {
                this.reader?.Dispose();

                foreach(var transferData in this.SharedTransferData.AvailableData.Values)
                {
                    transferData.Dispose();
                }

                this.SharedTransferData.AvailableData.Clear();
            }
        }
    }
}
