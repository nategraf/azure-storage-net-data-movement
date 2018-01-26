//------------------------------------------------------------------------------
// <copyright file="SyncTransferController.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.DataMovement.TransferControllers
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Blob;

    internal interface ISyncTransferController : ITransferController
    {
        SharedTransferData SharedTransferData
        {
            get;
        }

        bool ErrorOccurred
        {
            get;
        }

        TransferContext TransferContext
        {
            get;
        }

        bool IsForceOverwrite
        {
            get;
        }

        TaskCompletionSource<object> TaskCompletionSource
        {
            get;
            set;
        }

        void Dispose();

        void CheckCancellation();

        void UpdateProgress(Action updateAction);

        void UpdateProgressAddBytesTransferred(long bytesTransferredToAdd);

        void StartCallbackHandler();

        void FinishCallbackHandler(Exception exception);

        void CheckOverwrite(bool exist, object source, object dest);

        Task SetCustomAttributesAsync(object dest);
      }
}
