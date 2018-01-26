//------------------------------------------------------------------------------
// <copyright file="ReadDataState.cs" company="Microsoft">
//    Copyright (c) Microsoft Corporation
// </copyright>
//------------------------------------------------------------------------------
namespace Microsoft.WindowsAzure.Storage.DataMovement
{
    using System.IO;

    /// <summary>
    /// Keep the state of reading a single block from the input stream.
    /// </summary>
    internal class SlimReadDataState : TransferDataState
    {
        public Stream OutputStream
        {
            get;
            set;
        }

        public StreamSlice StreamSlice
        {
            get;
            set;
        }

        /// <summary>
        /// Private dispose method to release managed/unmanaged objects.
        /// If disposing = true clean up managed resources as well as unmanaged resources.
        /// If disposing = false only clean up unmanaged resources.
        /// </summary>
        /// <param name="disposing">Indicates whether or not to dispose managed resources.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (null != this.StreamSlice)
                {
                    this.StreamSlice.Dispose();
                    this.StreamSlice = null;
                }
            }
        }
    }
}
