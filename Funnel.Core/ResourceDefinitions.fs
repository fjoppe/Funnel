namespace Funnel.Core

open System
open System.IO


module ResourceDefinitions = 

    ///<summary>
    /// We are using our own FileAttributes type, because we don't want to encourage using the System.IO.FileInfo mutation functions, like FileInfo.Delete.
    /// It is still possible to use these functions though, however it requires an effort. 
    /// Preferably, this information is only used for administration purposes, while file mutational operations are done 
    /// in the Endpoint's configuration.
    ///</summary>
    type FileAttributes = {
        filename        : string
        extension       : string
        length          : int64
        fullPath        : string
        creationDate    : DateTime
        creationDateUTC : DateTime
    }
    with
        static member Create (info:FileInfo) =
            { filename = info.Name; extension = info.Extension; length = info.Length; fullPath = info.FullName; creationDate = info.CreationTime ; creationDateUTC = info.CreationTimeUtc }

    type ResourceAttributes = 
         | File of FileAttributes
