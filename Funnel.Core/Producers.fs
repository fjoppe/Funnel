namespace Funnel.Core

open System
open System.Reflection
open Operators
open System.Timers
open System.IO
open General
open Core
open ResourceDefinitions

///<summary>
/// How to create a new producer:
///     1.  Create a Record which implements the IProducer interface; implement the interface's methods. Some (configurable) event should cause the creation of a Message, with the message contents in the body, 
///         and this message is send to the MailboxProcessor, received via the IProducer.SetMessageProcessor function.
///     2.  Set attribute "EndpointType" on the new producer, defining the object's name. When a URI is used for producer configuration, then this name is the first part. For example in this uri: "http://<other stuff>", the part "http" is the object's name, which is the part you configure with attribute "EndpointType"
///     3.  Implement the static member public Create (configuration:string), which should return a new instance of the producer.
///     4.  If you want to add extra resource-specific attributes to the message, then add these attributes via the ResourceAttributes type (ResourceDefinitions.fs)
///</summary>

module Producers =
    exception   MissingMessageProcessor of string


    ///<summary>
    /// This class is used to hold a static initializer for the FileListener record
    ///</summary>
    type FileListenerStaticInit() =
        static do
            //  this workaround makes sure that you can use queryparameters in class Uri(), for the filesystem, ie: file://mypath/myfile?param=test
            //  see: http://stackoverflow.com/questions/8757585/why-doesnt-system-uri-recognize-query-parameter-for-local-file-path
            let uriParserType = typeof<UriParser>
            let fileParserInfo = uriParserType.GetField("FileUri", BindingFlags.Static ||| BindingFlags.NonPublic)
            let fileParser = fileParserInfo.GetValue(null) :?> UriParser
            let fileFlagsInfo = uriParserType.GetField("m_Flags", BindingFlags.NonPublic ||| BindingFlags.Instance);
            let fileFlags = fileFlagsInfo.GetValue(fileParser) :?> int
            let mayHaveQuery = 0x20
            let newFlags = fileFlags ||| mayHaveQuery
            fileFlagsInfo.SetValue(fileParser, newFlags)


    ///<summary>
    /// The FileListener is a producer, identified by component name "file://&lt;path&gt;", which polls a directory on the filesystem, and processes 
    /// incoming files as messages.
    ///</summary>
    [<EndpointType("file")>]
    type FileListener = 
        {
            Endpoint          : string
            Timer             : Timer
            MessageProcessor  : MailboxProcessor<Message> option
        }
        with
            static member private createTimer configString = 
                let configUri = new Uri(configString)
                let parameters = queryparams configUri
                let interval = Int32.Parse(parameters.["interval"])
                new Timer(float(interval))

            member private this.pollFiles =
                let getXmlContent (f:FileInfo) = 
                    if f.Extension.ToLower() = ".xml" then 
                        let stream = f.OpenText()
                        stream.ReadToEnd()
                    else
                        ""

                let configUri = new Uri(this.Endpoint)
                Directory.GetFiles(configUri.LocalPath) 
                    |> List.ofArray
                    |> List.map(fun f -> new FileInfo(f))
                    |> List.sortBy (fun f -> f.CreationTimeUtc)
                    |> List.iter(fun f -> 
                        if this.MessageProcessor.IsSome then
                            let content = getXmlContent f
                            let message = Message.Empty.SetBody content
                            let fileAttributes = File(FileAttributes.Create f)
                            let message = message.SetResourceInfo (Some(fileAttributes))
                            this.MessageProcessor.Value.Post message
                        )

            static member public Create endpoint =
                let init = FileListenerStaticInit() 
                let timer = FileListener.createTimer endpoint
                { Endpoint = endpoint; Timer = timer; MessageProcessor = None }

            interface IProducer with
                member this.SetMessageProcessor messageBox =
                    let result = { this with MessageProcessor = Some messageBox}
                    result :> IProducer

                member this.Start =
                    if this.MessageProcessor.IsSome then
                        this.Timer.Start()
                        let workflow = 
                            async {
                                while true do
                                    let! args = Async.AwaitEvent(this.Timer.Elapsed)
                                    this.pollFiles
                            }
                        Async.Start workflow
                    else
                        raise (MissingMessageProcessor("Cannot Start FileListener, MessageProcessor is missing, initialize with 'SetMessageProcessor'"))

                member this.Stop =
                    if this.MessageProcessor.IsSome then
                        this.Timer.Stop()
                    else
                        raise (MissingMessageProcessor("Cannot Stop FileListener, MessageProcessor is missing, initialize with 'SetMessageProcessor'"))

            interface IEndpointStructure with
                member this.GetEndpoint = this.Endpoint

