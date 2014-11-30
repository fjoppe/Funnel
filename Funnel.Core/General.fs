namespace Funnel.Core

open System
open System.Web
open ResourceDefinitions
open System.Xml.Linq
open System.Xml.XPath


module General =
    let queryparams (uri:Uri) = 
        let nvc = HttpUtility.ParseQueryString uri.Query
        nvc.AllKeys |> Seq.map(fun key -> key, nvc.Get key) |> Map.ofSeq

    type MessageContent =
        |   Xml of string
        |   Text of string
        |   Binary of Object

    type Message = 
        {
            Id              : Guid
            Header          : Map<string, string>
            Body            : MessageContent option
            ResourceInfo    : ResourceAttributes option
        }
        with
            static member Empty =
                { Id = Guid.NewGuid(); Header = Map.empty; Body = None; ResourceInfo = None }

            member this.AddHeader key value = 
                { this with Header = this.Header.Add(key, value)}

            member this.SetBody value =
                if (box value :? string) then
                    let s = string(value)
                    try
                        let doc = XDocument.Parse(s)
                        // if no exception, then it is a real XML document
                        { this with Body = Some(Xml(s))}
                    with
                    |   e -> { this with Body = Some(Text(s)) } 
                else
                    { this with Body = Some(Binary(value))}

            member this.SetResourceInfo ri = 
                { this with ResourceInfo = ri }


    type Exchange =
        {
            Id      : Guid
            Message : Message
            History : Message list
        }

    type IEndpointStructure =
        abstract member GetEndpoint : string

    type IProducer =
        inherit IEndpointStructure
        abstract member SetMessageProcessor : MailboxProcessor<Message> -> IProducer
        abstract member Start : unit
        abstract member Stop : unit


    type IConsumer = 
        inherit IEndpointStructure
        abstract member Receive : Message -> unit


    type EndpointType(text : string) =
        inherit System.Attribute()
        member this.Text = text 

