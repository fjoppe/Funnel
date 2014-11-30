namespace Funnel.Core

open NLog
open General
open Core

module Consumers=

    [<EndpointType("log")>]
    type LogConsumer =
        {
            Endpoint          : string
            MessageProcessor  : MailboxProcessor<Message> option
        }
        with
            member private this.log = LogManager.GetLogger("root")
            member this.Consume message = this.log.Info message.Body
                                          message

            static member public Create endpoint =
                { Endpoint = endpoint; MessageProcessor = None }

            member private this.logMessage (msg:Message) =
                if(msg.Body.IsSome) then
                    match msg.Body.Value with
                    |   Xml(xml)    -> this.log.Info (sprintf "Message.Body: %s" xml)
                    |   Text(text)  -> this.log.Info (sprintf "Message.Body: %s" text)
                    |   Binary(bin) -> this.log.Info "Message.Body: [binary data]"
                else
                    this.log.Info "Message.Body: [None]"

            interface IConsumer with
                member this.Receive msg = 
                    this.logMessage msg
                    if this.MessageProcessor.IsSome then
                        this.MessageProcessor.Value.Post msg

            interface IProducer with
                member this.SetMessageProcessor messageBox =
                    let result = { this with MessageProcessor = Some messageBox}
                    result :> IProducer

                member this.Start = "do nothing" |> ignore

                member this.Stop = "do nothing" |> ignore

            interface IEndpointStructure with
                member this.GetEndpoint = this.Endpoint
