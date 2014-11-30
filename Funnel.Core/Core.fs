namespace Funnel.Core

open System
open System.Web
open System.Reflection
open Operators
open General

module Core =
    exception NoEnpointsException of string
    exception IllegalEndpointException of string
    exception InvalidEndpoinUsage
    exception MissingStaticCreate of string
    exception UnsupportedFunctionSignature of string

    type XPathBind =
        |   XPath of key:string * path:string

    type RouteTask =
        |   Producer of (IProducer)
        |   Process of (Message -> Message)
        |   Bind of (XPathBind list * (Map<string,string> -> Message -> Message))
        |   Do of (Message -> unit)
        |   Consumer of (IConsumer)

    type Route = RouteStep list
    and RouteStep =
        {
            id      : Guid
            task    : RouteTask
        }
        with
            static member (=>=) ((source:Route), (destination:RouteStep)) : Route =
                destination :: source
            static member (=>=) ((source:RouteStep), (destination:RouteStep)) : Route =
                [destination; source]

            static member private getEndpointProcessor componentName (interfaceType:Type) =
                let candidates = 
                    interfaceType.Assembly.GetTypes() |> Seq.ofArray
                        |> Seq.filter(fun t ->
                            t.GetCustomAttributes()
                                |> Seq.exists(fun a ->
                                    match a with
                                    | :? EndpointType as mp -> mp.Text = componentName
                                    | _ ->  false                       
                                )
                            )
                if Seq.isEmpty candidates then raise (NoEnpointsException(sprintf "Did not find any endpoint component with name '%s'" componentName))
                else
                    let target = Seq.head candidates
                    let hasCorrectInterface = 
                        target.GetInterfaces() |> Seq.ofArray
                            |> Seq.exists( fun e -> e = interfaceType)
                    if hasCorrectInterface = false then raise (InvalidEndpoinUsage)
                    else target

            static member private getInstance uri (targetType:Type) =
                let createMethod = targetType.GetMethod("Create", [|typeof<string>|])
                if createMethod = null || createMethod.IsStatic = false then raise (MissingStaticCreate(sprintf "Missing static Create (uri:string) for type '%s', the type's implementation is incompliant." targetType.FullName))
                createMethod.Invoke(null, [|uri|])

            static member private createEndpoint (uri:string) interfaceType =
                let getComponentName (s:string) = 
                    let components = s.Split([|":"|], StringSplitOptions.RemoveEmptyEntries) |> Seq.ofArray
                    if Seq.isEmpty components then raise (IllegalEndpointException(sprintf "Endpoint does not contain valid componentname, missing ':' in: '%s'" uri))
                    else Seq.head components
                let cp = getComponentName uri 
                let processorType = RouteStep.getEndpointProcessor cp interfaceType
                RouteStep.getInstance uri processorType

            static member public createProducer s =
                let producer = RouteStep.createEndpoint s typeof<IProducer>
                RouteTask.Producer(producer :?> IProducer)
                
            static member public createConsumer s =
                let consumer = RouteStep.createEndpoint s typeof<IConsumer>
                Consumer(consumer :?> IConsumer)

            static member public empty = { id = Guid.NewGuid() ;task = Do(fun f -> ()) }    // dummy function doing nothing

    let From (s:string) = { RouteStep.empty with task = RouteStep.createProducer s }

    let To (s:string) =  { RouteStep.empty with task = RouteStep.createConsumer s }

    let Process f = { RouteStep.empty with task =  Process(f) }

    let ProcessBind (bindings,(processFunc:(Map<string,string> -> Message -> 'a))) =
            match box processFunc with
            | :? (Map<string,string> -> Message -> Message) as mb  -> { RouteStep.empty with task = Bind(bindings,mb)}
            | :? (Map<string,string> -> Message -> unit)    as mbu -> let wrapper mappings msg = mbu mappings msg
                                                                                                 msg
                                                                      { RouteStep.empty with task = Bind(bindings,wrapper) }
            | _ -> raise (UnsupportedFunctionSignature "Error in parameter 'processFunc', ProcessBind supports (Map<string,string> -> Message -> Message) and (Map<string,string> -> Message -> unit), the current input signature is incompliant.")

