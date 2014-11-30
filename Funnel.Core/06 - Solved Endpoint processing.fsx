#r "bin\Debug\Funnel.Core.dll"
#r "NLog.dll"

open Funnel.Core
open Funnel.Core.General
open Funnel.Core.Core
open NLog

let inout m = m

let myBindFunc (e:Map<string,string>) m = m


module RouteEngine = 
    exception RouteIsEmpty 
    exception NoProducerInRouteStart

    let log = LogManager.GetLogger("root")

    let mutable allRoutes = List.empty<Route>
    let mutable runningEndpoints = List.empty<IProducer>


    let Register route = 
        allRoutes <- route :: allRoutes

    let PrintRoutes() = 
        allRoutes |> List.rev |> List.iteri(fun i e -> printfn "%d: %A" i (e |> List.rev))

    let Stop() =
        runningEndpoints |> List.iter(fun item -> item.Stop)
        runningEndpoints <- List.empty

    let ClearRoutes() = 
        Stop()
        allRoutes <- List.empty<Route>

    let Run() = 
        let processStep msg =
            function
                | Process(f)         -> f msg
                | Bind(xpList, f)    -> f Map.empty msg
                | Do(f)              -> f msg
                                        msg
                | _                  -> printfn "%s" msg.Body
                                        msg

        let processor (route:RouteStep list) (msgBox:MailboxProcessor<Message>) =
            let rec loop() =
                async {
                    try
                        log.Debug "[EndpointConsumer] Waiting..."
                        //  process per step
                        let! msg = msgBox.Receive()
                        log.Debug "[EndpointConsumer] Received message"
                        log.Debug msg.Body
                        route |> List.fold processStep msg |> ignore
                    with
                        |   e -> log.ErrorException("[EndpointConsumer] Exception", e)
                    return! loop()
                }
            loop()

        allRoutes |> List.iter(
            fun route -> 
                route |> List.rev |>
                    function
                    |   Producer(prod) :: rest -> 
                            let messageProcessor = MailboxProcessor.Start(processor rest)
                            let listener = prod.SetMessageProcessor(messageProcessor)
                            listener.Start
                            runningEndpoints <- listener :: runningEndpoints
                    |   []  -> raise(RouteIsEmpty)
                    |   _   -> raise(NoProducerInRouteStart)
                )


let routeStep = From("file://C:/Users/Frank/Source/Workspaces/Funnel/TestData?interval=5000") 
                    =>= Process(inout) 
                    =>= Bind([XPath("key", "path"); XPath("key", "path")], myBindFunc)
                    =>= To("log:Test")


open RouteEngine
Register routeStep
PrintRoutes()

Run()
Stop()
