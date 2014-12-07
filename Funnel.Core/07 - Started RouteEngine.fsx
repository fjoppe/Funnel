#r "bin\Debug\Funnel.Core.dll"
#r "NLog.dll"

open System
open Funnel.Core.General
open Funnel.Core.Core
open Funnel.Core.RouteEngine


let inout m = m

let myBindFunc (e:Map<string,string>) m =
    if e.ContainsKey "title" then printfn "%s" e.["title"]
    m


let routeStep = From("file://C:/Users/Frank/Source/Workspaces/Funnel/TestData?interval=5000") 
                    =>= Process(inout)
                    =>= ProcessBind([XPath("title", "//message/title"); XPath("description", "//message/description")], myBindFunc)
                    =>= To("log:Test1")
                    =>= Process(inout)
                    =>= To("log:Test2")


Register routeStep
PrintRoutes()

Run()
Stop()


//  ================= testing helper functions for testing what is above here ================= 

let ``split into producer-to-consumer route parts`` route = 
    let rec ``parse route part`` accumulator rest =
        match rest with
        | []           ->   raise (ThisShouldNotOccur "The route cannot be empty at this point. Programming error!")
        | head :: tail ->
            match head.task with
            //  this should be the first element of the route
            | Producer(p) -> if List.length accumulator = 0 then    
                                ``parse route part`` [head] tail
                             else
                                raise (ProducerInTheMiddle (sprintf "Cannot send message to producer with endpoint %s" p.GetEndpoint))
            //  this is either a step in the middle, or at the end                                                                        
            | Consumer(c) -> if List.length accumulator = 0 then        //  if the length is 0, then this is somewhere in the middle of the full route; what we received in parameter "rest" is a subset of the full route. Note that the validation function already tested that this consumer is also a producer.
                                ``parse route part`` [head] tail
                             else                                       //  in this case we have a route-part, prodcuer-to-consumer. Note that this consumer does not have to be a producer, at the termination of the full route
                                let routePart = head::accumulator |> List.rev
                                if List.length rest > 1 then            
                                    (routePart, rest)
                                else    //  if rest doesn't contain any producer-to-consumer routeParts anymore, then quit without rest
                                    (routePart, [])
            //  if we're not looking for it, we just accumulate it and continue
            | _           -> let acc = head :: accumulator
                             ``parse route part`` acc tail

    let rec ``get list of route parts`` accumulator route =
        if List.length route = 0 then
            List.rev accumulator
        else
            let (routePart, rest) =  ``parse route part`` [] route
            let acc = routePart :: accumulator
            ``get list of route parts`` acc rest

    ``get list of route parts`` [] route

//  making a copy from RouteEngine.fs for testing purpose
let processor (route:RouteStep list) (msgBox:MailboxProcessor<Message>) =
    let rec loop() =
        async {
            try
                log.Debug "[EndpointConsumer] Waiting..."
                //  process per step
                let! msg = msgBox.Receive()
                log.Debug "[EndpointConsumer] Received message"
                log.Debug msg.Body
//                route |> List.fold processStep msg |> ignore
            with
                |   e -> log.ErrorException("[EndpointConsumer] Exception", e)
            return! loop()
        }
    loop()

let ri = List.rev routeStep
let allParts = ``split into producer-to-consumer route parts`` ri

//let ``chain agents to routeParts`` routeParts = 
//    routeParts  |> List.rev |> Seq.ofList
//                |> Seq.iter ()

List.length allParts

let firstPart = allParts.Item 0
let lastConsumer = Seq.last firstPart

allParts


let setProcessor (prod:IProducer) rest = 
    let messageProcessor = MailboxProcessor.Start(processor rest)
    prod.SetMessageProcessor(messageProcessor)

let initializeRoutePart routePart = 
    match routePart with
    | first :: rest -> 
        match first.task with
        | Producer(prod) ->
                let listener = setProcessor prod rest
                let initializedProducer = Producer(listener)
                (None, None,  {first with task = initializedProducer} :: rest)
        | Consumer(cons) ->
                let prod = cons :?> IProducer
                let listener = setProcessor prod rest
                let initializedProducer = Producer(listener)
                let initializedCons = listener :?> IConsumer
                (Some(first.id), Some(initializedCons), {first with task = initializedProducer} :: rest)
        | _    -> failwith "illegal"
    | [] -> failwith "error"



let initializeRouteParts routePartList =
    let hookToInitializedRoutePart id consumer (routePart:RouteStep list) = 
        routePart |> List.map(fun e -> 
            if e.id <> id then e
            else {e with task = Consumer(consumer)}
            )
    
    let rec procesRouteParts processed processing (id:Option<Guid>) (listener:Option<IConsumer>) = 
        match processing with
        |   head :: tail -> if id.IsNone then
                                let (id, listener, routePart) = initializeRoutePart head
                                procesRouteParts  (routePart::processed) tail id listener
                            else
                                let hookedRoute = hookToInitializedRoutePart id.Value listener.Value head
                                let (id, listener, routePart) = initializeRoutePart hookedRoute
                                procesRouteParts  (routePart::processed) tail id listener
        |   []           -> processed

    let routePartListReversed = routePartList  |> List.rev

    procesRouteParts [] routePartListReversed None None

initializeRouteParts allParts






