namespace Funnel.Core

open NLog
open General
open Core
open System
open System.Xml.Linq
open System.Xml.XPath

module RouteEngine = 
    exception RouteIsEmpty 
    exception RouteDoesNotStartWithProducer
    exception RouteDoesNotEndWithConsumer
    exception ProducerInTheMiddle of string
    exception ConsumerInTheMiddle of string
    exception InvalidMessageUsage of string * string
    exception ThisShouldNotOccur of string

    let log = LogManager.GetLogger("root")

    let mutable allRoutes = List.empty<Route>
    let mutable runningEndpoints = List.empty<IProducer>

    let Register route =
        let raiseProducerInTheMiddle(p:IEndpointStructure) = raise (ProducerInTheMiddle (sprintf "Cannot send message to producer with endpoint %s" p.GetEndpoint))

        let processBind msg xpList f =
            let ``map all xpath matches for`` (xpList:XPathBind list) (doc:XDocument)  = 
                let ``get xpath match value for`` pattern  (doc:XDocument) =
                    let element = doc.XPathSelectElement(pattern)
                    if element = null then ""
                    else element.Value
                let mappingList = 
                    xpList |> List.map(
                        fun e -> e |> function
                            | XPath(key, xpath) -> 
                                let mappedValue = ``get xpath match value for`` xpath doc
                                key, mappedValue)
                Map.ofList mappingList
            if msg.Body.IsSome then
                match msg.Body.Value with
                | Xml(content) ->
                    try
                        let doc = XDocument.Parse(content)
                        let mappings = ``map all xpath matches for`` xpList doc 
                        f mappings msg
                    with
                    | e -> raise (InvalidMessageUsage ("Invalid usage of type Message. Expected a valid Xml, but received exception while parsing it. This error is probably caused by an incompliant Producer, make sure that Message creation is done properly.", content))
                | _ -> f Map.empty msg
            else
                f Map.empty msg


        let processStep msg (routeStep:RouteStep)=
                routeStep.task |> function 
                | Process(f)        -> f msg
                | Bind(xpList, f)   -> processBind msg xpList f
                | Do(f)             -> f msg
                                       msg
                | Consumer(c)       -> c.Receive(msg)
                                       msg  // needs to be removed!!
                | Producer(p)       -> raise (ThisShouldNotOccur (sprintf "This situation should not occur, the producer with endpoint '%s' should already have been handled. Programming error!" p.GetEndpoint))

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

        let validate (route:Route) = 
            if route.Length = 0 then raise RouteIsEmpty

            let rs = Seq.ofList route

            let producer = Seq.head rs
            producer.task |> function  // check if the route starts with a producer
            |   Producer(x) -> ()
            |   _           -> raise RouteDoesNotStartWithProducer

            let consumer = Seq.last rs
            consumer.task |> function // check if the route ends with a consumer
            |   Consumer(x) -> ()
            |   _           -> raise RouteDoesNotEndWithConsumer

            
            let ``Everything else`` = rs |> Seq.filter(fun e -> e.id <> producer.id && e.id <> consumer.id) // record equality does not work here, bc of IProducer/IConsumer
            ``Everything else`` |> Seq.iter(fun e ->
                match e.task with
                |   Producer(p) -> raiseProducerInTheMiddle(p) // raise (ProducerInTheMiddle (sprintf "Cannot send message to producer with endpoint %s" p.GetEndpoint))
                |   Consumer(c) -> 
                                    if not((box c) :? IProducer) then
                                        raise (ConsumerInTheMiddle (sprintf "Cannot receive a message from consumer with endpoint %s" c.GetEndpoint))
                |   _           -> ()
                )


        ///<summary>
        ///     Split a full route, into route parts, where each part consists of a producer-to-consumer chain.
        ///</summary>
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
                                        raiseProducerInTheMiddle(p)
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
                

        let initializedRoute r =  
            r |> List.rev  |> function
            |   h :: rest -> 
                match h.task with
                |   Producer(prod)  ->
                            let messageProcessor = MailboxProcessor.Start(processor rest)
                            let listener = prod.SetMessageProcessor(messageProcessor)
                            {h with task = Producer(listener)} :: rest
                |   _               -> raise(RouteDoesNotStartWithProducer)
            |   []  -> raise(RouteIsEmpty)

        route |> List.rev |> validate 
        allRoutes <- initializedRoute route :: allRoutes


    let PrintRoutes() = 
        allRoutes |> List.rev |> List.iteri(fun i e -> printfn "%d: %A" i (e |> List.rev))

    let Stop() =
        runningEndpoints |> List.iter(fun item -> item.Stop)
        runningEndpoints <- List.empty

    let ClearRoutes() = 
        Stop()
        allRoutes <- List.empty<Route>

    let Run() = 
        allRoutes |> List.iter(
            fun route -> 
                let ``the enpoint is not running for`` r = not(runningEndpoints |> List.exists(fun e -> e = r))
                route |> List.map(fun e -> e.task) |> function
                |   Producer(``producer endpoint``) :: rest -> 
                        if ``the enpoint is not running for`` ``producer endpoint`` then
                            ``producer endpoint``.Start
                            runningEndpoints <- ``producer endpoint`` :: runningEndpoints
                |   []  -> raise(RouteIsEmpty)
                |   _   -> raise(RouteDoesNotStartWithProducer)
            )
