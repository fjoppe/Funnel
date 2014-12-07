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

    type InitializedRoute = Route list

    let mutable allRoutes = List.empty<InitializedRoute>
    let mutable runningEndpoints = List.empty<IProducer>


    let Register route =
        let raiseProducerInTheMiddle(p:IEndpointStructure) = raise (ProducerInTheMiddle (sprintf "Cannot send message to producer with endpoint %s" p.GetEndpoint))

        /// Processes xpath bindings.
        /// Only works for an Xml Message body. Also (current implementation), the Xpath expression must indicate an Xml element, it cannot indicate an attribute.
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


        /// <summary>Processes a single route-step</summary>
        /// <param name="message">The message to be used as an input for the step (if required).</param>
        /// <param name="routeStep">The step of the route to be executed.</param>
        /// <returns>The resulting message, which can be modified by the step execution</returns>
        let processStep message (routeStep:RouteStep)=
                routeStep.task |> function 
                | Process(f)        -> f message
                | Bind(xpList, f)   -> processBind message xpList f
                | Do(f)             -> f message
                                       message
                | Consumer(c)       -> c.Receive(message)
                                       message  // this is a "hack", to satisfy List.fold in the calling function. This data will never be used.
                | Producer(p)       -> raise (ThisShouldNotOccur (sprintf "This situation should not occur, the producer with endpoint '%s' should already have been handled. Programming error!" p.GetEndpoint))


        /// <summary>
        ///     Waits for an incoming message from a producer, and then processes the route-part until the first encountered consumer. 
        ///     If the consumer is also a producer, then route initialization must have attached it to the next route part, if available.
        /// </summary>
        /// <param name="route-part remainder">A route part remainder, which is a route-part without the initial producer, in order-to-process.</param>
        /// <param name="producer">An initialized MailboxProcessor, to which the producer sends messages.</param>
        /// <returns>Returns an async workflow which infinitely processes incoming messages on the remaining route-part</returns>
        let routePartProcessor (``route-part remainder``:RouteStep list) (producer:MailboxProcessor<Message>)=
            let rec loop() =
                async {
                    try
                        log.Debug "[Route] Waiting for producer..."
                        let! msg = producer.Receive()
                        log.Debug "[Route] Received message"
                        
                        if msg.Body.IsSome then
                            match msg.Body.Value with
                            |   Xml(xml)    -> log.Debug (sprintf "[Route] Received message body: [%s]" xml)
                            |   Text(text)  -> log.Debug (sprintf "[Route] Received message body: [%s]" text)
                            |   Binary(bin) -> log.Debug "[Route] Received message body: {binary}"
                        ``route-part remainder`` |> List.fold processStep msg |> ignore
                    with
                        |   e -> log.ErrorException("[Route] Exception", e)
                    return! loop()
                }
            loop()


        /// <summary>
        ///     Checks if a route is valid.
        /// </summary>
        /// <param name="route">The route to be validated.</param>
        /// <returns>Returns the route input parameter for currying purpose</returns>
        /// <exception cref="RouteDoesNotStartWithProducer">Thrown when the route does not start with a Producer.</exception>
        /// <exception cref="RouteDoesNotEndWithConsumer">Thrown when the route does not end with a Consumer.</exception>
        /// <exception cref="ProducerInTheMiddle">Thrown when the route contains a producer, which is not at the start of the route.</exception>
        /// <exception cref="ConsumerInTheMiddle">Thrown when the route contains a consumer, which is illegally also used as producer.</exception>
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

            route   //  for currying with |>


        /// <summary>
        ///     Splits a route in multiple route-parts. Each route-part starts with a Producer and ends with a Consumer.
        ///     If a Consumer is also used as a Producer, it will appear in two routes:
        ///         <para>1. at the end of the appropriate route-part as Consumer;</para>
        ///         <para>2. at the start of the appropriate route-part as Producer.</para>
        /// </summary>
        /// <param name="route">The route to be validated, the list must be in order of processing.</param>
        /// <returns>Returns the route input parameter for currying purpose</returns>
        /// <exception cref="ThisShouldNotOccur">Thrown in any case which is illegal to occur</exception>
        let ``split into producer-to-consumer route parts`` route = 
            let rec ``parse one route-part`` parsed unprocessed =
                match unprocessed with
                | []           ->   raise (ThisShouldNotOccur "The route cannot be empty at this point. Programming error!")
                | head :: tail ->
                    match head.task with
                    //  this should be the first element of the route
                    | Producer(p) -> if List.length parsed = 0 then    
                                        ``parse one route-part`` [head] tail
                                     else
                                        raise (ThisShouldNotOccur "Occurance of Producer in the middle, which is invalid at this point. Programming error!")

                    //  this is either a step in the middle, or at the end                                                                        
                    | Consumer(c) -> if List.length parsed = 0 then        //  if the length is 0, then this is somewhere in the middle of the full route; what we received in parameter "rest" is a subset of the full route. Note that the validation function already tested that this consumer is also a producer.
                                        ``parse one route-part`` [head] tail
                                     else                                       //  in this case we have a route-part, prodcuer-to-consumer. Note that this consumer does not have to be a producer, at the termination of the full route
                                        let routePart = head::parsed |> List.rev
                                        if List.length unprocessed > 1 then            
                                            (routePart, unprocessed)
                                        else    //  if rest doesn't contain any producer-to-consumer routeParts anymore, then quit without rest
                                            (routePart, [])
                    //  for any other RouteStep, accumulate it in reverse order (!) and continue
                    | _           -> let acc = head :: parsed
                                     ``parse one route-part`` acc tail

            let rec ``parse all route parts`` parsed unparsed =
                match unparsed with
                |   []      -> List.rev parsed
                |   _       -> let (routePart, remainder) =  ``parse one route-part`` [] unparsed
                               let acc = routePart::parsed
                               ``parse all route parts`` acc remainder

            ``parse all route parts`` [] route


        /// <summary>
        ///     <para>Initializes all routeparts, which means that every producer will be initialized with a MailboxProcessor to send its messages to.</para>
        ///     <para>Every consumer, which is also used as a producer, gets attached to the producer's MailboxProcessor.
        /// </summary>
        /// <param name="routePartList">A list of route-parts to be initialized; the list must be in order of processing.</param>
        /// <returns>Returns the list of route-parts, which are now initialized.</returns>
        /// <exception cref="RouteIsEmpty">Thrown when a route is empty</exception>
        /// <exception cref="RouteDoesNotStartWithProducer">Thrown when a route does not start with a Producer (or a Consumer which may be used as Producer)</exception>
        let ``initialize all routeparts`` routePartList=

            /// Initializes a message agent for the specified producer. Eventually, this message agent (MailboxProcessor) will wait for a message from a Producer, and process this message in the route-part remainder.
            /// Note the route-part remainder is a route-part, without the starting Producer, and it must be in order of processing.
            let ``initialize message agent for`` (producer:IProducer) ``route-part remainder`` = 
                let messageProcessor = MailboxProcessor.Start(routePartProcessor ``route-part remainder``)
                producer.SetMessageProcessor(messageProcessor)                

            /// Initializes the producers of a route-part, and returns its hooks. 
            /// These hooks are initialized Producers, which are also used as Consumers in a different route-part.
            let ``initialize routePart and return hooks for`` routePart =  
                match routePart with
                | firstElement :: ``route-part remainder`` -> 
                    match firstElement.task with
                    | Producer(producer) ->
                            //  initialize mailbox processor for producer
                            let listener = ``initialize message agent for`` producer ``route-part remainder``
                            let initializedProducer = Producer(listener)
                            (None,  {firstElement with task = initializedProducer} :: ``route-part remainder``)
                    | Consumer(consumer) ->
                            let producer = consumer :?> IProducer
                            //  initialize mailbox processor for consumer, which is also used as a producer
                            let listener = ``initialize message agent for`` producer ``route-part remainder``
                            let initializedProducer = Producer(listener)    // only after initialization, a Consumer which is used as Producer, will become a Producer.
                            let initializedCons = listener :?> IConsumer
                            (Some((firstElement.id, initializedCons)), {firstElement with task = initializedProducer} :: ``route-part remainder``)
                    |   _               -> raise(RouteDoesNotStartWithProducer)
                |   []  -> raise(RouteIsEmpty)


            /// Attaches to an initialized route-part.
            /// The Consumer in the specified routePart, is also used as a Producer in a differente route-part.
            /// The Producer was initialized before. Because of immutibility the Consumer in the specified routePart must be replaced with the initialized Consumer (or the initialized Producer, but they are one and the same object)
            /// Returns the routepart with the initialized consumer.
            let ``attach initialized routePart`` id consumer (routePart:RouteStep list) = 
                routePart |> List.map(fun e -> 
                    if e.id <> id then e
                    else {e with task = Consumer(consumer)}
                    )


            /// Initializes all routeParts and chains them together, in order of processing.
            let rec ``initialize and chain routeParts`` processed processing (hook:Option<Guid*IConsumer>) = 
                match processing with
                |   routePart :: remainingRouteParts -> match hook with
                                    |    None               ->
                                            let (hook, routePart) = ``initialize routePart and return hooks for``  routePart
                                            ``initialize and chain routeParts`` (routePart::processed) remainingRouteParts hook
                                    |   Some(id, listener)  ->
                                            let hookedRoute = ``attach initialized routePart`` id listener routePart
                                            let (hook, routePart) = ``initialize routePart and return hooks for``  hookedRoute
                                            ``initialize and chain routeParts`` (routePart::processed) remainingRouteParts hook
                |   []           -> processed

            let routePartListReversed = routePartList  |> List.rev
            ``initialize and chain routeParts`` [] routePartListReversed None


        let ``initialized routePart list`` = 
            route 
            |> List.rev 
            |> validate 
            |> ``split into producer-to-consumer route parts``
            |> ``initialize all routeparts``

        allRoutes <- ``initialized routePart list`` :: allRoutes


    let PrintRoutes() = 
        allRoutes |> List.rev |> List.iteri(fun i e -> printfn "%d: %A" i (e |> List.rev))

    let Stop() =
        runningEndpoints |> List.iter(fun item -> item.Stop)
        runningEndpoints <- List.empty

    let ClearRoutes() = 
        Stop()
        allRoutes <- List.empty<InitializedRoute>

    let Run() = 
        allRoutes |> List.iter(
            fun route -> 
                let ``the enpoint is not running for`` r = not(runningEndpoints |> List.exists(fun e -> e = r))

                route |> List.head |> List.map(fun e -> e.task) |> function
                |   Producer(``producer endpoint``) :: rest -> 
                        if ``the enpoint is not running for`` ``producer endpoint`` then
                            ``producer endpoint``.Start
                            runningEndpoints <- ``producer endpoint`` :: runningEndpoints
                |   []  -> raise(RouteIsEmpty)
                |   _   -> raise(RouteDoesNotStartWithProducer)
            )
