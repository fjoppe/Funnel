#r "bin\Debug\Funnel.Core.dll"
#r "NLog.dll"

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
                    =>= To("log:Test")
                    =>= Process(inout)
                    =>= To("log:Test")


Register routeStep
PrintRoutes()

Run()
Stop()


//  ================= testing helper functions ================= 

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


let ri = List.rev routeStep
``split into producer-to-consumer route parts`` ri
