
open Funnel.Core.Model
open Funnel.Core.Producers
open Funnel.Core.RouteEngine


let inout m = m

let pf m = printfn "hi"


Register (From("file://C:/Users/Frank/Documents/Visual Studio 2013/Projects/Funnel/TestData?interval=5000") 
        =>= Translate inout
        =>= Translate inout
        =>= Translate inout
        =>= Action pf)

RouteCount()


let fl = new FileListener("file://C:/Users/Frank/Documents/Visual Studio 2013/Projects/Funnel/TestData?interval=5000")

fl.start

fl.stop



