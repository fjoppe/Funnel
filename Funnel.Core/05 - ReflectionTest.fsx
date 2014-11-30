#r "bin\Debug\Funnel.Core.dll"

open System
open System.Reflection
open Microsoft.FSharp.Reflection
open Funnel.Core
open Funnel.Core.General
open Funnel.Core.Core

let asm = Assembly.Load("Funnel.Core")

let myfun cp tp = 
            asm.GetTypes()
                |> Seq.ofArray
                |> Seq.filter(fun t -> 
                    t.GetInterfaces() 
                        |> Seq.ofArray
                        |> Seq.exists( fun e -> e = tp)
                    )
                |> Seq.filter(fun t ->
                    t.GetCustomAttributes()
                        |> Seq.exists(fun a ->
                            if a.GetType() = typeof<EndpointType> then
                                let mp = a :?> EndpointType
                                mp.Text = cp
                            else false                       
                    )
                )

myfun "file" typeof<IProducer>


typeof<LogConsumer>.GetCustomAttributes()

open Funnel.Core.Producers
let cr = typeof<FileListener>.GetMethod("Create", [|typeof<string>|])

cr.IsStatic
