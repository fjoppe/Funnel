//  Second steps for EIP DSL 
//  send the compiled Funnel.Core to interactive before doing anything of the below

open System
open System.Web
open System.Reflection
open Operators
open System.Timers
open System.IO
open Funnel.Core.Model

let inout m = m

from("test") =>= processor inout =>= processor inout =>= processor inout

//  file listener test

let queryparams (uri:Uri) = 
    let nvc = HttpUtility.ParseQueryString uri.Query
    nvc.AllKeys |> Seq.map(fun key -> key, nvc.Get key) |> Map.ofSeq


let a = new Uri("file://c:/tmp/?p=1000&move=true")

a.Query

a.LocalPath

queryparams a


type FileListener(configuration : string) as this = 
    do
        //  this workaround makes sure that you can use queryparameters in class Uri(), for the filesystem, ie: file://mypath/myfile?param=test
        //  see: http://stackoverflow.com/questions/8757585/why-doesnt-system-uri-recognize-query-parameter-for-local-file-path
        let uriParserType = typeof<UriParser>
        let fileParserInfo = uriParserType.GetField("FileUri", BindingFlags.Static ||| BindingFlags.NonPublic)
        let fileParser = fileParserInfo.GetValue(null) :?> UriParser
        let fileFlagsInfo = uriParserType.GetField("m_Flags", BindingFlags.NonPublic ||| BindingFlags.Instance);
        let fileFlags = fileFlagsInfo.GetValue(fileParser) :?> int
        let mayHaveQuery = 0x20
        let newFlags = fileFlags ||| mayHaveQuery
        fileFlagsInfo.SetValue(fileParser, newFlags)

    let config = configuration
    let timer = FileListener.createTimer configuration

    member this.Configuration = config
    member this.Timer = timer

    static member private createTimer configString = 
        let configUri = new Uri(configString)
        let parameters = queryparams configUri
        let interval = Int32.Parse(parameters.["interval"])
        new Timer(float(interval))

    member this.start =
        timer.Start()
        let workflow = async {
                            while true do
                                let! args = Async.AwaitEvent(timer.Elapsed)
                                this.pollFiles
                        }
        Async.Start workflow

    member this.stop =
        timer.Stop()

    member private this.pollFiles =
        let configUri = new Uri(this.Configuration)
        let creationTime f = File.GetCreationTimeUtc(f)

        Directory.GetFiles(configUri.LocalPath) 
            |> List.ofArray
            |> List.map(fun f -> f, creationTime f)
            |> List.sortBy (fun (f,c) -> c)
            |> List.iter(fun (f,c) -> printfn "%s %A" f c)


let fl = new FileListener("file://C:/Users/Frank/Documents/Visual Studio 2013/Projects/Funnel/TestData?interval=5000")

fl.start

fl.stop

         