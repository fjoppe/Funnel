open System
open System.Reflection
open System.Timers
open System.IO
open System.Web
open NLog


let queryparams (uri:Uri) = 
        let nvc = HttpUtility.ParseQueryString uri.Query
        nvc.AllKeys |> Seq.map(fun key -> key, nvc.Get key) |> Map.ofSeq


type IProducer =
    abstract member Type   : string

type IConsumer = 
    abstract member Type : string


type Message = 
    {
        Id     : Guid
        Header : string
        Body   : string
    }

type Exchange =
    {
        Id      : Guid
        message : Message
        history : Message list
    }

type XPath =
    |   XPath of key:string * path:string

type RouteStep =
    |   Producer of (IProducer)
    |   Translate of (Message -> Message)
    |   Binding of ( XPath list * (Map<string,string> -> Message -> Message))
    |   Action of (Message -> unit)
    |   Consumer of (IConsumer)
    with
        static member (=>=) ((source:RouteStep), (destination:RouteStep))=
            [destination; source]
        static member (=>=) ((source:RouteStep list), (destination:RouteStep))=
            destination :: source

type RouteDef = {
        Id       : Guid
        Route    : RouteStep list
}

type FileListener(configuration : string) = 
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
    interface IProducer with
        member this.Type = "file"

type LogConsumer(configuration : string) =
    let log = LogManager.GetLogger("root")
    member this.Consume message = log.Info message.Body
                                  message
    interface IConsumer with
        member this.Type = "log"


let From (s:string) = Producer(FileListener(s))

let To (s:string) = Consumer(LogConsumer(s))

let Process f = Translate(f)

let convertToMap (bindings:XPath list) (m:Message) = Map.empty<string,string>

let Bind (bindings,f) = Binding(bindings,f)

let Action f = Action(f)

let inout m = m

let myBindFunc (e:Map<string,string>) m = m


let routeStep = From("file://C:/Users/Frank/Source/Workspaces/Funnel/TestData?interval=5000") 
                    =>= Process(inout) 
                    =>= Bind([XPath("key", "path"); XPath("key", "path")], myBindFunc)
                    =>= To("log:Test")

    