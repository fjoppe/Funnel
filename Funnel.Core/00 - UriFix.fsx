open System
open System.Web
open System.Reflection
open Operators

let uriParserType = typeof<UriParser>
let fileParserInfo = uriParserType.GetField("FileUri", BindingFlags.Static ||| BindingFlags.NonPublic)
let fileParser = fileParserInfo.GetValue(null) :?> UriParser
let fileFlagsInfo = uriParserType.GetField("m_Flags", BindingFlags.NonPublic ||| BindingFlags.Instance);
let fileFlags = fileFlagsInfo.GetValue(fileParser) :?> int
let mayHaveQuery = 0x20
let newFlags = fileFlags ||| mayHaveQuery
fileFlagsInfo.SetValue(fileParser, newFlags)
