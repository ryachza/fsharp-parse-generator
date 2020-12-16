#r "nuget:FParsec"

// TODO: F# keywords as record fields - escape all or detect?

open System.IO

// type StringFlag
//   = SFAllowEmpty
//   | SFAllowNull
type FieldKind
  = FKDate
  | FKTime
  | FKDateTime
  | FKInstant
  // | FKDuration // TODO: determine format for de/serialization of durations
  | FKGuid
  // TODO: unsigned int
  | FKInt
  | FKFloat
  | FKBool
  // | FKString of StringFlag array
  | FKString
  | FKRecord of RecordDefinition
  | FKOption of FieldKind
  | FKArray of FieldKind
  and FieldDefinition = {
    name:string
    kind:FieldKind
  }
  and RecordDefinition = {
    name:string
    fields:FieldDefinition array
  }
  and ModuleDefinition = {
    // name:string
    records:RecordDefinition array
  }

module Parse =
  module P = FParsec.CharParsers
  open FParsec

  type Input = {
    name:string
    stream:System.IO.Stream
    encoding:System.Text.Encoding
  }
  type State = Map<string,RecordDefinition>

  let ws = skipMany (anyOf " \t")
  let ws1 = skipMany1 (anyOf " \t")

  let rec private parseLine (parser:Parser<'a,State>) : Parser<'a,State> = parse {
    let comment = skipChar '#' >>. skipRestOfLine false
    let eol = ws >>? optional comment >>? (skipNewline <|> eof)
    let! result = ws >>? parser
    do! eol
    do! skipMany (notFollowedBy eof >>. eol)
    return result
  }

  let private parseIdentifier = identifier (IdentifierOptions()) .>> ws

  let private lookupRecord target : Parser<RecordDefinition,State> = parse {
    let! state = getUserState
    match state.TryFind(target) with
    | Some x -> return x
    | None -> return! fail (sprintf "unrecognized record - %s" target)
  }

  let private parseFieldKind = parse {
    let! (primitive:FieldKind) = choice [
      stringReturn "datetime" FKDateTime
      stringReturn "instant" FKInstant
      stringReturn "date" FKDate
      stringReturn "time" FKTime

      stringReturn "guid" FKGuid
      stringReturn "int" FKInt
      stringReturn "float" FKFloat
      stringReturn "bool" FKBool
      
      stringReturn "string" FKString

      pchar '%' >>. parseIdentifier >>= lookupRecord |>> FKRecord
    ]
    do! ws
    let! (wrappers:(FieldKind -> FieldKind) list) =
      sepBy (choice [stringReturn "option" FKOption;stringReturn "array" FKArray]) ws1
    return List.fold (fun agg wrapper -> wrapper agg) primitive wrappers
  }

  let private parseExpansion = parseLine <| parse {
    do! skipString "..."
    let! target = parseIdentifier
    let! record = lookupRecord target
    return record.fields
  }

  let private parseField = parseLine <| parse {
    let! name = parseIdentifier
    do! ws
    do! pchar ':' |>> ignore
    do! ws
    let! kind = parseFieldKind
    return [|{ name=name;kind=kind }|]
  }

  let private parseRecord = parse {
    let! name = parseLine (pchar '@' >>. parseIdentifier) <?> "type name (@name)"
    let! fields = many1 (parseExpansion <|> parseField) <?> "at least one field (name:type)"
    let record = { name=name;fields=Array.concat fields }
    let! state = getUserState
    do! setUserState (state.Add(name,record))
    return record
  }

  let private parseModule = parse {
    let! records = many1 parseRecord
    return { records=List.toArray records }
  }

  let parseFile (input:Input) : P.ParserResult<ModuleDefinition,State> =
    P.runParserOnStream parseModule Map.empty input.name input.stream input.encoding

let generateJsonMatch ({ name=name }:FieldDefinition) (matches:_) =
  sprintf
    @"(match x with | %s | _ -> Error [name,""type""])"
    (matches |> Seq.map (fun (t,m) -> sprintf "%s(x) -> (%s)" t m) |> String.concat " | " )

let generateParserMatch ({ name=name }:FieldDefinition) (parse:string) (check:string) =
  sprintf
    @"(match %s with | %s -> Ok result | _ -> Error [name,""invalid""])"
    parse
    check

let generateParser (field:FieldDefinition) : string =
  let rec inner ({ name=name;kind=kind } as field) =
    match kind with
    | FKOption FKString ->
      sprintf
        @"(match x with | JsonValue.Null -> Ok None | JsonValue.String("""") -> None | JsonValue.String(x) -> Ok (Some x)) | _ -> Error [name,""type""]"
    | FKOption kind ->
      sprintf @"Result.map Some %s" (inner { field with name="";kind=kind })
    | FKInstant ->
      generateJsonMatch field [
        "JsonValue.String",generateParserMatch field "NodaTime.Text.InstantPattern.General.Parse(x)" "NodaParseSuccess result"
      ]
    | FKDateTime ->
      generateJsonMatch field [
        "JsonValue.String",generateParserMatch field @"NodaTime.Text.LocalDateTimePattern.CreateWithInvariantCulture(""yyyy-MM-dd HH:mm:ss"").Parse(x)" "NodaParseSuccess result"
      ]
    | FKDate ->
      generateJsonMatch field [
        "JsonValue.String",generateParserMatch field @"NodaTime.Text.LocalDatePattern.CreateWithInvariantCulture(""yyyy-MM-dd"").Parse(x)" "NodaParseSuccess result"
      ]
    | FKTime ->
      generateJsonMatch field [
        "JsonValue.String",generateParserMatch field @"NodaTime.Text.LocalTimePattern.CreateWithInvariantCulture(""HH:mm:ss"").Parse(x)" "NodaParseSuccess result"
      ]
    | FKBool ->
      generateJsonMatch field [
        "JsonValue.Bool","Ok x"
        "JsonValue.String",generateParserMatch field "System.Boolean.TryParse(x)" "true,result"
      ]
    | FKGuid ->
      generateJsonMatch field [
        "JsonValue.String",generateParserMatch field "System.Guid.TryParse(x)" "true,result"
      ]
    | FKInt ->
      // TODO: does this work for large integers?
      sprintf
        @"(match x with | JsonValue.Float(x) when x %% 1.0 = 0.0 -> Ok (int x) | JsonValue.Number(x) when x %% 1M = 0M -> Ok (int x) | JsonValue.String(x) -> %s | _ -> Error [name,""type""])"
        (generateParserMatch field
          "System.Int32.TryParse(x)"
          "true,result"
        )
    | FKFloat ->
      sprintf
        @"(match x with | JsonValue.Float(x) -> Ok (decimal x) | JsonValue.Number(x) -> Ok x | JsonValue.String(x) -> %s | _ -> Error [name,""type""])"
        (generateParserMatch field
          "System.Decimal.TryParse(x)"
          "true,result"
        )
    | FKString ->
      sprintf
        @"(match x with | JsonValue.String("""") -> Error [name,""required""] | JsonValue.String(x) -> Ok x | _ -> Error [name,""type""])"
    | FKArray kind ->
      sprintf
        @"(match x with | JsonValue.Array(values) -> Result.map List.toArray <| Seq.foldBack (fun x agg -> match x,agg with | Ok x,Ok xs -> Ok (x::xs) | Error es1,Error es2 -> Error (es1@es2) | Error es,Ok _ -> Error es | Ok _,Error es -> Error es) (Array.mapi (fun i x -> let name = sprintf ""%%s[%%d]"" name i in %s) values) (Ok []) | _ -> Error [name,""type""])"
        (inner { field with name="";kind=kind })
    | FKRecord record ->
      sprintf
        @"(match x with | JsonValue.Record(properties) -> Result.mapError (List.map (fun (f,m) -> sprintf ""%%s.%%s"" name f,m)) (%s.parseJson (Map.ofArray properties)) | _ -> Error [name,""type""])"
        record.name
  sprintf
    @"(let name = ""%s"" in match map.TryFind(""%s"") with | Some x -> %s | None -> Error [name,""missing""])"
    field.name
    field.name
    (inner field)
let generateParseJson ({ name=name;fields=fields }:RecordDefinition) : string =
  let parse =
    fields
    |> Array.map generateParser
    |> String.concat ","
  let success =
    fields
    |> Array.mapi (fun i _ -> sprintf "Ok _%d" i)
    |> String.concat ","
  let construct =
    fields
    |> Array.mapi (fun i { name=name } -> sprintf "%s=_%d" name i)
    |> String.concat ";"
    |> sprintf "Ok { %s }"
  let failure =
    fields
    |> Array.mapi (fun i _ -> sprintf "_%d" i)
    |> String.concat ","
  let error =
    fields
    |> Array.mapi (fun i _ -> sprintf "(match _%d with | Ok _ -> [] | Error es -> es)" i)
    |> String.concat ";"
    |> sprintf "Error (List.collect id [%s])"
  sprintf
    @"(match %s with | %s -> %s | %s -> %s)"
    parse
    success
    construct
    failure
    error
let rec generateKind : FieldKind -> string = function
  | FKDate -> "NodaTime.LocalDate"
  | FKTime -> "NodaTime.LocalTime"
  | FKDateTime -> "NodaTime.LocalDateTime"
  | FKInstant -> "NodaTime.Instant"
  | FKGuid -> "System.Guid"
  | FKInt -> "int"
  | FKFloat -> "decimal"
  | FKBool -> "bool"
  | FKString -> "string"
  | FKRecord x -> x.name
  | FKArray x -> sprintf "%s array" (generateKind x)
  | FKOption x -> sprintf "%s option" (generateKind x)
let generateField : FieldDefinition -> string = function
  | { name=name;kind=kind } ->
    sprintf "%s:%s" name (generateKind kind)
let generateRecord : RecordDefinition -> string = function
  | { name=name;fields=fields } as x ->
    sprintf @"
type %s = {
  %s
} with
  // TODO: handler structure errors and parse errors separately
  static member parseJson (map:Map<string,JsonValue>) : Result<%s,(string*string) list> =
    %s
  static member parseJsonRaw (x:string) : Result<%s,(string * string) list> =
    match JsonValue.TryParse(x) with
    | Some (JsonValue.Record(properties)) ->
      %s.parseJson (Map.ofArray properties)
    | Some _ -> Error [""_"",""type""]
    | None -> Error [""_"",""parse""]
  static member parseJsonArray (x:JsonValue array) : Result<%s,((string*string) list) array> =
    // TODO: loop through calling `parseJson` on each, collecting all success or set of failures
    failwith ""parseJsonArray: not implemented""
  static member parseJsonRawArray (x:string) : Result<%s,((string * string) list) array> =
    match JsonValue.TryParse(x) with
    | Some (JsonValue.Array(values)) ->
      %s.parseJsonArray values
    | Some _ -> Error [|[""_"",""type""]|]
    | None -> Error [|[""_"",""parse""]|]
"
      name
      (String.concat "\n  " (Array.map generateField fields))
      name
      (generateParseJson x)
      name
      name
      name
      name
      name

let target = "out/parsers.fs"
let generated () =
  let valfiles =
    Directory.GetFiles("src","*.val",SearchOption.AllDirectories)
  seq {
    yield "module App.GeneratedParsers"
    yield "open FSharp.Data"
    yield "let (|NodaParseSuccess|NodaParseFailure|) (x:NodaTime.Text.ParseResult<'a>) = match x.TryGetValue(Unchecked.defaultof<'a>) with | true,x -> NodaParseSuccess x | false,_ -> NodaParseFailure"
    for file in valfiles do
      printfn "%A" file
      match Parse.parseFile { name=file;stream=File.OpenRead(file);encoding=System.Text.Encoding.UTF8 } with
      | FParsec.CharParsers.Success ({ records=records },_,_) ->
        for record in records -> generateRecord record
        // yield "type Test = { one:int;two:NodaTime.LocalDateTime }"
      | FParsec.CharParsers.Failure (result,_,_) ->
        failwith result

  }
try
  let sb = System.Text.StringBuilder()
  for line in generated () do
    ignore<System.Text.StringBuilder> <| sb.AppendLine(line)
  let result = sb.ToString()
  ignore<DirectoryInfo> (Directory.CreateDirectory(Path.GetDirectoryName(target)))
  using (File.CreateText(target)) <| fun writer -> writer.Write(result)
with
| e ->
  printfn "%s:%s" e.Message e.StackTrace
  failwith e.Message
