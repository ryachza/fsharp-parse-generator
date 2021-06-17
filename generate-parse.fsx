#r "nuget:FParsec"

// TODO: F# keywords as record fields - escape all or detect?

open System.IO

type FieldName  = FieldName  of string with member this.extract = match this with | FieldName x -> x
type RecordName = RecordName of string with member this.extract = match this with | RecordName x -> x
type ModuleName = ModuleName of string with member this.extract = match this with | ModuleName x -> x

let emptyFieldName = FieldName ""

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

  | FKDynamic
  | FKValid of FieldKind

  | FKLiteral of string
  and FieldDefinition = {
    name:FieldName
    kind:FieldKind
    source:RecordName option
  }
  and RecordDefinition = {
    name:RecordName
    fields:FieldDefinition array
  }
  and ModuleBlock = ModuleBlockRecord of RecordDefinition | ModuleBlockRaw of string
  and ModuleDefinition = {
    // name:string
    blocks:ModuleBlock array
  }

module Parse =
  module P = FParsec.CharParsers
  open FParsec

  type Input = {
    name:string
    stream:System.IO.Stream
    encoding:System.Text.Encoding
  }
  type State = Map<RecordName,RecordDefinition>

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
  let private parseFieldName = parseIdentifier |>> FieldName
  let private parseRecordName = parseIdentifier |>> RecordName

  let private lookupRecord target : Parser<RecordDefinition,State> = parse {
    let! state = getUserState
    match state.TryFind(target) with
    | Some x -> return x
    | None -> return! fail (sprintf "unrecognized record - %s" target.extract)
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

      stringReturn "dynamic" FKDynamic

      pchar '%' >>. parseRecordName >>= lookupRecord |>> FKRecord
      pchar '!' >>. parseIdentifier |>> FKLiteral
    ]
    do! ws
    let! (wrappers:(FieldKind -> FieldKind) list) =
      sepBy (choice [
        stringReturn "valid" FKValid
        stringReturn "option" FKOption
        stringReturn "array" FKArray
      ]) ws1
    return List.fold (fun agg wrapper -> wrapper agg) primitive wrappers
  }

  let private parseExpansion = parseLine <| parse {
    do! skipString "..."
    let! target = parseRecordName
    let! record = lookupRecord target
    return record.fields |> Array.map (fun x -> { x with source=Some target })
  }

  let private parseField = parseLine <| parse {
    let! name = parseFieldName
    do! ws
    do! pchar ':' |>> ignore
    do! ws
    let! kind = parseFieldKind
    return [|{ name=name;kind=kind;source=None }|]
  }

  let private parseRecord = parse {
    let! name = parseLine (pchar '@' >>. parseRecordName) <?> "type name (@name)"
    let! fields = many1 (parseExpansion <|> parseField) <?> "at least one field (name:type)"
    let record = { name=name;fields=Array.concat fields }
    let! state = getUserState
    do! setUserState (state.Add(name,record))
    return record
  }

  let private parseRaw = parse {
    do! parseLine (pstring "!raw") |>> ignore<string>
    let! raw = charsTillString "!endraw" false System.Int32.MaxValue
    do! parseLine (pstring "!endraw") |>> ignore<string>
    return raw
  }

  let private parseModule = parse {
    let! blocks = many1 ((parseRaw |>> ModuleBlockRaw) <|> (parseRecord |>> ModuleBlockRecord))
    return { blocks=List.toArray blocks }
  }

  let parseFile (input:Input) : P.ParserResult<ModuleDefinition,State> =
    P.runParserOnStream parseModule Map.empty input.name input.stream input.encoding

let generateDynamicsTypeParameters (rd:RecordDefinition) : string option =
  match rd.fields |> Array.choose (function | { name=x;kind=FKDynamic } -> Some x | _ -> None) with
  | [||] -> None
  | xs   -> Some (sprintf "<%s>" (xs |> Array.map (fun (FieldName x) -> sprintf "'%s" x) |> String.concat ","))
let generateTypeName (rd:RecordDefinition) : string =
  sprintf "%s%s" (rd.name.extract) (Option.defaultValue "" (generateDynamicsTypeParameters rd))

let rec generateKind (name:FieldName) : FieldKind -> string = function
  | FKDate -> "NodaTime.LocalDate"
  | FKTime -> "NodaTime.LocalTime"
  | FKDateTime -> "NodaTime.LocalDateTime"
  | FKInstant -> "NodaTime.Instant"
  | FKGuid -> "System.Guid"
  | FKInt -> "int"
  | FKFloat -> "decimal"
  | FKBool -> "bool"
  | FKString -> "string"
  | FKRecord x -> generateTypeName x
  | FKArray x -> sprintf "%s array" (generateKind name x)
  | FKOption x -> sprintf "%s option" (generateKind name x)
  | FKValid x -> generateKind name x
  | FKDynamic -> sprintf "'%s" name.extract
  | FKLiteral x -> x

let rec extractValidateFields (rd:RecordDefinition) : string option =
  rd.fields
  |> Array.choose (function
    | { name=x;kind=FKValid ((FKRecord z) as y) }
    | { name=x;kind=FKValid ((FKOption (FKRecord z)) as y) }
    | { name=x;kind=FKValid ((FKArray (FKRecord z)) as y) } ->
      match extractValidateFields z with
      | None -> Some (sprintf "%s:ValidateParser<%s>" x.extract (generateKind x y))
      | Some t -> Some (sprintf "%s:ValidateParser<%s>;%s':%s" x.extract (generateKind x y) x.extract t)
    | { name=x;kind=FKValid y }
    | { name=x;kind=FKOption (FKValid y) }
    | { name=x;kind=FKArray (FKValid y) } ->
      Some (sprintf "%s:ValidateParser<%s>" x.extract (generateKind x y))
    | { name=x;kind=FKRecord rd }
    | { name=x;kind=FKOption (FKRecord rd) }
    | { name=x;kind=FKArray (FKRecord rd) } ->
      match extractValidateFields rd with
      | None -> None
      | Some t -> Some (sprintf "%s':%s" x.extract t)
    | _ -> None
  )
  |> function
    | [||] -> None
    | xs -> xs |> String.concat ";" |> sprintf "{| %s |}" |> Some
let generateValidatesParameter (rd:RecordDefinition) : {| name:string;kind:string |} option =
  match extractValidateFields rd with
  | None -> None
  | Some x -> Some {| name="validates";kind=x |}

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
  let rec inner ({ name=name;kind=kind } as field) (fieldname:FieldName) =
    match kind with
    | FKDynamic ->
      sprintf
        @"(match dynamics.%s x with | Ok x -> Ok x | Error es -> Error es)"
        name.extract
    | FKValid x ->
      sprintf
        @"(match %s with | Ok x -> (match validates.%s x with | Ok x -> Ok x | Error x -> Error [name,%s]) | Error x -> Error x)"
        (inner { field with name=emptyFieldName;kind=x } fieldname)
        name.extract
        @"(sprintf ""%s"" x)"
    | FKOption FKString ->
      sprintf
        @"(match x with | JsonValue.Null -> Ok None | JsonValue.String("""") -> Ok None | JsonValue.String(x) -> Ok (Some x) | _ -> Error [name,""type""])"
    | FKOption kind ->
      sprintf @"(match x with | JsonValue.Null -> Ok None | x -> Result.map Some %s)" (inner { field with name=emptyFieldName;kind=kind } fieldname)
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
        "JsonValue.Boolean","Ok x"
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
        (inner { field with name=emptyFieldName;kind=kind } fieldname)
    | FKRecord record ->
      sprintf
        @"(match x with | JsonValue.Record(properties) -> Result.mapError (List.map (fun (f,m) -> sprintf ""%%s.%%s"" name f,m)) (%s.parseJson (Map.ofArray properties%s)) | _ -> Error [name,""type""])"
        record.name.extract
        (match extractValidateFields record with | None -> "" | Some _ -> sprintf ",validates.%s'" fieldname.extract)
    | FKLiteral literal ->
      sprintf
        @"(%s.parseJson x)"
        literal
  sprintf
    @"(let name = ""%s"" in match map.TryFind(""%s"") with | Some x -> %s | None -> Error [name,""missing""])"
    field.name.extract
    field.name.extract
    (inner field field.name)
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
    |> Array.mapi (fun i { name=name } -> sprintf "%s=_%d" name.extract i)
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
let generateDynamicsParameter (rd:RecordDefinition) : {| name:string;kind:string |} option =
  match rd.fields |> Array.choose (function | { name=x;kind=FKDynamic } -> Some x | _ -> None) with
  | [||] -> None
  | xs   -> Some {| name="dynamics";kind=sprintf "{| %s |}" (xs |> Array.map (fun (FieldName x) -> sprintf "%s:DynamicParser<'%s>" x x) |> String.concat ";") |}
let additionalParameters (rd:RecordDefinition) : {| name:string;kind:string |} array =
  Array.choose id [|generateDynamicsParameter rd;generateValidatesParameter rd|]
let parameterToString (p:{| name:string;kind:string |}) : string =
  sprintf "(%s:%s)" p.name p.kind
let generateTypeDynamics (rd:RecordDefinition) =
  match generateDynamicsParameter rd with
  | None -> ""
  | Some x -> sprintf "type %s_Dynamic%s = %s" rd.name.extract (Option.defaultValue "" (generateDynamicsTypeParameters rd)) x.kind
let generateTypeValidates (rd:RecordDefinition) =
  match generateValidatesParameter rd with
  | None -> ""
  | Some x -> sprintf "type %s_Validate = %s" rd.name.extract x.kind
let generateField : FieldDefinition -> string = function
  | { name=name;kind=kind } ->
    sprintf "%s:%s" name.extract (generateKind name kind)
let generateFromSegments : RecordDefinition -> string = function
  | { name=name;fields=fields } ->
    let segments,fields =
      Array.foldBack (fun ({ source=source } as z) (xs,ys) ->
        match source with
        | Some x ->
          ((x,z)::xs,ys)
        | None ->
          (xs,z::ys)
      ) fields ([],[])
    match segments with
    | [] -> ""
    | segments ->
      let grouped =
        segments
        |> List.groupBy fst
      let parametersS =
        grouped
        |> List.mapi (fun i (name,_) ->
          sprintf "(r_%d:%s)" i name.extract
        )
        |> String.concat " "
      let parameterF =
        match fields with
        | [] -> ""
        | fields ->
          sprintf "(fs:{| %s |})"
            (
              fields
              |> List.map generateField
              |> String.concat ";"
            )
      let assignments =
        (
          (
            grouped
            |> List.mapi (fun i (_,x) ->
              x |> List.map (fun (_,{ name=name }) -> sprintf "%s=r_%d.%s" name.extract i name.extract)
            )
            |> List.collect id
          )
          @
          (fields |> List.map (fun { name=name } -> sprintf "%s=fs.%s" name.extract name.extract))
        )
        |> String.concat ";"
      sprintf @"
  static member fromSegments %s %s : %s =
    {
      %s
    }
"
        parametersS
        parameterF
        name.extract
        assignments
let generateRecord : RecordDefinition -> string = function
  | { name=name;fields=fields } as x ->
    let parameters = additionalParameters x
    let parametersSignature =
      match parameters with
      | [||] -> ""
      | xs   -> sprintf ",%s" (parameters |> Array.map parameterToString |> String.concat ",")
    let parametersCall =
      match parameters with
      | [||] -> ""
      | xs   -> sprintf ",%s" (parameters |> Array.map (fun x -> x.name) |> String.concat ",")
    sprintf @"
%s
%s
type %s = {
  %s
} with
  // TODO: handler structure errors and parse errors separately
  static member parseJson (map:Map<string,JsonValue>%s) : Result<%s,(string*string) list> =
    %s
  static member parseJsonRaw (x:string%s) : Result<%s,(string * string) list> =
    match JsonValue.TryParse(x) with
    | Some (JsonValue.Record(properties)) ->
      %s.parseJson (Map.ofArray properties%s)
    | Some _ -> Error [""_"",""type""]
    | None -> Error [""_"",""parse""]
  static member parseJsonArray (x:JsonValue array%s) : Result<%s array,((string*string) list) array> =
    // TODO: loop through calling `parseJson` on each, collecting all success or set of failures
    failwith ""parseJsonArray: not implemented""
  static member parseJsonRawArray (x:string%s) : Result<%s array,((string * string) list) array> =
    match JsonValue.TryParse(x) with
    | Some (JsonValue.Array(values)) ->
      %s.parseJsonArray (values%s)
    | Some _ -> Error [|[""_"",""type""]|]
    | None -> Error [|[""_"",""parse""]|]
  %s
"
      // type definition:
      (generateTypeDynamics x)
      (generateTypeValidates x)
      (generateTypeName x)
      (fields |> Array.map generateField |> String.concat "\n  ")

      // parseJson definition:
      parametersSignature
      (generateTypeName x)
      (generateParseJson x)

      // parseJsonRaw definition:
      parametersSignature
      (generateTypeName x)
      name.extract
      parametersCall

      // parseJsonArray definition:
      parametersSignature
      (generateTypeName x)
      // name.extract
      // parametersCall

      // parseJsonRawArray definition:
      parametersSignature
      (generateTypeName x)
      name.extract
      parametersCall

      (generateFromSegments x)

let target = "out/parsers.fs"
let generated () =
  let valfiles =
    Directory.GetFiles("src","*.val",SearchOption.AllDirectories)
  seq {
    yield "module App.GeneratedParsers"
    yield "open FSharp.Data"
    yield "type DynamicParser<'a> = JsonValue -> Result<'a,(string*string) list> // Map<string,JsonValue> -> Result<'a,string>"
    yield "type ValidateParser<'a> = 'a -> Result<'a,string>"
    yield "let private (|NodaParseSuccess|NodaParseFailure|) (x:NodaTime.Text.ParseResult<'a>) = match x.TryGetValue(Unchecked.defaultof<'a>) with | true,x -> NodaParseSuccess x | false,_ -> NodaParseFailure"
    for file in valfiles do
      printfn "%A" file
      match Parse.parseFile { name=file;stream=File.OpenRead(file);encoding=System.Text.Encoding.UTF8 } with
      | FParsec.CharParsers.Success ({ blocks=blocks },_,_) ->
        for block in blocks -> match block with | ModuleBlockRecord record -> generateRecord record | ModuleBlockRaw raw -> raw
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
