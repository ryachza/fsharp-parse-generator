#r "nuget:FParsec"

// TODO: F# keywords as record fields - escape all or detect?

open System.IO

type FieldName  = FieldName  of string with member this.extract = match this with | FieldName x -> x
type RecordName = RecordName of string with member this.extract = match this with | RecordName x -> x
type ModuleName = ModuleName of string with member this.extract = match this with | ModuleName x -> x

// type StringFlag
//   = SFAllowEmpty
//   | SFAllowNull
type FieldKind
  = FKPrimitive of FieldKindPrimitive
  | FKRecord of RecordDefinition
  | FKLiteral of string

  | FKOption of FieldKind
  | FKArray of FieldKind
  | FKValid of FieldKind
  and FieldKindPrimitive
  = FKDate
  | FKTime
  | FKDateTime
  | FKInstant
  // | FKDuration // TODO: determine format for de/serialization of durations
  | FKGuid
  // TODO: unsigned int
  | FKInt
  | FKDecimal
  | FKBool
  // | FKString of StringFlag array
  | FKStringE
  | FKStringNE

  | FKDynamic
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
    let! scalar = choice [
      choice [
        stringReturn "datetime" FKDateTime
        stringReturn "instant" FKInstant
        stringReturn "date" FKDate
        stringReturn "time" FKTime

        stringReturn "guid" FKGuid
        stringReturn "int" FKInt
        stringReturn "decimal" FKDecimal
        stringReturn "bool" FKBool

        stringReturn "stringe" FKStringE
        stringReturn "stringne" FKStringNE

        stringReturn "dynamic" FKDynamic
      ] |>> FKPrimitive
      pchar '!' >>. parseIdentifier |>> FKLiteral
      pchar '@' >>. parseRecordName >>= lookupRecord |>> FKRecord
    ]
    do! ws
    let! (wrappers:(FieldKind -> FieldKind) list) =
      sepBy (choice [
        stringReturn "valid" FKValid
        stringReturn "option" FKOption
        stringReturn "array" FKArray
      ]) ws1
    return List.fold (fun agg wrapper -> wrapper agg) scalar wrappers
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
    let! raw = charsTillString "!endraw" true System.Int32.MaxValue
    return raw
  }

  let private parseModule = parse {
    let! blocks = many1 ((parseRaw |>> ModuleBlockRaw) <|> (parseRecord |>> ModuleBlockRecord))
    return { blocks=List.toArray blocks }
  }

  let parseFile (input:Input) : P.ParserResult<ModuleDefinition,State> =
    runParserOnStream parseModule Map.empty input.name input.stream input.encoding
let generateDynamicsTypeParametersOut (rd:RecordDefinition) : string option =
  let rec variables xs =
    xs |> Array.collect (function
      | { name=x;kind=FKPrimitive FKDynamic } -> [|x|]
      | { kind=FKPrimitive _ } -> [||]
      | { kind=FKRecord y } -> variables y.fields
      | { kind=FKLiteral _ } -> [||]
      | { kind=FKValid y } | { kind=FKOption y } | { kind=FKArray y } as x -> variables [|{ x with kind=y }|]
    )
  match variables rd.fields with
  | [||] -> None
  | xs   -> Some (sprintf "<%s>" (xs |> Array.map (fun (FieldName x) -> sprintf "'%s" x) |> String.concat ","))
let generateDynamicsTypeParametersInOut (rd:RecordDefinition) : string option =
  let rec variables xs =
    xs |> Array.collect (function
      | { name=x;kind=FKPrimitive FKDynamic } -> [|x|]
      | { kind=FKPrimitive _ } -> [||]
      | { kind=FKRecord y } -> variables y.fields
      | { kind=FKLiteral _ } -> [||]
      | { kind=FKValid y } | { kind=FKOption y } | { kind=FKArray y } as x -> variables [|{ x with kind=y }|]
    )
  match variables rd.fields with
  | [||] -> None
  | xs   -> Some (sprintf "<'__,%s>" (xs |> Array.map (fun (FieldName x) -> sprintf "'%s" x) |> String.concat ","))
let generateTypeName (rd:RecordDefinition) : string =
  sprintf "%s%s" rd.name.extract (generateDynamicsTypeParametersOut rd |> Option.defaultValue "")

let rec generateKind (name:FieldName) : FieldKind -> string = function
  | FKPrimitive FKDate -> "NodaTime.LocalDate"
  | FKPrimitive FKTime -> "NodaTime.LocalTime"
  | FKPrimitive FKDateTime -> "NodaTime.LocalDateTime"
  | FKPrimitive FKInstant -> "NodaTime.Instant"
  | FKPrimitive FKGuid -> "System.Guid"
  | FKPrimitive FKInt -> "int"
  | FKPrimitive FKDecimal -> "decimal"
  | FKPrimitive FKBool -> "bool"
  | FKPrimitive FKStringE -> "string"
  | FKPrimitive FKStringNE -> "string"

  | FKPrimitive FKDynamic -> sprintf "'%s" name.extract
  | FKLiteral x -> x

  | FKRecord x -> generateTypeName x
  | FKArray x -> sprintf "%s array" (generateKind name x)
  | FKOption x -> sprintf "%s option" (generateKind name x)
  | FKValid x -> generateKind name x

let extractDynamicFields (dvs:string option) (rd:RecordDefinition) : string option =
  let rec inner (inArray:bool) = function
    | { name=x;kind=FKPrimitive FKDynamic } ->
      [x.extract,sprintf "DynamicParser<%s,'%s>" (Option.defaultValue "'__" dvs) x.extract]
    | { kind=FKPrimitive _ }
    | { kind=FKLiteral _ } ->
      []
    | { kind=FKRecord y } ->
      y.fields |> List.ofArray |> List.collect (inner inArray)
    | { name=x;kind=FKValid y }
    | { name=x;kind=FKOption y } as z ->
      inner inArray { z with kind=y }
    | { name=x;kind=FKArray y } as z ->
      match inner true { z with kind=y } with
      | [] -> []
      | xs -> [x.extract,sprintf "{| %s |}" (xs |> List.map (fun (a,b) -> sprintf "%s:%s" a b) |> String.concat ";")]
  match rd.fields |> List.ofArray |> List.collect (inner false) with
  | [] -> None
  | xs -> Some <| sprintf "{| %s |}" (xs |> List.map (fun (a,b) -> sprintf "%s:%s" a b) |> String.concat ";")
let generateDynamicsParameter (dvs:string option) (rd:RecordDefinition) : {| name:string;kind:string |} option =
  extractDynamicFields dvs rd |> Option.map (fun x -> {| name="dynamics";kind=x |})
let rec extractValidateFields (dvs:string option) (rd:RecordDefinition) : string option =
  let rec inner = function
    | { name=FieldName x as fn;kind=FKValid y } ->
      (fn.extract,sprintf "ValidateParser<%s,%s>" (Option.defaultValue "'__" dvs) (generateKind fn y))::inner { name=FieldName (x+"'");kind=y;source=Some rd.name }
    | { name=x;kind=FKOption y } | { name=x;kind=FKArray y } ->
      inner { name=x;kind=y;source=Some rd.name }
    | { name=x;kind=FKRecord rd } ->
      match extractValidateFields dvs rd with
      | None -> []
      | Some s -> [x.extract,s]
    | { kind=FKLiteral _ } ->
      []
    | { kind=FKPrimitive _ } ->
      []
  match rd.fields |> Array.toList |> List.collect inner with
  | [] -> None
  | xs -> xs |> List.map (fun (n,t) -> sprintf "%s:%s" n t) |> String.concat ";" |> sprintf "{| %s |}" |> Some
let generateValidatesParameter (dvs:string option) (rd:RecordDefinition) : {| name:string;kind:string |} option =
  extractValidateFields dvs rd |> Option.map (fun x -> {| name="validates";kind=x |})
let rec hasDynamics : FieldKind -> bool = function
  | FKValid x | FKOption x | FKArray x -> hasDynamics x
  | FKPrimitive FKDynamic -> true
  | FKPrimitive _ -> false
  | FKRecord x -> x.fields |> Array.map (fun x -> x.kind) |> Array.exists hasDynamics
  | FKLiteral _ -> false

let rec hasDynamicInArray : FieldKind -> bool = function
  | FKRecord x -> x.fields |> Array.map (fun x -> x.kind) |> Array.exists hasDynamicInArray
  | FKArray x -> hasDynamics x
  | FKValid x | FKOption x -> hasDynamicInArray x
  | FKPrimitive FKDynamic | FKPrimitive _ | FKLiteral _ -> false

let rec hasValidates = function
  | FKValid _ -> true
  | FKOption x | FKArray x -> hasValidates x
  | FKRecord x -> x.fields |> Array.map (fun x -> x.kind) |> Array.exists hasValidates
  | FKPrimitive _ -> false
  | FKLiteral _ -> false

let generateParserPrimitive x =
  let generateJsonMatch (matches:_) =
    sprintf
      @"(match x with | %s | _ -> Error [name,""type""])"
      (matches |> Seq.map (fun (t,m) -> sprintf "%s(x) -> (%s)" t m) |> String.concat " | " )
  let generateParserMatch (parse:string) (check:string) =
    sprintf
      @"(match %s with | %s -> Ok result | _ -> Error [name,""invalid""])"
      parse
      check
  match x with
  | FKDynamic -> failwith "generateParserPrimitive - cannot generate FKDynamic"
  | FKInstant ->
    generateJsonMatch [
      "JsonValue.String",generateParserMatch "NodaTime.Text.InstantPattern.General.Parse(x)" "NodaParseSuccess result"
    ]
  | FKDateTime ->
    generateJsonMatch [
      "JsonValue.String",generateParserMatch @"NodaTime.Text.LocalDateTimePattern.CreateWithInvariantCulture(""yyyy-MM-dd HH:mm:ss"").Parse(x)" "NodaParseSuccess result"
    ]
  | FKDate ->
    generateJsonMatch [
      "JsonValue.String",generateParserMatch @"NodaTime.Text.LocalDatePattern.CreateWithInvariantCulture(""yyyy-MM-dd"").Parse(x)" "NodaParseSuccess result"
    ]
  | FKTime ->
    generateJsonMatch [
      "JsonValue.String",generateParserMatch @"NodaTime.Text.LocalTimePattern.CreateWithInvariantCulture(""HH:mm:ss"").Parse(x)" "NodaParseSuccess result"
    ]
  | FKBool ->
    generateJsonMatch [
      "JsonValue.Boolean","Ok x"
      "JsonValue.String",generateParserMatch "System.Boolean.TryParse(x)" "true,result"
    ]
  | FKGuid ->
    generateJsonMatch [
      "JsonValue.String",generateParserMatch "System.Guid.TryParse(x)" "true,result"
    ]
  | FKInt ->
    // TODO: does this work for large integers?
    sprintf
      @"(match x with | JsonValue.Float(x) when x %% 1.0 = 0.0 -> Ok (int x) | JsonValue.Number(x) when x %% 1M = 0M -> Ok (int x) | JsonValue.String(x) -> %s | _ -> Error [name,""type""])"
      (generateParserMatch
        "System.Int32.TryParse(x)"
        "true,result"
      )
  | FKDecimal ->
    sprintf
      @"(match x with | JsonValue.Float(x) -> Ok (decimal x) | JsonValue.Number(x) -> Ok x | JsonValue.String(x) -> %s | _ -> Error [name,""type""])"
      (generateParserMatch
        "System.Decimal.TryParse(x)"
        "true,result"
      )
  | FKStringE ->
    sprintf
      @"(match x with | JsonValue.String(x) -> Ok x | _ -> Error [name,""type""])"
  | FKStringNE ->
    sprintf
      @"(match x with | JsonValue.String("""") -> Error [name,""required""] | JsonValue.String(x) -> Ok x | _ -> Error [name,""type""])"

let generateParser (e:bool) (dvs:bool) (field:FieldDefinition) : string =
  let rec inner (inArray:bool) ({ name=name;kind=kind } as field) (fieldname:FieldName option) =
    match kind with
    | FKPrimitive FKDynamic ->
      sprintf
        @"(match dynamics.%s %s x with | Ok x -> Ok x | Error es -> Error es)"
        name.extract
        (match e with | true -> "(__,i)" | false -> "__")
    | FKPrimitive x ->
      generateParserPrimitive x
    | FKValid x ->
      sprintf
        @"(match %s with | Ok x -> (match validates.%s __ x with | Ok x -> Ok x | Error x -> Error [name,%s]) | Error x -> Error x)"
        (inner inArray { field with name=name;kind=x } None)
        name.extract
        @"(sprintf ""%s"" x)"
    | FKOption kind ->
      sprintf
        @"(match x with | JsonValue.Null -> Ok None | x -> Result.map Some %s)"
        (inner inArray { field with name=name;kind=kind } None)
    | FKArray kind ->
      sprintf
        @"(match x with | JsonValue.Array(values) -> Result.map List.toArray <| Seq.foldBack (fun x agg -> match x,agg with | Ok x,Ok xs -> Ok (x::xs) | Error es1,Error es2 -> Error (es1@es2) | Error es,Ok _ -> Error es | Ok _,Error es -> Error es) (Array.mapi (fun i x -> let name = sprintf ""%%s[%%d]"" name i in %s) values) (Ok []) | _ -> Error [name,""type""])"
        (inner true { field with name=name;kind=kind } (Some name))
    | FKRecord record as record_ ->
      sprintf
        @"(match x with | JsonValue.Record(properties) -> Result.mapError (List.map (fun (f,m) -> sprintf ""%%s.%%s"" name f,m)) (%s._parseJson%s (%s)) | _ -> Error [name,""type""])"
        record.name.extract
        (match e || inArray with | true -> "E" | false -> "")
        ([
          Some "Map.ofArray properties"
          match e || inArray with | true -> Some "i" | false -> None
          match hasDynamics record_ || hasValidates record_ with | false -> None | true -> Some "__"
          match hasDynamics record_ with | false -> None | true -> Some (fieldname |> Option.map (fun x -> sprintf "dynamics.%s" x.extract) |> Option.defaultValue "dynamics")
          match hasValidates record_ with | false -> None | true -> Some (sprintf "validates.%s" name.extract)
        ] |> List.choose id |> String.concat ",")
    | FKLiteral literal ->
      sprintf
        @"(%s.parseJson x)"
        literal
  sprintf
    @"(let name = ""%s"" in match map.TryFind(""%s"") with | Some x -> %s | None -> Error [name,""missing""])"
    field.name.extract
    field.name.extract
    (inner false field None)
let generateParseJson (e:bool) (dvs:bool) ({ name=name;fields=fields } as rd:RecordDefinition) : string =
  let parse =
    fields
    |> Array.map (generateParser e dvs)
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
  match dvs with
  | true ->
    sprintf
      @"(match _%s.parseJson map with | Ok __ -> %s._parseJson (%s) | Error e -> Error e)"
      name.extract
      name.extract
      ([
        Some "map"
        match generateDynamicsParameter (if dvs then Some "" else if e then Some "'__ * int" else None) rd |> Option.map (fun _ -> "dynamics"),generateValidatesParameter (if dvs then Some "" else None) rd |> Option.map (fun _ -> "validates") with
        | None,None -> None
        | _ -> Some "__"
        generateDynamicsParameter  (if dvs then Some "" else if e then Some "'__ * int" else None) rd |> Option.map (fun _ -> "dynamics")
        generateValidatesParameter (if dvs then Some "" else None) rd |> Option.map (fun _ -> "validates")
      ] |> List.choose id |> String.concat ",")
  | false ->
    sprintf
      @"(match %s with | %s -> %s | %s -> %s)"
      parse
      success
      construct
      failure
      error

let parameterToString (p:{| name:string;kind:string |}) : string =
  sprintf "(%s:%s)" p.name p.kind
let generateTypeDynamics (dvs:string option) (rd:RecordDefinition) =
  generateDynamicsParameter dvs rd |> Option.map (fun x -> sprintf "type %s_Dynamic%s = %s" rd.name.extract (Option.defaultValue "" (generateDynamicsTypeParametersInOut rd)) x.kind)
let generateTypeValidates (dvs:string option) (rd:RecordDefinition) =
  generateValidatesParameter dvs rd |> Option.map (fun x -> sprintf "type %s_Validate%s = %s" rd.name.extract (Option.defaultValue "<'__>" (generateDynamicsTypeParametersInOut rd)) x.kind)
let generateField : FieldDefinition -> string = function
  | { name=name;kind=kind } ->
    sprintf "%s:%s" name.extract (generateKind name kind)
let rec removeDVs (rd:RecordDefinition) : RecordDefinition =
  printfn "removeDVs - %A" rd
  let rec inner : FieldDefinition -> FieldDefinition option = function
    | { kind=FKPrimitive FKDynamic } ->
      None
    | { kind=FKPrimitive _ }
    | { kind=FKLiteral _ } as x ->
      Some x
    | { kind=FKArray (FKRecord y) } as x ->
      inner { x with kind=FKRecord y } |> Option.map (fun z -> { z with kind=FKArray z.kind })
    | { kind=FKOption (FKRecord y) } as x ->
      inner { x with kind=FKRecord y } |> Option.map (fun z -> { z with kind=FKOption z.kind })
    | { kind=FKValid (FKRecord y) } as x ->
      inner { x with kind=FKRecord y } |> Option.map (fun z -> { z with kind=z.kind })
    | { kind=FKArray y } as x ->
      inner { x with kind=y } |> Option.map (fun z -> { z with kind=FKArray z.kind })
    | { kind=FKOption y } as x ->
      inner { x with kind=y } |> Option.map (fun z -> { z with kind=FKOption z.kind })
    | { kind=FKValid y } as x ->
      inner { x with kind=y } |> Option.map (fun z -> { z with kind=z.kind })
    | { kind=FKRecord y } as x ->
      match y.fields |> Array.choose inner with
      | [||] -> None
      | fs -> Some { x with kind=FKRecord { y with name=RecordName ("_"+y.name.extract);fields=fs } }
  { rd with fields=rd.fields |> Array.choose inner }
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
let recordDefinitionToFieldKind (x:RecordDefinition) : FieldKind =
  FKRecord x
let generateRecord (x':RecordDefinition) : string seq = seq {
  let whatever1 (dvs:bool) (x:RecordDefinition) =
    let _parametersE = List.choose id [
      generateDynamicsParameter (Some "'__ * int") x
      generateValidatesParameter None x
    ]
    let _parameters = List.choose id [
      generateDynamicsParameter (if hasDynamicInArray (recordDefinitionToFieldKind x') then Some "'__ * int" else None) x
      generateValidatesParameter None x
    ]
    let parameters = List.choose id [
      match dvs,hasDynamicInArray (recordDefinitionToFieldKind x') with
      | false,false -> generateDynamicsParameter None x
      | true,false -> generateDynamicsParameter (if dvs then Some ("_"+x.name.extract) else None) x
      | true,true -> generateDynamicsParameter (if dvs then Some ("_"+x.name.extract+" * int") else Some "'__ * int") x
      | false,true -> generateDynamicsParameter (Some "'__ * int") x
      generateValidatesParameter (if dvs then Some ("_"+x.name.extract) else None) x
    ]
    let _parametersSignatureE =
      match _parametersE with
      | [] -> ""
      | xs   -> sprintf ",(__:'__),%s" (xs |> List.map parameterToString |> String.concat ",")
    let _parametersSignature =
      match _parameters with
      | [] -> ""
      | xs   -> sprintf ",(__:'__),%s" (xs |> List.map parameterToString |> String.concat ",")
    let parametersSignature =
      match parameters with
      | [] -> ""
      | xs -> sprintf ",%s" (xs |> List.map parameterToString |> String.concat ",")
    let parametersCall =
      match parameters with
      | [] -> ""
      | xs -> sprintf ",%s" (xs |> List.map (fun x -> x.name) |> String.concat ",")
    sprintf @"
type %s = %s with
  // TODO: handle structure errors and parse errors separately
  static member _parseJsonE (map:Map<string,JsonValue>,i:int%s) : Result<%s,(string*string) list> =
    %s
  static member _parseJson (map:Map<string,JsonValue>%s) : Result<%s,(string*string) list> =
    %s
  static member parseJson (map:Map<string,JsonValue>%s) : Result<%s,(string*string) list> =
    %s
  static member parseJson_ (x:JsonValue%s) : Result<%s,(string*string) list> =
    match x with
    | JsonValue.Record(properties) -> %s.parseJson (Map.ofArray properties%s)
    | _ -> Error [""_"",""type""]
  static member parseJsonRaw (x:string%s) : Result<%s,(string * string) list> =
    match JsonValue.TryParse(x) with
    | Some x -> %s.parseJson_ (x%s)
    | None -> Error [""_"",""parse""]
  %s
"
  (*
  static member parseJsonArray (x:JsonValue array%s) : Result<%s array,((string*string) list) array> =
    // TODO: loop through calling `parseJson` on each, collecting all success or set of failures
    failwith ""parseJsonArray: not implemented""
  static member parseJsonRawArray (x:string%s) : Result<%s array,((string * string) list) array> =
    match JsonValue.TryParse(x) with
    | Some (JsonValue.Array(values)) ->
      %s.parseJsonArray (values%s)
    | Some _ -> Error [|[""_"",""type""]|]
    | None -> Error [|[""_"",""parse""]|]
  *)
  (*
      // parseJsonArray definition:
      parametersSignature
      (generateTypeName x)
      // name.extract
      // parametersCall

      // parseJsonRawArray definition:
      parametersSignature
      (generateTypeName x)
      x.name.extract
      parametersCall
  *)
      // type definition:
      (generateTypeName x)
      (x.fields |> Array.map generateField |> String.concat ";" |> sprintf "{ %s }")

      // _parseJsonE definition
      _parametersSignatureE
      (generateTypeName x)
      (generateParseJson true false x)

      // _parseJson definition
      _parametersSignature
      (generateTypeName x)
      (generateParseJson false false x)

      // parseJson definition
      parametersSignature
      (generateTypeName x)
      (generateParseJson false dvs x)

      // parseJson_ definition:
      parametersSignature
      (generateTypeName x)
      x.name.extract
      parametersCall

      // parseJsonRaw definition:
      parametersSignature
      (generateTypeName x)
      x.name.extract
      parametersCall

      (generateFromSegments x)
  yield generateTypeDynamics None x' |> Option.defaultValue ""
  yield generateTypeValidates None x' |> Option.defaultValue ""
  yield whatever1 false { removeDVs x' with name=RecordName ("_"+x'.name.extract) }
  yield whatever1 true x'
}

let target = "out/parsers.fs"
let generated () =
  printfn "%A" fsi.CommandLineArgs
  let valfiles = fsi.CommandLineArgs |> Array.skip 1 |> Array.collect (fun x -> x.Split(';') |> Array.filter (fun x -> not (System.String.IsNullOrWhiteSpace(x))))
  printfn "%A" valfiles
  seq {
    yield "module App.GeneratedParsers"
    yield "open FSharp.Data"
    yield "type DynamicParser<'i,'o> = 'i -> JsonValue -> Result<'o,(string*string) list>"
    yield "type ValidateParser<'i,'o> = 'i -> 'o -> Result<'o,string>"
    yield "let private (|NodaParseSuccess|NodaParseFailure|) (x:NodaTime.Text.ParseResult<'a>) = match x.TryGetValue(Unchecked.defaultof<'a>) with | true,x -> NodaParseSuccess x | false,_ -> NodaParseFailure"
    yield sprintf "let _parseDate name x = %s" (generateParserPrimitive FKDate)
    yield sprintf "let _parseTime name x = %s" (generateParserPrimitive FKTime)
    yield sprintf "let _parseDateTime name x = %s" (generateParserPrimitive FKDateTime)
    yield sprintf "let _parseInstant name x = %s" (generateParserPrimitive FKInstant)
    yield sprintf "let _parseGuid name x = %s" (generateParserPrimitive FKGuid)
    yield sprintf "let _parseInt name x = %s" (generateParserPrimitive FKInt)
    yield sprintf "let _parseDecimal name x = %s" (generateParserPrimitive FKDecimal)
    yield sprintf "let _parseBool name x = %s" (generateParserPrimitive FKBool)
    yield sprintf "let _parseStringNE name x = %s" (generateParserPrimitive FKStringNE)
    yield sprintf @"let _parseStringE name x = %s" (generateParserPrimitive FKStringE)
    yield sprintf @"let _parseOption f name x = match x with | JsonValue.Null -> Ok None | x -> Result.map Some (f name x)"
    for file in valfiles do
      printfn "%A" file
      match Parse.parseFile { name=file;stream=File.OpenRead(file);encoding=System.Text.Encoding.UTF8 } with
      | FParsec.CharParsers.Success ({ blocks=blocks },_,_) ->
        for block in blocks do
          match block with
          | ModuleBlockRecord record ->
            yield! generateRecord record
          | ModuleBlockRaw raw ->
            yield raw
      | FParsec.CharParsers.Failure (result,_,_) ->
        failwith result
  }
try
  let sb = System.Text.StringBuilder()
  for line in generated () do
    ignore<System.Text.StringBuilder> <| sb.AppendLine(line)
  let result = sb.ToString()
  using (File.CreateText(target)) <| fun writer -> writer.Write(result)
with
| e ->
  printfn "%s:%s" e.Message e.StackTrace
  failwith e.Message
