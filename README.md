# App.GeneratedParsers
A strongly-typed JSON parser generator for F# using a declarative VAL schema format.
Documentation style inspired by Giraffe, Saturn, and FSharp.Data.

---

## Overview

App.GeneratedParsers is a code-generation system that transforms a compact, human-friendly VAL schema into:

- Fully typed F# record definitions
- JSON parsers (parseJson, parseJson_, parseJsonRaw)
- Automatic validation plumbing
- Automatic dynamic type parameters
- Composition for nested, optional, validated, and array fields
- Support for record expansion (field inheritance)
- Embedded !raw F# blocks for custom code

The resulting `parsers.fs` file contains fully usable F# types and functions for ingestion, ETL layers, and strict typed parsing.

---

## Quick Start

### 1. Create a schema file (VAL format)

Example: `schema.val`

```
@Address
street: stringne
city: stringne

@Person
... Address
name: stringne
age: int valid
pet: @Pet option

@Pet
kind: stringne
age: int
```

---

### 2. Run the generator

```
dotnet fsi generate.fsx schema.val
```

Output will be written to:

```
out/parsers.fs
```

---

### 3. Use the generated types and parsers

```fsharp
open App.GeneratedParsers

let json = """{ "name":"Alice", "street":"Main", "city":"NY", "age":30 }"""

match Person.parseJsonRaw json with
| Ok p -> printfn $"Parsed: %A{p}"
| Error errs -> printfn $"Errors: %A{errs}"
```

---

## The VAL Schema Specification

The VAL format defines F# record structures using a compact type notation.

### Record Definition

```
@RecordName
fieldName: fieldKind
```

A schema file may define multiple records.

---

## Field Kinds

### Primitive Types

| VAL       | F# Type                         |
|-----------|---------------------------------|
| int       | int                             |
| decimal   | decimal                         |
| bool      | bool                            |
| stringe   | string (empty allowed)          |
| stringne  | string (empty forbidden)        |
| date      | NodaTime.LocalDate              |
| time      | NodaTime.LocalTime              |
| datetime  | NodaTime.LocalDateTime          |
| instant   | NodaTime.Instant                |
| guid      | System.Guid                     |
| dynamic   | 'fieldname (generated type var) |

---

### Record Reference

```
pet: @Pet
```

---

### Literal Parser Type

```
level: !Level
```

This invokes:

```fsharp
Level.parseJson x
```

---

## Field Wrappers

Wrappers can be applied in sequence. They evaluate right-to-left.

| Wrapper | Meaning |
|---------|---------|
| option  | Optional field |
| array   | Array of values |
| valid   | Attaches a validator |

Example:

```
age: int valid option
```

Interpreted as `(int valid) option`.

---

## Record Expansion

A record may inherit fields from another:

```
@Person
... Address
name: stringne
```

This expands all fields from `Address` into `Person`.

---

## Dynamic Fields

Fields declared as `dynamic` automatically generate type parameters.

Example:

```
payload: dynamic
```

Generates:

```fsharp
type Event<'payload> = { payload: 'payload }
```

Nested dynamic fields result in appropriate type parameter structures.

---

## Validation Support

A `valid` wrapper attaches validation requirements.

Example:

```
age: int valid
```

You must supply a validator:

```fsharp
let validateAge : ValidateParser<unit,int> =
  fun _ age ->
    if age >= 0 then Ok age
    else Error "Age must be non-negative"
```

Call the parser:

```fsharp
Person.parseJson(map, validates = {| age = validateAge |})
```

---

## JSON Parsing API

Each record generates the following functions:

| Function | Purpose |
|----------|---------|
| parseJson     | Parse Map<string, JsonValue> |
| parseJson_    | Parse JsonValue             |
| parseJsonRaw  | Parse raw JSON string       |

Example:

```fsharp
match Person.parseJsonRaw "{\"name\":\"Bob\",\"age\":40}" with
| Ok person -> printfn "OK: %A" person
| Error errors -> printfn "ERR: %A" errors
```

---

## Error Reporting

Errors are returned as:

```
("fieldName", "reason")
```

Possible reasons include:

- missing
- type
- required
- invalid
- parse

---

## Raw Blocks

You may embed raw F# code inside a VAL file:

```
!raw
let someUtility x = x + 1
!endraw
```

These blocks are copied directly into the generated output file.

---

## Advanced Features

### Segmented Constructors

If a record uses expansion, a segmented constructor is generated:

```fsharp
static member fromSegments address {| name = "Alice" |}
```

---

# Complete Example

This example demonstrates:

1. A VAL schema with nested records, dynamic fields, and validation  
2. The generated F# types  
3. A fully typed HTTP handler consuming the parsed & validated model  

---

## 1. VAL Schema

```
@Event
id: guid
timestamp: instant
payload: dynamic
metadata: @Metadata option valid

@Metadata
source: stringne
tags: stringe array
```

---

## 2. Generated Types (Simplified)

```fsharp
type Event<'payload> = {
  id: Guid
  timestamp: Instant
  payload: 'payload
  metadata: Metadata option
}

type _Event = {
  id: Guid
  timestamp: Instant
  payload: obj
  metadata: Metadata option
}

type Event_Validate<'ctx> = {
  metadata: ValidateParser<'ctx, Metadata option>
}
```

The `Event<'payload>` type is what your handler receives  
after successful parsing and validation.

The `_Event` type is used internally by the parser generator.

The `Event_Validate<'ctx>` type indicates which fields require or support custom validation.

---

## 3. Example HTTP Handler (Conceptual)

The following demonstrates how a handler consumes the generated types and validation contract.

```fsharp
let handleEventInput : HttpHandler =
  App.Util.Web.withBodyParsedJsonValidate<
      App.GeneratedParsers.Event<'payload>,
      App.GeneratedParsers.Event_Validate<App.GeneratedParsers._Event>
    >
    {|
      metadata = fun _ md ->
        match md with
        | None -> Ok None
        | Some m ->
            if m.source = "" then Error "Metadata.source cannot be empty."
            else Ok (Some m)
    |}
    (fun eventModel next ctx -> task {
      // eventModel : Event<'payload>
      // This is fully parsed and validated input.

      // Example application logic:
      let logger = ctx.GetService<ILogger>()
      logger.LogInformation("Received event {id}", eventModel.id)

      return! Successful.OK eventModel next ctx
    })
```

This illustrates:

- `withBodyParsedJsonValidate` performs JSON parsing using the generated parser  
- Validation runs automatically using the supplied validation record  
- `eventModel` is fully typed (`Event<'payload>`) when it reaches your logic  
- No manual JSON parsing or structure checking is necessary  



## License

MIT License.
