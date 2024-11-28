use std::fmt::Display;

use anyhow::Result;

use winnow::{
    ascii::{digit1, multispace0, Caseless},
    combinator::{alt, delimited, opt, separated, separated_pair, trace},
    error::{ContextError, ErrMode, ParserError},
    stream::{AsBStr, AsChar, Compare, FindSlice, ParseSlice, Stream, StreamIsPartial},
    token::take_until,
    PResult, Parser,
};
#[derive(Debug, Clone, PartialEq)]
enum Num {
    Int(i64),
    Float(f64),
}
#[derive(Debug, Clone, PartialEq)]
enum JsonValue {
    Null,
    Bool(bool),
    Number(Num),
    String(String),
    Array(Vec<JsonValue>),
    Object(std::collections::HashMap<String, JsonValue>),
}
fn main() -> Result<()> {
    let s = r#"{
        "name": "John Doe",
        "age": 43,
        "is_adult": true,
        "mark:": [90.1, 80.2, 85.0],
        "address": {
            "city": "New York",
            "state": "NY"
        }
    }"#;
    let s = &mut (&*s);
    let v = parse_value(s).map_err(|e: ErrMode<ContextError>| anyhow::anyhow!(e))?;
    println!("{:#?}", v);
    Ok(())
}

fn parse_null<Input, Error>(s: &mut Input) -> PResult<(), Error>
where
    Input: StreamIsPartial + Stream + Compare<&'static str>,
    Error: ParserError<Input>,
{
    "null".value(()).parse_next(s)
}

fn parse_bool<Input, Error>(s: &mut Input) -> PResult<bool, Error>
where
    Input: StreamIsPartial + Stream + Compare<&'static str>,
    <Input as Stream>::Slice: ParseSlice<bool>,
    Error: ParserError<Input>,
{
    alt(("true", "false")).parse_to().parse_next(s)
}

// fn parse_int<Input, Error>(s: &mut Input) -> PResult<i64, Error>
// where
//     Input: StreamIsPartial + Stream + Compare<&'static str>,
//     <Input as Stream>::Token: AsChar,
//     <Input as Stream>::Slice: ParseSlice<i64>,
//     Error: ParserError<Input>,
// {
//     let sign = opt("-").map(|x| x.is_some()).parse_next(s)?;
//     let num = digit1.parse_to::<i64>().parse_next(s)?;
//     Ok(if sign { -num } else { num })
// }

fn parse_number<Input, Error>(s: &mut Input) -> PResult<Num, Error>
where
    Input: StreamIsPartial
        + Stream
        + AsBStr
        + Compare<Caseless<&'static str>>
        + Clone
        + Compare<char>
        + Compare<&'static str>,
    <Input as Stream>::Slice:
        ParseSlice<u64> + ParseSlice<bool> + ParseSlice<f64> + Display + Clone,

    <Input as Stream>::Token: AsChar + Clone,
    <Input as Stream>::IterOffsets: Clone,
    <Input as Stream>::Slice: ParseSlice<f64>,
    Error: ParserError<Input>,
{
    let sign = opt("-").map(|x| x.is_some()).parse_next(s)?;
    let num = digit1.parse_to::<u64>().parse_next(s)?;
    let ret: std::result::Result<<Input as Stream>::Slice, ErrMode<ContextError>> =
        ".".parse_next(s);

    if ret.is_ok() {
        let frac = digit1.parse_to::<u64>().parse_next(s)?;
        let total = num as f64 + frac as f64 / 10u64.pow(frac.to_string().len() as u32) as f64;
        Ok(Num::Float(if sign { -total as _ } else { total as _ }))
    } else {
        Ok(Num::Int(if sign { -(num as i64) } else { num as _ }))
    }
}

fn parse_string<Input, Error>(s: &mut Input) -> PResult<String, Error>
where
    Input: StreamIsPartial + Stream + Compare<char> + FindSlice<char> + AsBStr,
    <Input as Stream>::Token: AsChar,

    <Input as Stream>::Slice: Display,
    Error: ParserError<Input>,
{
    let ret = delimited('"', take_until(0.., '"'), '"').parse_next(s)?;
    Ok(ret.to_string())
}

fn parse_array<Input, Error>(s: &mut Input) -> PResult<Vec<JsonValue>, Error>
where
    Input: StreamIsPartial
        + Stream
        + Compare<char>
        + Compare<&'static str>
        + Compare<Caseless<&'static str>>
        + AsBStr
        + FindSlice<char>
        + Clone,
    <Input as Stream>::Slice:
        ParseSlice<i64> + ParseSlice<bool> + ParseSlice<u64> + ParseSlice<f64> + Display + Clone,
    <Input as Stream>::Token: AsChar + Clone,
    <Input as Stream>::IterOffsets: Clone,
    Error: ParserError<Input>,
{
    let sep1 = sep_with_ws('[');
    let sep2 = sep_with_ws(']');
    let sep_comma = sep_with_ws(',');

    let ret = delimited(sep1, separated(0.., parse_value, sep_comma), sep2).parse_next(s)?;

    Ok(ret)
}

fn sep_with_ws<Input, Output, Error, ParseNext>(
    mut parser: ParseNext,
) -> impl Parser<Input, (), Error>
where
    Input: Stream + StreamIsPartial,

    <Input as Stream>::Token: Clone + AsChar,
    Error: ParserError<Input>,
    ParseNext: Parser<Input, Output, Error>,
{
    trace("sep_with_ws", move |input: &mut Input| {
        let _ = multispace0.parse_next(input)?;
        parser.parse_next(input)?;
        multispace0.parse_next(input)?;
        Ok(())
    })
}

fn parse_object<Input, Error>(
    s: &mut Input,
) -> PResult<std::collections::HashMap<String, JsonValue>, Error>
where
    Input: StreamIsPartial
        + Stream
        + Compare<char>
        + Compare<&'static str>
        + Compare<Caseless<&'static str>>
        + AsBStr
        + FindSlice<char>
        + Clone,
    <Input as Stream>::Slice:
        Display + ParseSlice<i64> + ParseSlice<bool> + ParseSlice<f64> + ParseSlice<u64> + Clone,
    <Input as Stream>::Token: AsChar + Clone,
    <Input as Stream>::IterOffsets: Clone,
    Error: ParserError<Input>,
{
    let sep1 = sep_with_ws('{');
    let sep2 = sep_with_ws('}');
    let sep_comma = sep_with_ws(',');
    let sep_sep = sep_with_ws(':');
    let parse_kv_pair = separated_pair(parse_string, sep_sep, parse_value);
    let parse_kv = separated(1.., parse_kv_pair, sep_comma);
    let ret = delimited(sep1, parse_kv, sep2).parse_next(s)?;
    Ok(ret)
}

fn parse_value<Input, Error>(s: &mut Input) -> PResult<JsonValue, Error>
where
    Input: StreamIsPartial
        + Stream
        + Compare<char>
        + Compare<&'static str>
        + Compare<Caseless<&'static str>>
        + AsBStr
        + FindSlice<char>
        + Clone,
    <Input as Stream>::Slice:
        ParseSlice<i64> + ParseSlice<bool> + ParseSlice<f64> + ParseSlice<u64> + Display + Clone,
    <Input as Stream>::Token: AsChar + Clone,
    <Input as Stream>::IterOffsets: Clone,
    Error: ParserError<Input>,
{
    println!("parse_value{:?}", s);
    alt((
        parse_null.map(|_| JsonValue::Null),
        parse_bool.map(JsonValue::Bool),
        // alt((
        //     float.map(JsonValue::Number),
        //     parse_int.map(JsonValue::Int),
        //     // parse_number.map(JsonValue::Number),
        // )), //
        parse_number.map(JsonValue::Number),
        parse_string.map(JsonValue::String),
        parse_array.map(JsonValue::Array),
        parse_object.map(JsonValue::Object),
    ))
    .parse_next(s)
}

#[cfg(test)]
mod tests {
    use winnow::error::InputError;

    use super::*;
    #[test]
    fn test_parse_null() {
        let s = "null";
        parse_null::<&str, InputError<&str>>(&mut (&*s)).unwrap();
        // assert_eq!((), ());
    }
    #[test]
    fn test_parse_bool() {
        let s = "true";
        let v = parse_bool::<&str, InputError<&str>>(&mut (&*s)).unwrap();
        assert!(v);
        let s = "false";
        let v = parse_bool::<&str, InputError<&str>>(&mut (&*s)).unwrap();
        assert!(!v);
    }
    // #[test]
    // fn test_parse_int() {
    //     let s = "123";
    //     let v = parse_int::<&str, InputError<&str>>(&mut (&*s)).unwrap();
    //     assert_eq!(v, 123);
    //     let s = "-123";
    //     let v = parse_int::<&str, InputError<&str>>(&mut (&*s)).unwrap();
    //     assert_eq!(v, -123);
    // }
    #[test]
    fn test_parse_number() {
        let s = "123.45";
        let v = parse_number::<&str, InputError<&str>>(&mut (&*s)).unwrap();
        assert_eq!(v, Num::Float(123.45));
    }
    #[test]
    fn test_parse_string() {
        let s = r#""hello""#;
        let v = parse_string::<&str, InputError<&str>>(&mut (&*s)).unwrap();
        assert_eq!(v, "hello");
    }

    #[test]
    fn test_parse_array() {
        let s = r#"[90.1, 80.2, 85.0]"#;
        let v = parse_array::<&str, InputError<&str>>(&mut (&*s)).unwrap();
        assert_eq!(
            v,
            vec![
                JsonValue::Number(Num::Float(90.1)),
                JsonValue::Number(Num::Float(80.2)),
                JsonValue::Number(Num::Float(85.0))
            ]
        );

        let s = r#"[90, 80, 85]"#;
        let v = parse_array::<&str, InputError<&str>>(&mut (&*s)).unwrap();
        assert_eq!(
            v,
            vec![
                JsonValue::Number(Num::Int(90)),
                JsonValue::Number(Num::Int(80)),
                JsonValue::Number(Num::Int(85))
            ]
        );
        // let input = r#"["a", "b", "c"]"#;
        // let v = parse_array::<&str, InputError<&str>>(&mut (&*input)).unwrap();
        // assert_eq!(
        //     v,
        //     vec![
        //         JsonValue::String("a".to_string()),
        //         JsonValue::String("b".to_string()),
        //         JsonValue::String("c".to_string())
        //     ]
        // );
    }
    #[test]
    fn test_parse_object() {
        let s = r#"{"a": 1, "b": 2, "c": 3}"#;
        let v = parse_object::<&str, InputError<&str>>(&mut (&*s)).unwrap();
        let mut map = std::collections::HashMap::new();
        map.insert("a".to_string(), JsonValue::Number(Num::Int(1)));
        map.insert("b".to_string(), JsonValue::Number(Num::Int(2)));
        map.insert("c".to_string(), JsonValue::Number(Num::Int(3)));

        assert_eq!(v, map);
    }
}
