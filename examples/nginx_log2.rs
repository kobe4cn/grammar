use std::{
    fmt::Debug,
    fs::File,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    str::{self},
    sync::Arc,
};

use chrono::{DateTime, Utc};

use winnow::{
    ascii::{digit1, hex_digit1, space1},
    combinator::{alt, delimited, separated},
    token::take_until,
    PResult, Parser,
};
//parse

use arrow::{
    array::{RecordBatch, StringArray, UInt16Array, UInt64Array},
    datatypes::{DataType, Field, Schema},
};

use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
#[derive(Debug, Copy, Clone)]
enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
    Head,
    Options,
    Connect,
    Trace,
    Patch,
}
#[derive(Debug, Copy, Clone)]
enum HttpProtocol {
    HTTP1_0,
    HTTP1_1,
    HTTP2_0,
    HTTP3_0,
}

#[derive(Debug)]
#[allow(unused)]
struct NginxLog {
    addr: IpAddr,
    datetime: DateTime<Utc>,
    method: HttpMethod,
    url: String,
    protocol: HttpProtocol,
    status: u16,
    body_bytes: u64,
    referrer: String,
    ua: String,
}
impl From<HttpProtocol> for String {
    fn from(p: HttpProtocol) -> String {
        match p {
            HttpProtocol::HTTP1_0 => "HTTP/1.0".to_string(),
            HttpProtocol::HTTP1_1 => "HTTP/1.1".to_string(),
            HttpProtocol::HTTP2_0 => "HTTP/2.0".to_string(),
            HttpProtocol::HTTP3_0 => "HTTP/3.0".to_string(),
        }
    }
}
impl From<HttpMethod> for String {
    fn from(p: HttpMethod) -> String {
        match p {
            HttpMethod::Get => "GET".to_string(),
            HttpMethod::Post => "POST".to_string(),
            HttpMethod::Put => "PUT".to_string(),
            HttpMethod::Delete => "DELETE".to_string(),
            HttpMethod::Head => "HEAD".to_string(),
            HttpMethod::Options => "OPTIONS".to_string(),
            HttpMethod::Connect => "CONNECT".to_string(),
            HttpMethod::Trace => "TRACE".to_string(),
            HttpMethod::Patch => "PATCH".to_string(),
        }
    }
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // let s = r#"2a01:7e00::f03c:91ff:fe70:a4cc - - [18/May/2015:16:05:29 +0000] "GET /downloads/product_1 HTTP/1.1" 200 85619205 "-" "Chef Client/12.0.3 (ruby-2.1.4-p265; ohai-8.0.1; x86_64-linux; +http://opscode.com)""#;
    // let logs=parse_nginx_log(s);
    // let mut s = r#"2a01:7e00::f03c:91ff:fe70:a4cc"#;
    // let mut s = r#"2001:4801:7824:102:8bee:6e66:ff10:6aa2"#;
    // let ip = parse_ip(&mut s);
    // println!("{:?}", ip);
    read_nginx_log("https://raw.githubusercontent.com/elastic/examples/master/Common%20Data%20Formats/nginx_logs/nginx_logs").await?;
    Ok(())
}

//parse this log
//93.180.71.3 - - [17/May/2015:08:05:32 +0000] "GET /downloads/product_1 HTTP/1.1" 304 0 "-" "Debian APT-HTTP/1.3 (0.8.16~exp12ubuntu10.21)"
#[allow(clippy::useless_asref)]
fn parse_nginx_log(s: &str) -> PResult<NginxLog> {
    // println!("++++++++++++++s {}", s);
    let input = &mut s.as_ref();
    let ip = parse_ip(input)?;
    space1(input)?;
    parse_ignore(input)?;
    parse_ignore(input)?;
    let datetime = parse_datetime(input)?;
    let (method, url, protocol) = parse_http(input)?;
    let status = parse_status(input)?;
    let body_bytes = parse_body_bytes(input)?;
    let referrer = parse_referrer(input)?;
    let ua = parse_ua(input)?;
    Ok(NginxLog {
        addr: ip,
        datetime,
        method,
        url,
        protocol,
        status,
        body_bytes,
        referrer,
        ua,
    })
}

fn parse_ignore(s: &mut &str) -> PResult<()> {
    let _ = "-".parse_next(s)?;
    space1(s)?;
    Ok(())
}

fn parse_ip(s: &mut &str) -> PResult<IpAddr> {
    // parse_ip_v4.map(IpAddr).or(parse_ip_v6.map(IpAddr::V6)).parse_next(s);
    alt((parse_ip_v4, parse_ip_v6_len)).parse_next(s)
}
//parse 54.194.175.38
//parse 2001:4801:7824:102:8bee:6e66:ff10:6aa2
fn parse_ip_v4(s: &mut &str) -> PResult<IpAddr> {
    // println!("v4 {}", s);
    let ret: Vec<u8> = separated(4, digit1.parse_to::<u8>(), '.').parse_next(s)?;
    // space1(s)?;
    Ok(IpAddr::V4(Ipv4Addr::new(ret[0], ret[1], ret[2], ret[3])))
}

fn parse_ip_v6_len(s: &mut &str) -> PResult<IpAddr> {
    let ret: Vec<u16> = separated(
        1..=8,
        |input: &mut &str| {
            hex_digit1
                .try_map(|s| u16::from_str_radix(s, 16))
                .parse_next(input)
        },
        ':',
    )
    .parse_next(s)?;
    // 处理压缩格式，补全缺失的零段

    // println!("{:?}", ret);
    // space1(s)?;
    Ok(IpAddr::V6(Ipv6Addr::new(
        ret[0], ret[1], ret[2], ret[3], ret[4], ret[5], ret[6], ret[7],
    )))
}

fn parse_datetime(s: &mut &str) -> PResult<DateTime<Utc>> {
    let ret = delimited('[', take_until(1.., ']'), ']').parse_next(s)?;
    space1(s)?;
    Ok(DateTime::parse_from_str(ret, "%d/%b/%Y:%H:%M:%S %z")
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap())
}

fn parse_http(s: &mut &str) -> PResult<(HttpMethod, String, HttpProtocol)> {
    let parser = (parse_method, parse_url, parse_protocol);
    let ret = delimited('"', parser, '"').parse_next(s)?;
    space1(s)?;
    Ok(ret)
}

fn parse_method(s: &mut &str) -> PResult<HttpMethod> {
    let ret = alt((
        "GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "CONNECT", "TRACE", "PATCH",
    ))
    .parse_next(s)?;
    space1(s)?;
    Ok(match ret {
        "GET" => HttpMethod::Get,
        "POST" => HttpMethod::Post,
        "PUT" => HttpMethod::Put,
        "DELETE" => HttpMethod::Delete,
        "HEAD" => HttpMethod::Head,
        "OPTIONS" => HttpMethod::Options,
        "CONNECT" => HttpMethod::Connect,
        "TRACE" => HttpMethod::Trace,
        "PATCH" => HttpMethod::Patch,
        _ => unreachable!(),
    })
}

fn parse_url(s: &mut &str) -> PResult<String> {
    let ret = take_until(1.., ' ').parse_next(s)?;
    space1(s)?;
    Ok(ret.to_string())
}

fn parse_protocol(s: &mut &str) -> PResult<HttpProtocol> {
    let ret = alt(("HTTP/1.0", "HTTP/1.1", "HTTP/2.0", "HTTP/3.0")).parse_next(s)?;
    Ok(match ret {
        "HTTP/1.0" => HttpProtocol::HTTP1_0,
        "HTTP/1.1" => HttpProtocol::HTTP1_1,
        "HTTP/2.0" => HttpProtocol::HTTP2_0,
        "HTTP/3.0" => HttpProtocol::HTTP3_0,
        _ => unreachable!(),
    })
}

fn parse_status(s: &mut &str) -> PResult<u16> {
    let ret = digit1.parse_to().parse_next(s)?;
    space1(s)?;
    Ok(ret)
}

fn parse_body_bytes(s: &mut &str) -> PResult<u64> {
    let ret = digit1.parse_to().parse_next(s)?;
    space1(s)?;
    Ok(ret)
}

fn parse_referrer(s: &mut &str) -> PResult<String> {
    let ret = delimited('"', take_until(1.., '"'), '"').parse_next(s)?;
    space1(s)?;
    Ok(ret.to_string())
}

fn parse_ua(s: &mut &str) -> PResult<String> {
    let ret = delimited('"', take_until(1.., '"'), '"').parse_next(s)?;
    Ok(ret.to_string())
}

async fn read_nginx_log(s: &str) -> anyhow::Result<()> {
    let url = s;
    let response = reqwest::get(url).await?;
    let mut data: Vec<NginxLog> = Vec::new();
    for line in response.text().await?.lines() {
        // let line = r#"2a01:7e00::f03c:91ff:fe70:a4cc - - [18/May/2015:16:05:29 +0000] "GET /downloads/product_1 HTTP/1.1" 200 85619205 "-" "Chef Client/12.0.3 (ruby-2.1.4-p265; ohai-8.0.1; x86_64-linux; +http://opscode.com)""#;

        println!("{}", line);
        //println!("+++++++++++++s {}", line);
        let mut zero = String::new();
        if line.contains("::") {
            if let Some((str1, _str2)) = line.split_once(" ") {
                let ct = 8 - str1.chars().filter(|&c| c == ':').count();
                println!("ct {}", ct);
                zero = ":0:".repeat(ct).replace("::", ":");
            }
        }
        // println!("zero {}", zero);
        let line = line.replace("::", zero.as_str());
        // println!("+++++++++++++s {}", line);
        let log = parse_nginx_log(line.as_str()).map_err(|e| anyhow::anyhow!(e))?;
        // println!("{:?}", log);
        data.push(log);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("ip", DataType::Utf8, false),
        Field::new("datetime", DataType::Utf8, false),
        Field::new("method", DataType::Utf8, false),
        Field::new("url", DataType::Utf8, false),
        Field::new("protocol", DataType::Utf8, false),
        Field::new("status", DataType::UInt16, false),
        Field::new("body_bytes", DataType::UInt64, false),
        Field::new("referrer", DataType::Utf8, false),
        Field::new("ua", DataType::Utf8, false),
    ]));

    let id: StringArray = data
        .iter()
        .map(|x| x.addr.to_string())
        .collect::<Vec<_>>()
        .into();
    let datetime: StringArray = data
        .iter()
        .map(|x| x.datetime.to_rfc3339())
        .collect::<Vec<_>>()
        .into();
    let method: StringArray = data
        .iter()
        .map(|x| x.method.into())
        .collect::<Vec<String>>()
        .into();
    let url: StringArray = data
        .iter()
        .map(|x| x.url.clone())
        .collect::<Vec<_>>()
        .into();
    let protocol: StringArray = data
        .iter()
        .map(|x| x.protocol.into())
        .collect::<Vec<String>>()
        .into();
    let status: UInt16Array = data.iter().map(|x| x.status).collect::<Vec<_>>().into();
    let body_bytes: UInt64Array = data.iter().map(|x| x.body_bytes).collect::<Vec<_>>().into();
    let referrer: StringArray = data
        .iter()
        .map(|x| x.referrer.clone())
        .collect::<Vec<_>>()
        .into();
    let ua: StringArray = data.iter().map(|x| x.ua.clone()).collect::<Vec<_>>().into();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id),
            Arc::new(datetime),
            Arc::new(method),
            Arc::new(url),
            Arc::new(protocol),
            Arc::new(status),
            Arc::new(body_bytes),
            Arc::new(referrer),
            Arc::new(ua),
        ],
    );
    let file = File::create("assets/nginx_log_2.parquet")?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch?)?;
    writer.close()?;
    Ok(())
}
