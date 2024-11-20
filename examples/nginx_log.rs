use std::{fs::File, sync::Arc};

//parse
//93.180.71.3 - - [17/May/2015:08:05:32 +0000] "GET /downloads/product_1 HTTP/1.1" 304 0 "-" "Debian APT-HTTP/1.3 (0.8.16~exp12ubuntu10.21)"
use anyhow::Result;

use arrow::{
    array::{RecordBatch, StringArray, UInt16Array, UInt64Array},
    datatypes::{DataType, Field, Schema},
};

use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use regex::Regex;

#[derive(Debug)]
#[allow(unused)]
struct NginxLog {
    ip: String,
    datetime: String,
    method: String,
    url: String,
    protocol: String,
    status: u16,
    body_bytes: u64,
    referrer: String,
    ua: String,
}
#[tokio::main]
async fn main() -> Result<()> {
    read_nginx_log("https://raw.githubusercontent.com/elastic/examples/master/Common%20Data%20Formats/nginx_logs/nginx_logs").await?;
    Ok(())
}

fn parse_nginx_log(s: &str) -> Result<NginxLog> {
    let re = Regex::new(
        r#"(?P<ip>[\d.:a-fA-F]+) - - \[(?P<date>[^\]]+)\] "(?P<method>[A-Z]+) (?P<url>[^\s]+) HTTP/(?P<protocol>[\d.]+)" (?P<status>\d{3}) (?P<length>\d+|-) "(?P<referrer>[^"]*)" "(?P<ua>[^"]*)""#,
    )?;
    let caps = re.captures(s).ok_or(anyhow::anyhow!("not match"))?;
    Ok(NginxLog {
        ip: caps["ip"].to_string(),
        datetime: caps["date"].to_string(),
        method: caps["method"].to_string(),
        url: caps["url"].to_string(),
        protocol: caps["protocol"].to_string(),
        status: caps["status"].parse()?,
        body_bytes: caps["length"].parse()?,
        referrer: caps["referrer"].to_string(),
        ua: caps["ua"].to_string(),
    })
}

async fn read_nginx_log(s: &str) -> Result<()> {
    let url = s;
    let response = reqwest::get(url).await?;
    let mut data = Vec::new();
    for line in response.text().await?.lines() {
        // println!("{}", line);
        let log = parse_nginx_log(line)?;
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

    let id: StringArray = data.iter().map(|x| x.ip.clone()).collect::<Vec<_>>().into();
    let datetime: StringArray = data
        .iter()
        .map(|x| x.datetime.clone())
        .collect::<Vec<_>>()
        .into();
    let method: StringArray = data
        .iter()
        .map(|x| x.method.clone())
        .collect::<Vec<_>>()
        .into();
    let url: StringArray = data
        .iter()
        .map(|x| x.url.clone())
        .collect::<Vec<_>>()
        .into();
    let protocol: StringArray = data
        .iter()
        .map(|x| x.protocol.clone())
        .collect::<Vec<_>>()
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
    let file = File::create("assets/nginx_log.parquet")?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch?)?;
    writer.close()?;
    Ok(())
}
