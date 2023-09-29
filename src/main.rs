use std::{collections::HashMap, convert::Infallible};
use futures::stream::TryStreamExt as _;
use hyper::{HeaderMap, Request};
use serde_json::json;
use structopt::StructOpt as _;

#[derive(Debug, structopt::StructOpt)]
struct Opt {
    #[structopt(
        short,
        long,
        env,
        default_value = "0.0.0.0:8080",
        about = "Bind address"
    )]
    bind: String,
    #[structopt(
        short,
        long,
        env,
        default_value = "http://localhost:9000",
        about = "Target root URL of RIE"
    )]
    target_url: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let Opt { bind, target_url } = Opt::from_args();

    let make_service = hyper::service::make_service_fn(move |_| {
        let target_url = target_url.clone();
        async {
            Ok::<_, std::convert::Infallible>(hyper::service::service_fn(move |r| {
                handle(target_url.clone(), r)
            }))
        }
    });
    let server = (if let Some(listener) = listenfd::ListenFd::from_env().take_tcp_listener(0)? {
        log::info!("Listen {}", listener.local_addr()?);
        hyper::server::Server::from_tcp(listener)?
    } else {
        let addr = bind.parse()?;
        log::info!("Listen {}", addr);
        hyper::server::Server::bind(&addr)
    })
    .serve(make_service)
    .with_graceful_shutdown(async {
        let _ = tokio::signal::ctrl_c().await;
        log::info!("Shutting down...");
        ()
    });
    server.await?;
    Ok(())
}

// https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-develop-integrations-lambda.html
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct ApiGatewayV2LambdaProxyIntegrationV2<'a> {
    http_method: String,
    resource: &'a str,
    path: &'a str,
    headers: Option<std::collections::HashMap<String, String>>,
    query_string_parameters: Option<std::collections::HashMap<String, String>>,
    path_parameters: Option<std::collections::HashMap<String, String>>,
    stage_variables: Option<std::collections::HashMap<String, String>>,
    multi_value_headers: Option<std::collections::HashMap<String, String>>,
    body: Option<String>,
    is_base64_encoded: bool,
    request_context: ApiGatewayV2LambdaProxyIntegrationV2RequestContext<'a>,
}
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct ApiGatewayV2LambdaProxyIntegrationV2RequestContext<'a> {
    http_method: String,
    resource_path: &'a str,
    stage: &'a str,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiGatewayV2LambdaResponseV1 {
    is_base64_encoded: Option<bool>,
    status_code: Option<u16>,
    #[serde(default)]
    headers: Option<std::collections::HashMap<String, String>>,
    body: Option<String>,
}

fn extract_query_string(req: &Request<hyper::Body>) -> Result<HashMap<String, String>, Infallible> {
    // Get the URI from the request
    let uri = req.uri();

    // Extract the query string as a &str
    if let Some(query_str) = uri.query() {
        // Parse the query string into a HashMap
        let params: HashMap<String, String> = url::form_urlencoded::parse(query_str.as_bytes())
            .into_owned()
            .collect();

        Ok(params)
    } else {
        // If there is no query string, return an empty HashMap
        Ok(HashMap::new())
    }
}

fn extract_headers(req: &Request<hyper::Body>) -> Result<HashMap<String, String>, Infallible> {
    // Get a reference to the headers from the request
    let headers: &HeaderMap = req.headers();

    // Create a HashMap to store the headers
    let mut header_map = HashMap::new();

    // Iterate through the headers and store them in the HashMap
    for (name, value) in headers.iter() {
        // Convert the header name and value to strings
        let name_str = name.as_str().to_string();
        let value_str = value.to_str().unwrap_or("").to_string();

        // Insert the header into the HashMap
        header_map.insert(name_str, value_str);
    }

    Ok(header_map)
}

async fn handle(
    target_url: String,
    request: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, anyhow::Error> {
    let query_string_parameters: Option<HashMap<String, String>> =
        extract_query_string(&request).ok(); // Converts the Result into an Option, preserving the HashMap if it's Ok, or None if it's Err.

    let headers:  Option<HashMap<String, String>> = extract_headers(&request)
        .ok();

    let method = request.method().clone();
    let uri = request.uri().clone();
    let path_parameters = None;
    let stage_variables = None;
    let multi_value_headers = None;

    let body = request
        .into_body()
        .map_ok(|b| bytes::BytesMut::from(&b[..]))
        .try_concat()
        .await?;
    let payload = ApiGatewayV2LambdaProxyIntegrationV2 {
        resource: "/",
        http_method: format!("{}", method),
        path: uri.path(),
        headers,
        query_string_parameters,
        stage_variables,
        multi_value_headers,
        path_parameters,
        body: if body.is_empty() {
            None
        } else {
            Some(base64::encode(&body))
        },
        is_base64_encoded: false,
        request_context: ApiGatewayV2LambdaProxyIntegrationV2RequestContext {
            http_method: format!("{}", method),
            resource_path: uri.path(),
            stage: "staging",
        },
    };

    log::info!(
        "Send upstream request: {}",
        serde_json::to_string(&payload)?
    );

    let resp = reqwest::Client::new()
        .post(&format!(
            "{}/2015-03-31/functions/function/invocations",
            target_url
        ))
        .json(&payload)
        .send()
        .await?;

    let lambda_response: ApiGatewayV2LambdaResponseV1 = resp.json().await?;
    log::info!("Received upstream response: {:?}", lambda_response);

    let mut builder = hyper::Response::builder()
        .status(lambda_response.status_code
            .unwrap_or(hyper::StatusCode::INTERNAL_SERVER_ERROR.into()));
    
    if let Some(headers_map) = &lambda_response.headers {
        for (k, v) in headers_map.iter() {
            builder = builder.header(k.as_bytes(), v.as_str())
        }
    }

    Ok(builder.body(hyper::Body::from(lambda_response.body.unwrap_or(String::new())))?)
    
}
