use hyper::client::Builder;
use futures::stream::TryStreamExt as _;
use structopt::StructOpt as _;

#[derive(Debug, structopt::StructOpt)]
struct Opt {
    #[structopt(short, long, env, default_value = "127.0.0.1:8080", about = "Bind address")]
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
            Ok::<_, std::convert::Infallible>(
                hyper::service::service_fn(move |r| { handle(target_url.clone(), r) })
            )
        }
    });
    let server = (
        if let Some(listener) = listenfd::ListenFd::from_env().take_tcp_listener(0)? {
            log::info!("Listen {}", listener.local_addr()?);
            hyper::server::Server::from_tcp(listener)?
        } else {
            let addr = bind.parse()?;
            log::info!("Listen {}", addr);
            hyper::server::Server::bind(&addr)
        }
    )
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
    headers: std::collections::HashMap<String, String>,
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
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct ApiGatewayV2LambdaProxyIntegrationV2RequestContextHttp<'a> {
    http_method: String,
    resource_path: &'a str,
    source_ip: &'a str,
}
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiGatewayV2LambdaResponseV1 {
    is_base64_encoded: Option<bool>,
    status_code: u16,
    #[serde(default)]
    headers: std::collections::HashMap<String, String>,
    body: String,
}

async fn handle(
    target_url: String,
    request: hyper::Request<hyper::Body>
) -> Result<hyper::Response<hyper::Body>, anyhow::Error> {
    let query_string_parameters = if request.uri().query().is_some() {
        let u = url::Url::parse(&format!("{}", request.uri()))?;
        let mut params = std::collections::HashMap::new();
        for (k, v) in u.query_pairs() {
            params.insert(k.into_owned(), v.into_owned());
        }
        Some(params)
    } else {
        None
    };
    let method = request.method().clone();
    let uri = request.uri().clone();
    let mut headers = std::collections::HashMap::new();
    let mut path_parameters = None;
    let mut stage_variables = None;
    let mut multi_value_headers = None;

    let body = request
        .into_body()
        .map_ok(|b| bytes::BytesMut::from(&b[..]))
        .try_concat().await?;
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
            stage: "local",
        },
    };

    log::info!("Send upstream request: {}", serde_json::to_string(&payload)?);
    let resp = reqwest::Client
        ::new()
        .post(&format!("{}/2015-03-31/functions/function/invocations", target_url))
        .json(&payload)
        .send().await?;
    let lambda_response: ApiGatewayV2LambdaResponseV1 = resp.json().await.unwrap();
    log::info!("Received upstream response: {:?}", lambda_response);

    let mut builder = hyper::Response::builder().status(lambda_response.status_code);
    for (k, v) in lambda_response.headers {
        builder = builder.header(k.as_bytes(), v);
    }

    Ok(builder.body(hyper::Body::from(lambda_response.body))?)
}
