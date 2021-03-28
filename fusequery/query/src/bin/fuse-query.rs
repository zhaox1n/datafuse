// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use fuse_query::api::{HttpService, RpcService};
use fuse_query::clusters::Cluster;
use fuse_query::configs::Config;
use fuse_query::metrics::MetricService;
use fuse_query::servers::MysqlHandler;
use fuse_query::sessions::Session;
use log::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conf = Config::create_from_args();
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or(conf.log_level.to_lowercase().as_str()),
    )
    .init();

    info!("{:?}", conf.clone());
    info!("FuseQuery v-{}", conf.version);

    let cluster = Cluster::create(conf.clone());
    let session_manager = Session::create();

    // MySQL handler.
    {
        let handler = MysqlHandler::create(conf.clone(), cluster.clone(), session_manager.clone());
        tokio::spawn(async move { handler.start() });

        info!(
            "MySQL handler listening on {}:{}, Usage: mysql -h{} -P{}",
            conf.mysql_handler_host,
            conf.mysql_handler_port,
            conf.mysql_handler_host,
            conf.mysql_handler_port
        );
    }

    // Metric API service.
    {
        let srv = MetricService::create(conf.clone());
        tokio::spawn(async move {
            srv.make_server().unwrap();
        });
        info!("Metric API server listening on {}", conf.metric_api_address);
    }

    // HTTP API service.
    {
        let srv = HttpService::create(conf.clone(), cluster.clone());
        tokio::spawn(async move {
            srv.make_server().await.unwrap();
        });
        info!("HTTP API server listening on {}", conf.metric_api_address);
    }

    // RPC API service.
    {
        let srv = RpcService::create(conf.clone(), cluster.clone(), session_manager.clone());
        info!("RPC API server listening on {}", conf.rpc_api_address);
        srv.make_server().await.unwrap();
    }

    Ok(())
}
