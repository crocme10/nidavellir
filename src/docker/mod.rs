use bollard::container::{Config, CreateContainerOptions, NetworkingConfig, StartContainerOptions};
use bollard::image::CreateImageOptions;
use bollard::network::{CreateNetworkOptions, ListNetworksOptions};
use bollard::service::{BuildInfo, EndpointSettings, HostConfig, Ipam, PortBinding};
use bollard::Docker;
use futures::future;
use futures::stream::{self, StreamExt, TryStreamExt};
use slog::{error, trace, Logger};
use snafu::ResultExt;
use std::net::TcpListener;
use tokio::fs::File;
use tokio::io::{self, AsyncReadExt};
// use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::default::Default;

use crate::error;
use crate::settings::Settings;

#[derive(Debug, Serialize, Deserialize)]
pub struct DockerConfig {
    image: String,
    tag: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NetworkConfig {
    addr_base: String,
    addr_suffix: u16,
    id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ServiceConfig {
    service: String,
    docker: DockerConfig,
    network: NetworkConfig,
    envs: Option<Vec<String>>,
    ports: Option<HashMap<String, Option<String>>>, // Internal, External
}

/// Create a twerg, and return the twerg's frontend port
pub async fn get_config(
    settings: &Settings,
    logger: &Logger,
) -> Result<Vec<ServiceConfig>, error::Error> {
    let config = settings.twerg.config.clone();
    let mut file = File::open(&config).await.context(error::TokioIOError {
        msg: format!("Could not open twerg configuration at {}", config),
    })?;
    let mut config = String::new();
    file.read_to_string(&mut config)
        .await
        .context(error::TokioIOError {
            msg: format!("Could not read twerg configuration at {}", config),
        })?;
    serde_json::from_str(&config).context(error::JSONError {
        msg: format!("Could not deserialize {}", config),
    })
}

pub async fn create_twerg(
    name: &str,
    settings: &Settings,
    logger: &Logger,
) -> Result<u16, error::Error> {
    let config = get_config(settings, &logger).await?;

    let docker = Docker::connect_with_unix_defaults().context(error::DockerError {
        msg: String::from("Could not connect to docker"),
    })?;

    trace!(logger, "Connected to docker");

    let network_base = get_network_base(&docker).await?;

    let port_base = settings.twerg.base;

    let port = get_available_port(port_base).await?;

    trace!(logger, "About to launch {} on port {}", name, port);

    let network_id = create_network(&docker, name, network_base.as_str(), &logger).await?;

    stream::iter(config.into_iter().map(|c| Ok(c)))
        .try_for_each(|mut config| {
            config.network.id = Some(network_id.clone());
            // For nginx, we bind the external port to port 80. So we add a "80:{external port}"
            // binding.
            if config.service == "nginx" {
                // FIXME Hardcoded internal port number
                let mut ports = HashMap::new();
                let external_port = format!("{}", port);
                ports.insert(String::from("80"), Some(external_port));
                config.ports = Some(ports);
            }
            launch_service(&docker, &name, config, &logger)
        })
        .await?;

    Ok(port)
}

/// Returns the first available port available after the base.
/// We iterate over the range base..base+99, and try to create a TcpListener.
/// If this fails, we try the next port.
///
/// FIXME: The loop should not use std::net::TcpListener which is blocking.
/// But I have an error I can't find a quick solution to...
pub async fn get_available_port(base: u16) -> Result<u16, error::Error> {
    let end = base + 99;
    // let stream = stream::iter((base..end).into_iter()).take_while(|port: &u16| async move {
    //     let endpoint = format!("127.0.0.1:{}", port);
    //     let listener = TcpListener::bind(endpoint).await;
    //     listener.is_err()
    // });

    let stream = stream::iter((base..end).into_iter()).take_while(|port| {
        let endpoint = format!("127.0.0.1:{}", port);
        let listener = TcpListener::bind(endpoint);
        future::ready(listener.is_err())
    });

    let mut failed_ports = stream.collect::<Vec<u16>>().await;
    // FIXME Need to properly handle the case when no port is available in the given range.
    // or when the base port is available.
    let available_port = failed_ports.pop().expect("some port") + 1;
    Ok(available_port)
}

// This function tries to identify the first available network CIDR.
// We list all docker networks, and focus on their IPAM configuration.
// We extract all the subnets
// We extract from those subnets the second part of the address (172.x.0.0/16 => x)
// We sort these, and try to find the first one available, that is if you
// have existing subnets 172.16.0.0/16, 172.17.0.0/16, 172.17.0.0/16, it will return 172.18
pub async fn get_network_base(docker: &Docker) -> Result<String, error::Error> {
    let config = ListNetworksOptions::<String> {
        ..Default::default()
    };
    let networks = docker
        .list_networks(Some(config))
        .await
        .context(error::DockerError {
            msg: String::from("Could not list networks"),
        })?;
    let subnets = networks
        .into_iter()
        .filter_map(|network| network.ipam)
        .filter_map(|ipam| ipam.config)
        .flatten()
        .filter_map(|ipam_conf| ipam_conf.get("Subnet").map(std::clone::Clone::clone))
        .collect::<Vec<String>>();

    // FIXME The following makes huge assumptions!!!
    // It assumes all the subnet are in the form 172.xxx.0.0/16
    let mut ids = subnets
        .iter()
        .filter_map(|subnet| {
            subnet
                .trim_start_matches("172.")
                .trim_end_matches(".0.0/16")
                .parse::<u16>()
                .ok()
        })
        .collect::<Vec<u16>>();

    ids.sort();
    let first = ids[0];
    let res = ids
        .iter()
        .zip(0u16..)
        .find(|(val, idx)| (first + idx) < **val);
    let res = match res {
        Some((_val, idx)) => first + idx,
        None => first + u16::try_from(ids.len()).expect("list len"),
    };
    Ok(format!("172.{}", res))
}

pub async fn launch_service(
    docker: &Docker,
    env_name: &str,
    config: ServiceConfig,
    logger: &Logger,
) -> Result<(), error::Error> {
    let image_name = format_image(&config.docker.image, &config.docker.tag);
    let container_name = format_container(&config.service, env_name);
    create_image(&docker, &image_name, &logger).await?;
    create_container(&docker, &env_name, &config, &logger).await?;
    start_container(&docker, &container_name, &logger).await?;
    Ok(())
}

pub async fn create_container(
    docker: &Docker,
    env_name: &str,
    config: &ServiceConfig,
    logger: &Logger,
) -> Result<(), error::Error> {
    let network_name = format_network(env_name);
    let image_name = format_image(&config.docker.image, &config.docker.tag);
    let container_name = format_container(&config.service, env_name);
    let options = CreateContainerOptions {
        name: String::from(container_name),
    };

    let network_id = String::from(&config.network.id.clone().unwrap());

    let mut endpoints = HashMap::new();
    endpoints.insert(
        String::from(&network_name),
        EndpointSettings {
            aliases: Some(vec![String::from(&config.service)]),
            network_id: Some(String::from(&network_id)),
            gateway: Some(format!("{}.0.1", &config.network.addr_base)),
            ip_address: Some(format!(
                "{}.0.{}",
                &config.network.addr_base, &config.network.addr_suffix
            )),
            ip_prefix_len: Some(8), // 255.255.255.0 (we can have 254 services on that network)
            ..Default::default()
        },
    );

    let port_bindings = config.ports.clone().map(|ports| {
        let mut port_bindings = HashMap::new();
        for (internal, external) in ports.iter() {
            // The key should contain two ports separated by a colon:
            // [port on host]:[port on container]
            if let Some(external) = external {
                port_bindings.insert(
                    format!("{}/tcp", internal),
                    Some(vec![PortBinding {
                        host_ip: Some(String::from("0.0.0.0")),
                        host_port: Some(external.clone()),
                    }]),
                );
            }
        }
        port_bindings
    });

    let host_config = HostConfig {
        port_bindings,
        network_mode: Some(String::from(network_name)),
        ..Default::default()
    };

    let exposed_ports = config.ports.clone().map(|ports| {
        let mut exposed_ports = HashMap::new();
        for (internal, _) in ports.iter() {
            let v: HashMap<(), ()> = HashMap::new();
            exposed_ports.insert(format!("{}/tcp", internal), v);
        }
        exposed_ports
    });
    let config = Config {
        image: Some(String::from(image_name)),
        networking_config: Some(NetworkingConfig {
            endpoints_config: endpoints,
        }),
        host_config: Some(host_config),
        env: config.envs.clone(),
        exposed_ports,
        ..Default::default()
    };
    let result = &docker
        .create_container(Some(options), config)
        .await
        .context(error::DockerError {
            msg: String::from("Could not create container"),
        })?;

    assert_ne!(result.id.len(), 0);
    Ok(())
}

pub async fn start_container(
    docker: &Docker,
    container_name: &str,
    logger: &Logger,
) -> Result<(), error::Error> {
    &docker
        .start_container(container_name, None::<StartContainerOptions<String>>)
        .await
        .context(error::DockerError {
            msg: String::from("Could not start container"),
        })?;

    Ok(())
}

pub async fn create_image(
    docker: &Docker,
    image_name: &str,
    logger: &Logger,
) -> Result<(), error::Error> {
    trace!(logger, "Creating docker image {}", image_name);
    let options = Some(CreateImageOptions {
        from_image: &image_name[..],
        ..Default::default()
    });

    let result: Vec<BuildInfo> = docker
        .create_image(options, None, None)
        .try_collect()
        .await
        .context(error::DockerError {
            msg: String::from("Could not create image"),
        })?;

    match result.len() {
        0 => {
            error!(logger, "Not expecting image creation return empty info");
            // FIXME That's not Ok!
            Ok(())
        }
        1 => {
            // FIXME There are two fields that probably need to be defensively reported, error, and
            // error_detail
            trace!(
                logger,
                "Successfully created docker image {} => id {}",
                image_name,
                &result[0].id.clone().unwrap_or_else(|| String::from("NA"))
            );
            Ok(())
        }
        _ => {
            // FIXME Not expecting more than one
            Ok(())
        }
    }
}

/// Creates a network, and returns its id
pub async fn create_network(
    docker: &Docker,
    env_name: &str,
    ip_range_base: &str,
    logger: &Logger,
) -> Result<String, error::Error> {
    let mut ipam_config = HashMap::new();
    ipam_config.insert(String::from("Subnet"), format!("{}.0.0/16", ip_range_base));
    ipam_config.insert(String::from("Gateway"), format!("{}.0.1", ip_range_base));
    let ipam = Ipam {
        driver: Some(String::from("default")),
        config: Some(vec![ipam_config]),
        options: None,
    };
    let mut labels = HashMap::new();
    labels.insert(String::from("nidavellir.network"), String::from("default"));
    labels.insert(String::from("nidavellir.version"), String::from("0.4.2"));
    labels.insert(
        String::from("nidavellir.environment"),
        String::from(env_name),
    );

    let options = CreateNetworkOptions {
        name: format_network(env_name),
        driver: String::from("bridge"),
        ipam,
        labels,
        ..Default::default()
    };

    let result = docker
        .create_network(options)
        .await
        .context(error::DockerError {
            msg: String::from("Could not create network"),
        })?;

    match result.id {
        Some(id) => {
            trace!(logger, "Network created: {}", id);
            Ok(id)
        }
        None => {
            error!(logger, "Could not get network id");
            Err(error::Error::MiscError {
                msg: String::from("Could not get network id"),
            })
        }
    }
}

pub fn registry_http_addr() -> String {
    format!(
        "{}",
        ::std::env::var("REGISTRY_HTTP_ADDR").unwrap_or_else(|_| "localhost:5000".to_string())
    )
}

pub fn format_image(name: &str, tag: &str) -> String {
    format!("{}/{}:{}", registry_http_addr(), name, tag)
}

pub fn format_container(name: &str, env: &str) -> String {
    format!("{}_{}", env, name)
}

pub fn format_network(env: &str) -> String {
    format!("{}_default", env)
}
