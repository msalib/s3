use std::{
    collections::{HashMap, HashSet},
    io::Cursor,
    path::PathBuf,
    str::FromStr,
};

use anyhow::{bail, ensure, Error, Result};
use aws_sdk_s3::{primitives::ByteStream, Client};
use bat::{Input, PrettyPrinter};
use futures::{stream::TryStreamExt, StreamExt};
use humansize::{format_size, DECIMAL};
use structopt::StructOpt;
use tokio::io::{AsyncRead, BufReader};
use tokio_util::io::SyncIoBridge;
use url::Url;

#[derive(Debug, Clone)]
struct S3Path {
    inner: Url,
}

impl FromStr for S3Path {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let inner = Url::parse(s)?;
        ensure!(inner.scheme() == "s3", "only S3 URLs are supported");
        ensure!(inner.host_str().is_some(), "bucket must be in URL");
        Ok(S3Path { inner })
    }
}

impl S3Path {
    fn bucket(&self) -> &str {
        // SAFETY: unwrap is fine because we verify there's a host in
        // the ctor.
        self.inner.host_str().unwrap()
    }

    fn key(&self) -> &str {
        self.maybe_key().unwrap()
    }

    fn maybe_key(&self) -> Option<&str> {
        self.inner.path().strip_prefix('/')
    }
}

#[derive(Debug, Clone)]
enum LocalOrRemote {
    Local(PathBuf),
    Remote(S3Path),
}

impl FromStr for LocalOrRemote {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match S3Path::from_str(s) {
            Ok(remote) => LocalOrRemote::Remote(remote),
            Err(_) => LocalOrRemote::Local(PathBuf::from(s)),
        })
    }
}

#[derive(StructOpt, Clone)]
#[structopt(about = "Command Line Interface for S3")]
enum Commands {
    /// cat the contents of an S3Path
    Cat {
        #[structopt(short, long)]
        pretty_print: bool,

        paths: Vec<S3Path>,
    },

    /// list the contents of an S3Path
    Ls {
        #[structopt(short, long)]
        human: bool,

        #[structopt(short, long)]
        recursive: bool,

        path: S3Path,
    },

    /// delete some S3Paths
    Rm { paths: Vec<S3Path> },

    /// write an empty file
    Touch { destination: S3Path },

    /// Copy a path
    Cp {
        /// either a local path or an s3:// URL
        source: LocalOrRemote,

        /// either a local path or an s3:// URL
        destination: LocalOrRemote,
    },
}

enum CatContents<T: AsyncRead> {
    Pretty(Cursor<String>),
    Reader(SyncIoBridge<BufReader<T>>),
}

async fn region_name(client: &Client, bucket: &str) -> Result<String> {
    Ok(client
        .get_bucket_location()
        .bucket(bucket)
        .send()
        .await?
        .location_constraint()
        .map(|region| region.as_str())
        .unwrap_or("us-east-1")
        .to_owned())
}

async fn region_client(client: &Client, path: &S3Path) -> Result<Option<Client>> {
    let region_name = region_name(client, path.bucket()).await?;
    Ok(if region_name != "" {
        let region = aws_types::region::Region::new(region_name);
        let config = aws_config::from_env().region(region).load().await;
        Some(Client::new(&config))
    } else {
        None
    })
}

const PARALLELISM: usize = 16;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Commands::from_args();

    let config = aws_config::load_from_env().await;
    let mut client = Client::new(&config);

    match cli {
        Commands::Cat {
            pretty_print,
            paths,
        } => {
            for path in paths.into_iter() {
                if let Some(region_client) = region_client(&client, &path).await? {
                    client = region_client;
                }

                let key = path.key().to_owned();
                let resp = client
                    .get_object()
                    .bucket(path.bucket())
                    .key(&key)
                    .send()
                    .await?;
                let body = resp.body;
                let contents = if pretty_print {
                    let text = body.collect().await?.into_bytes();
                    let json: serde_json::Value = serde_json::from_slice(&text)?;
                    let pretty = serde_json::to_string_pretty(&json)?;
                    CatContents::Pretty(Cursor::new(pretty))
                } else {
                    let reader = BufReader::new(body.into_async_read());
                    CatContents::Reader(SyncIoBridge::new(reader))
                };

                let handle = tokio::task::spawn_blocking(move || {
                    let input = match contents {
                        CatContents::Pretty(pretty) => Input::from_reader(pretty),
                        CatContents::Reader(reader) => Input::from_reader(reader),
                    };
                    let mut printer = PrettyPrinter::new();
                    if pretty_print {
                        printer.language("json");
                    }
                    printer
                        .input(input.name(key))
                        .paging_mode(bat::PagingMode::QuitIfOneScreen)
                        .print()
                });
                let _x = handle.await??;
                println!();
            }
        }

        Commands::Ls {
            human,
            recursive,
            path,
        } => {
            if let Some(region_client) = region_client(&client, &path).await? {
                client = region_client;
            }

            let prefix = path.maybe_key().map(String::from);
            let delimiter = if recursive {
                None
            } else {
                Some(String::from("/"))
            };
            let mut stream = client
                .list_objects_v2()
                .bucket(path.bucket())
                .set_prefix(prefix.clone())
                .set_delimiter(delimiter)
                .into_paginator()
                .send();
            while let Some(item) = stream.try_next().await? {
                if let Some(prefixes) = item.common_prefixes() {
                    for key in prefixes {
                        if let Some(key) = key.prefix() {
                            let key = match &prefix {
                                Some(prefix) => key.strip_prefix(prefix).unwrap(),
                                None => key,
                            };
                            println!("{key}");
                        }
                    }
                }
                if let Some(objects) = item.contents() {
                    for obj in objects {
                        let dt = obj
                            .last_modified()
                            .unwrap()
                            .fmt(aws_smithy_types::date_time::Format::DateTime)?
                            .replace('T', " ");
                        let size = if human {
                            format_size(obj.size() as usize, DECIMAL)
                        } else {
                            obj.size().to_string()
                        };
                        let key = obj.key().unwrap();
                        let key = match &prefix {
                            Some(prefix) => key.strip_prefix(prefix).unwrap(),
                            None => key,
                        };
                        println!("{dt} {size:>20}    {key}");
                    }
                }
            }
        }

        Commands::Rm { paths } => {
            let buckets: HashSet<&str> = paths.iter().map(|path| path.bucket()).collect();
            let regions: HashMap<String, String> = futures::stream::iter(buckets.iter())
                .map(|x| bucket_region_pair(&client, x))
                .boxed()
                .buffered(PARALLELISM)
                .try_collect()
                .await?;

            let mut region_paths: HashMap<String, HashMap<String, Vec<S3Path>>> =
                HashMap::with_capacity(buckets.len());
            for path in paths.into_iter() {
                let region = regions[path.bucket()].clone();
                region_paths
                    .entry(region)
                    .or_default()
                    .entry(path.bucket().to_owned())
                    .or_default()
                    .push(path);
            }

            futures::stream::iter(region_paths.into_values().map(Ok))
                .try_for_each_concurrent(PARALLELISM, |bucket_paths| {
                    rm_one_region(client.clone(), bucket_paths)
                })
                .await?;
        }

        Commands::Touch { destination } => {
            if let Some(region_client) = region_client(&client, &destination).await? {
                client = region_client;
            }
            client
                .put_object()
                .bucket(destination.bucket())
                .key(destination.key())
                .body(ByteStream::from(vec![]))
                .send()
                .await?;
        }

        Commands::Cp {
            source,
            destination,
        } => {
            use LocalOrRemote::*;
            match (source, destination) {
                (Local(source), Local(destination)) => {
                    // just a local copy
                    std::fs::copy(source, destination)?;
                }

                (Local(source), Remote(destination)) => {
                    // upload
                    if let Some(region_client) = region_client(&client, &destination).await? {
                        client = region_client;
                    }
                    client
                        .put_object()
                        .bucket(destination.bucket())
                        .key(destination.key())
                        .body(ByteStream::from_path(source).await?)
                        .send()
                        .await?;
                }

                (Remote(source), Local(destination)) => {
                    // download
                    if let Some(region_client) = region_client(&client, &source).await? {
                        client = region_client;
                    }
                    let response = client
                        .get_object()
                        .bucket(source.bucket())
                        .key(source.key())
                        .send()
                        .await?;
                    let mut input = response.body.into_async_read();
                    let mut output = tokio::fs::File::create(destination).await?;
                    tokio::io::copy(&mut input, &mut output).await?;
                }

                (Remote(source), Remote(destination)) => {
                    // direct AWS copy
                    if let Some(region_client) = region_client(&client, &destination).await? {
                        client = region_client;
                    }

                    let _x = client
                        .copy_object()
                        .bucket(destination.bucket())
                        .key(destination.key())
                        .copy_source(format!("{}/{}", source.bucket(), source.key()))
                        .send()
                        .await?;
                    // FIXME: verify that copy worked
                }
            }
        }
    }
    Ok(())
}

async fn bucket_region_pair(client: &Client, bucket: &str) -> Result<(String, String)> {
    Ok((bucket.to_owned(), region_name(client, bucket).await?))
}

async fn rm_one_region(mut client: Client, paths: HashMap<String, Vec<S3Path>>) -> Result<()> {
    if let Some(region_client) = region_client(&client, &paths.values().next().unwrap()[0]).await? {
        client = region_client;
    }

    for (bucket, paths) in paths.into_iter() {
        let oids: Vec<_> = paths
            .into_iter()
            .map(|path| {
                aws_sdk_s3::types::ObjectIdentifier::builder()
                    .key(path.key())
                    .build()
            })
            .collect();
        let delete = aws_sdk_s3::types::Delete::builder()
            .set_objects(Some(oids))
            .build();
        let response = client
            .delete_objects()
            .bucket(bucket.clone())
            .delete(delete)
            .send()
            .await?;
        if let Some(errors) = response.errors() {
            if !errors.is_empty() {
                for error in errors {
                    let key = error.key().unwrap_or("");
                    let message = error.message().unwrap_or("");
                    println!("delete failed: s3://{bucket}/{key}: {message}");
                }
                bail!("delete failed");
            }
        }
    }

    Ok(())
}
