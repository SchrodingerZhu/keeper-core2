#![feature(map_first_last)]

use structopt::*;
use sqlx::prelude::SqliteQueryAs;
use sqlx::{SqlitePool, Sqlite};
use xactor::{Actor, Addr, Handler, Context};
use futures::io::WriteHalf;
use futures::{AsyncWriteExt, AsyncReadExt, StreamExt};
use hashbrown::HashMap;
use std::collections::BTreeSet;
use futures_codec::{LinesCodec, FramedWrite, FramedRead};
use ws_stream_tungstenite::WsStream;

type WSStream = WsStream<async_std::net::TcpStream>;
type WriteStream = FramedWrite<WriteHalf<WSStream>, LinesCodec>;

#[derive(StructOpt, Debug)]
struct Opt {
    #[structopt(short, long, help = "Database URL")]
    database: String,
    #[structopt(short, long, help = "Listen Address", default_value = "0.0.0.0")]
    listen_address: std::net::IpAddr,
    #[structopt(short, long, help = "Listen Port", default_value = "8080")]
    port: u16,
    #[structopt(subcommand)]
    mode: Mode
}

#[derive(Debug, StructOpt)]
enum Mode {
    #[structopt(about="Start the server")]
    Server {
        #[structopt(short, long, help="Authorization Token")]
        token: String
    },
    #[structopt(about="Draw lucky dogs")]
    Draw {
        #[structopt(short, long, help = "First class")]
        first_class: usize,
        #[structopt(short, long, help = "Second class")]
        second_class: usize,
        #[structopt(short, long, help = "Third class")]
        third_class: usize
    }
}


struct HouseKeeper {
    registry: hashbrown::HashMap<u64, xactor::Addr<StreamActor>>,
    current_key: std::time::SystemTime,
    valid: std::collections::BTreeSet<std::time::SystemTime>,
    pool: SqlitePool,
    encryptor: botan::Encryptor,
    decryptor: botan::Decryptor,
    finished: bool,
}

struct StreamActor {
    stream: WriteStream,
    keeper: xactor::Addr<HouseKeeper>,
}

#[xactor::message(result = "()")]
#[derive(Clone)]
enum HouseKeeperMsg {
    Clean,
    Next,
    Register(Addr<StreamActor>),
    UnRegister(Addr<StreamActor>),
    Submit {
        source: Addr<StreamActor>,
        student_id: String,
        key: String,
    },
    Draw {
        first_class: usize,
        second_class: usize,
        third_class: usize,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag="type", content="content")]
enum ClientMessage {
    Submit {
        student_id: String,
        key: String,
    },
    Register(String)
}


#[xactor::message(result = "()")]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", content = "content")]
enum StreamActorMsg {
    UpdateQR(String),
    UpdatePoolSize(usize),
    SubmissionResult {
        success: bool,
        msg: String,
    },
    DrawResult(Vec<String>, Vec<String>, Vec<String>),
    Error(String),
}

impl HouseKeeper {
    fn generate_key(&self) -> String {
        let buffer = simd_json::to_vec(&self.current_key).unwrap();
        let random = botan::RandomNumberGenerator::new_system().unwrap();
        let encrypted = self.encryptor.encrypt(&buffer, &random).unwrap();
        radix64::URL_SAFE.encode(&encrypted)
    }
    async fn get_results(&self) -> anyhow::Result<StreamActorMsg> {
        let first_class: Vec<String> = sqlx::query_as("SELECT student_id FROM record WHERE grade = 1")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|x: (String, )| x.0)
            .collect();
        let second_class: Vec<String> = sqlx::query_as("SELECT student_id FROM record WHERE grade = 2")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|x: (String, )| x.0)
            .collect();
        let third_class: Vec<String> = sqlx::query_as("SELECT student_id FROM record WHERE grade = 3")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|x: (String, )| x.0)
            .collect();
        Ok(StreamActorMsg::DrawResult(
            first_class,
            second_class,
            third_class,
        ))
    }
    // async fn send_results(&self) -> anyhow::Result<()> {
    //    let result = self.get_results().await?;
    //    for (_, actor) in self.registry.iter() {
    //        if let Err(e) = actor.send(result.clone()) {
    //            log::error!("failed to communicate with actor {}: {}", actor.actor_id(), e);
    //       }
    //   }
    //    Ok(())
    // }
}

#[xactor::message(result = "bool")]
struct GetFinished;

#[async_trait::async_trait]
impl Handler<GetFinished> for HouseKeeper {
    async fn handle(&mut self, _ctx: &mut Context<Self>, _msg: GetFinished) -> bool {
        return self.finished;
    }
}

static UPDATE_SQL: &str =
    "UPDATE record SET grade = ? WHERE id IN (SELECT id FROM record WHERE grade = 0 ORDER BY RANDOM() LIMIT ?)";

#[async_trait::async_trait]
impl Handler<HouseKeeperMsg> for HouseKeeper {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: HouseKeeperMsg) {
        match msg {
            HouseKeeperMsg::Clean => {
                let now = std::time::SystemTime::now();
                while let Some(x) = self.valid.first() {
                    if now.duration_since(x.clone()).unwrap().as_secs() >= 90 {
                        self.valid.pop_first();
                    } else {
                        break;
                    }
                }
            }
            HouseKeeperMsg::Next => {
                if !self.finished {
                    self.current_key = std::time::SystemTime::now();
                    self.valid.insert(self.current_key.clone());
                    let msg = self.generate_key();
                    for (_, actor) in self.registry.iter() {
                        if let Err(e) = actor.send(StreamActorMsg::UpdateQR(msg.clone())) {
                            log::error!("failed to communicate with actor {}: {}", actor.actor_id(), e)
                        }
                    }
                }
            }
            HouseKeeperMsg::Register(addr) => {
                self.registry.insert(addr.actor_id(), addr.clone());
                if !self.finished {
                    if let Err(e) = addr.send(StreamActorMsg::UpdateQR(self.generate_key())) {
                        log::error!("failed to communicate with actor {}: {}", addr.actor_id(), e)
                    }
                    if let Ok((size, )) = sqlx::query_as::<Sqlite, (i32, )>("SELECT COUNT(id) FROM record")
                        .fetch_one(&self.pool).await {
                            if let Err(e) = addr.send(StreamActorMsg::UpdatePoolSize(size as usize)) {
                                log::error!("{}", e)
                            }
                        }
                } else {
                    if let Err(e) = self.get_results().await.and_then(|x| addr.send(x)) {
                        log::error!("failed to communicate with actor {}: {}", addr.actor_id(), e)
                    }
                }
            }
            HouseKeeperMsg::UnRegister(addr) => {
                self.registry.remove(&addr.actor_id());
            }
            HouseKeeperMsg::Submit { source, student_id, key } => {
                let validation = radix64::URL_SAFE.decode(&key)
                    .map_err(Into::<anyhow::Error>::into)
                    .and_then(|x| {
                        self.decryptor.decrypt(x.as_slice())
                            .map_err(|e| anyhow::anyhow!("{:?}", e))
                    })
                    .and_then(|mut x| {
                        simd_json::from_slice::<std::time::SystemTime>(&mut x)
                            .map_err(Into::<anyhow::Error>::into)
                    });
                let msg = if validation.is_err() {
                    StreamActorMsg::SubmissionResult {
                        success: false,
                        msg: validation.unwrap_err().to_string(),
                    }
                } else {
                    let validation = validation.unwrap();
                    if self.valid.contains(&validation) {
                        match sqlx::query("INSERT INTO record (student_id) VALUES (?)")
                            .bind(student_id)
                            .execute(&self.pool)
                            .await {
                            Ok(_) => {
                                if let Ok((size, )) = sqlx::query_as::<Sqlite, (i32, )>("SELECT COUNT(id) FROM record")
                                    .fetch_one(&self.pool).await {
                                    for (_, j) in self.registry.iter() {
                                        if let Err(e) = j.send(StreamActorMsg::UpdatePoolSize(size as usize)) {
                                            log::error!("{}", e);
                                        };
                                    }
                                }
                                StreamActorMsg::SubmissionResult {
                                    success: true,
                                    msg: String::from("recorded"),
                                }
                            }
                            Err(e) => StreamActorMsg::SubmissionResult {
                                success: false,
                                msg: e.to_string(),
                            }
                        }
                    } else {
                        StreamActorMsg::SubmissionResult {
                            success: false,
                            msg: String::from("invalid key"),
                        }
                    }
                };
                if let Err(e) = source.send(msg) {
                    return log::error!("failed to reply actor {}: {}", source.actor_id(), e);
                }
            }
            HouseKeeperMsg::Draw { first_class, second_class, third_class } => {
                if !self.finished {
                   sqlx::query(UPDATE_SQL)
                        .bind(1)
                        .bind(first_class as i32)
                        .execute(&self.pool)
                        .await.unwrap();
                    sqlx::query(UPDATE_SQL)
                        .bind(2)
                        .bind(second_class as i32)
                        .execute(&self.pool)
                        .await.unwrap();
                    sqlx::query(UPDATE_SQL)
                        .bind(3)
                        .bind(third_class as i32)
                        .execute(&self.pool)
                        .await.unwrap();
                    self.finished = true;
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<StreamActorMsg> for StreamActor {
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: StreamActorMsg) {
        log::debug!("sending message {:?}", msg);
        let json_buffer = simd_json::to_vec(&msg).unwrap();
        if let Err(e) = self.stream.write(&json_buffer).await {
            log::error!("actor {} failed to write: {}", ctx.actor_id(), e);
            if let Err(e) = self.keeper.send(HouseKeeperMsg::UnRegister(ctx.address())) {
                log::error!("failed to write unregister actor {}: {}", ctx.actor_id(), e);
            }
            ctx.stop(None);
        } else if let Err(e) = self.stream.flush().await {
            log::error!("actor {} failed to flush: {}", ctx.actor_id(), e);
            if let Err(e) = self.keeper.send(HouseKeeperMsg::UnRegister(ctx.address())) {
                log::error!("failed to write unregister actor {}: {}", ctx.actor_id(), e);
            }
            ctx.stop(None);
        }
    }
}


unsafe impl Send for HouseKeeper {}

unsafe impl Sync for HouseKeeper {}

#[async_trait::async_trait]
impl Actor for HouseKeeper {
    async fn started(&mut self, ctx: &mut Context<Self>) -> xactor::Result<()> {
        ctx.send_interval(HouseKeeperMsg::Clean, std::time::Duration::from_secs(15));
        ctx.send_interval(HouseKeeperMsg::Next, std::time::Duration::from_secs(30));
        Ok(())
    }
}

impl Actor for StreamActor {}


impl StreamActor {
    async fn start_new(keeper: Addr<HouseKeeper>, stream: WriteStream, register: bool) -> anyhow::Result<Addr<Self>> {
        let actor = StreamActor {
            stream,
            keeper: keeper.clone(),
        };
        let addr = actor.start().await?;
        if register {
            keeper.send(HouseKeeperMsg::Register(addr.clone()))?;
        }
        Ok(addr)
    }
}

#[async_std::main]
async fn main() -> anyhow::Result<()> {
    pretty_env_logger::init();
    let opt = Opt::from_args();
    let pool = sqlx::sqlite::SqlitePool::new(&opt.database)
        .await?;
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS 'record' (
            id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL,
            time DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
            student_id TEXT NOT NULL UNIQUE,
            grade INTEGER NOT NULL DEFAULT 0
        )"#).execute(&pool).await?;
    log::info!("SQL started");
    let (finished, ) = sqlx::query_as("SELECT EXISTS(SELECT id FROM record WHERE grade != 0)")
        .fetch_one(&pool)
        .await?;
    if finished {
        log::info!("already finished");
    }
    let random = botan::RandomNumberGenerator::new_system().unwrap();
    let key = botan::Privkey::create("RSA", "1024", &random).unwrap();
    let pkey = key.pubkey().unwrap();
    log::debug!("RSA initialized with {}", key.pem_encode().unwrap());
    let decryptor = botan::Decryptor::new(&key, "OAEP(MD5)").unwrap();
    let encryptor = botan::Encryptor::new(&pkey, "OAEP(MD5)").unwrap();
    let current_key = std::time::SystemTime::now();
    let mut valid = BTreeSet::new();
    valid.insert(current_key.clone());
    let keeper: Addr<HouseKeeper> = HouseKeeper {
        registry: HashMap::new(),
        current_key,
        valid,
        pool,
        encryptor,
        decryptor,
        finished,
    }.start().await?;
    match opt.mode {
        Mode::Server { token } => {
            let listener = async_std::net::TcpListener::bind(format!("{}:{}", opt.listen_address, opt.port)).await?;
            while let Some(stream) = listener.incoming().next().await {
                log::debug!("incoming stream");
                match stream {
                    Err(e) => log::error!("failed to accept stream: {}", e),
                    Ok(stream) => {
                        let keeper = keeper.clone();
                        let token = token.clone();
                        async_std::task::spawn(async move {
                            match async_tungstenite::accept_async(stream).await {
                                Err(e) => log::error!("failed to accept stream: {}", e),
                                Ok(socket) => {
                                    let socket = WsStream::new(socket);
                                    let (read_half, write_half) = socket.split();
                                    let mut write_half = Some(write_half);
                                    let mut actor = None;
                                    let mut reader = FramedRead::new(read_half, LinesCodec);
                                    while let Some(t) = reader.next().await {
                                        match t.map_err(Into::<anyhow::Error>::into)
                                            .and_then(|mut x| simd_json::from_str::<ClientMessage>(&mut x).map_err(Into::into)) {
                                                Err(e) => log::error!("{}", e),
                                                Ok(ClientMessage::Submit { student_id, key }) => {
                                                    let real_actor =  if actor.is_some() {
                                                        Ok(actor.clone().unwrap())
                                                    } else if let Some(write_half) = write_half.take() {
                                                        StreamActor::start_new(keeper.clone(), FramedWrite::new(write_half, LinesCodec), false)
                                                            .await
                                                            .map_err(Into::into)
                                                    } else {
                                                        Err(anyhow::anyhow!("invalid submission state"))
                                                    };
                                                    if let Ok(real_actor) = real_actor {
                                                        actor.replace(real_actor.clone());
                                                        if let Err(e) = keeper.send(HouseKeeperMsg::Submit {
                                                            source: real_actor,
                                                            student_id,
                                                            key
                                                        }) {
                                                            log::error!("{}", e);
                                                        }
                                                    }
                                                },
                                                Ok(ClientMessage::Register(reg_token)) => {
                                                    if reg_token == token && write_half.is_some() {
                                                        match StreamActor::start_new(keeper.clone(), FramedWrite::new(write_half.take().unwrap(), LinesCodec), true).await {
                                                            Err(e) => log::error!("{}", e),
                                                            Ok(addr) => {
                                                                log::info!("actor {} registered", addr.actor_id());
                                                                actor.replace(addr);
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                    }
                                    if let Some(mut actor) = actor {
                                        if let Err(e) = keeper.call(HouseKeeperMsg::UnRegister(actor.clone())).await {
                                            log::error!("{}", e);
                                        }
                                        if let Err(e) = actor.stop(None) {
                                            log::error!("{}", e);
                                        }
                                    }
                                    log::info!("communication down!");
                                }
                            }
                        });
                    }
                }
            }
        }
        Mode::Draw { first_class, second_class, third_class } => {
            if let Err(e) = keeper.call(HouseKeeperMsg::Draw { first_class, second_class, third_class }).await {
                log::error!("{}", e);
            }
        }
    }
    Ok(())
}
