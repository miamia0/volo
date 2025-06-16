use std::{
    cell::RefCell,
    collections::VecDeque,
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc,
    },
};

use metainfo::MetaInfo;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{
        oneshot::{self, Receiver},
        Mutex,
    },
};
use tokio_condvar::Condvar;
use volo::{
    context::{Role, RpcInfo},
    net::Address,
};

use crate::{
    codec::{Decoder, Encoder, MakeCodec},
    context::{ClientContext, ThriftContext},
    transport::{
        multiplex::utils::TxHashMap,
        pool::{Poolable, Reservation},
    },
    ApplicationError, ApplicationErrorKind, EntryMessage, Error, ThriftMessage,
};

lazy_static::lazy_static! {
    // This is used for debug.
    static ref TRANSPORT_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);
}

struct BufferQueue<Req> {
    buffer: Mutex<VecDeque<ThriftMessage<Req>>>,
    queue_cv: Condvar,
}

impl<Req> BufferQueue<Req> {
    pub fn new() -> Self {
        Self {
            buffer: Mutex::new(VecDeque::new()),
            queue_cv: Condvar::new(),
        }
    }

    pub async fn push(&self, msg: ThriftMessage<Req>) {
        self.buffer.lock().await.push_back(msg);
        self.queue_cv.notify_all();
    }

    pub fn notify_all(&self) {
        self.queue_cv.notify_all();
    }

    pub async fn get<F>(&self, buf: &mut Vec<ThriftMessage<Req>>, check_fn: F)
    where
        F: Fn() -> bool,
    {
        let mut queue = self.buffer.lock().await;
        while queue.is_empty() && check_fn() {
            queue = self.queue_cv.wait(queue).await;
        }
        while !queue.is_empty() {
            let current = queue.pop_front().expect("this queue should not be empty");
            buf.push(current);
        }
    }
}

pub struct ThriftTransport<E, Req, Resp> {
    _phantom1: PhantomData<fn() -> E>,

    #[allow(clippy::type_complexity)]
    tx_map: Arc<
        TxHashMap<
            oneshot::Sender<crate::Result<Option<(MetaInfo, ClientContext, ThriftMessage<Resp>)>>>,
        >,
    >,
    write_error: Arc<AtomicBool>,
    // read has error
    read_error: Arc<AtomicBool>,
    // read connection is closed
    read_closed: Arc<AtomicBool>,
    // TODO make this to lockless
    batch_queue: Arc<BufferQueue<Req>>,
}

impl<E, Req, Resp> ThriftTransport<E, Req, Resp> {
    pub fn close(&self) {
        self.read_closed
            .store(true, std::sync::atomic::Ordering::Release);
        self.batch_queue.notify_all();
    }
}

impl<E, Req, Resp> ThriftTransport<E, Req, Resp>
where
    E: Encoder,
    Req: EntryMessage + Send + 'static + Sync,
    Resp: EntryMessage + Send + 'static + Sync,
{
    pub fn write_loop(&mut self, mut write_half: WriteHalf<E>) {
        let batch_queu = self.batch_queue.clone();
        let inner_tx_map = self.tx_map.clone();
        let inner_read_error: Arc<AtomicBool> = self.read_error.clone();
        let inner_read_closed = self.read_closed.clone();
        let inner_write_error = self.write_error.clone();
        tokio::spawn(async move {
            let mut buff = Vec::with_capacity(32);
            let mut resolved = Vec::with_capacity(32);
            let mut has_error;
            loop {
                {
                    buff.clear();
                    resolved.clear();
                    write_half.reset().await;
                    has_error = false;
                    batch_queu
                        .get(&mut buff, || {
                            !inner_read_error.load(std::sync::atomic::Ordering::Acquire)
                                && !inner_read_closed.load(std::sync::atomic::Ordering::Acquire)
                        })
                        .await;

                    if inner_read_error.load(std::sync::atomic::Ordering::Acquire)
                        || inner_read_closed.load(std::sync::atomic::Ordering::Acquire)
                    {
                        return;
                    }

                    for b in buff.drain(..) {
                        let seq = b.meta.seq_id;
                        resolved.push(seq);
                        if !has_error {
                            let mut cx = ClientContext::new(
                                seq,
                                RpcInfo::with_role(Role::Client),
                                pilota::thrift::TMessageType::Call,
                            );
                            let res = write_half.encode(&mut cx, b).await;
                            match res {
                                Ok(_) => {}
                                Err(err) => {
                                    tracing::error!(
                                        "[VOLO] multiplex connection encode error: {}",
                                        err
                                    );
                                    inner_write_error
                                        .store(true, std::sync::atomic::Ordering::Relaxed);
                                    has_error = true;
                                    break;
                                }
                            }
                        }
                    }
                    if has_error {
                        for seq in resolved.iter() {
                            match inner_tx_map.remove(seq).await {
                                Some(chan) => {
                                    let _ = chan
                                        .send(Err(Error::Application(ApplicationError::new(
                                            ApplicationErrorKind::UNKNOWN,
                                            format!("write error "),
                                        ))))
                                        .map_err(|_| {
                                            tracing::error!("[VOLO] got err when send result");
                                        });
                                }
                                None => {}
                            };
                        }
                        return;
                    }
                    let res = write_half.flush().await;
                    match res {
                        Ok(_) => {}
                        Err(err) => {
                            tracing::error!("[VOLO] multiplex connection flush error: {}", err,);
                            inner_write_error.store(true, std::sync::atomic::Ordering::Relaxed);
                            for seq in resolved.iter() {
                                match inner_tx_map.remove(seq).await {
                                    Some(chan) => {
                                        let _ = chan
                                            .send(Err(Error::Application(ApplicationError::new(
                                                ApplicationErrorKind::UNKNOWN,
                                                format!("write error "),
                                            ))))
                                            .map_err(|_| {
                                                tracing::error!("[VOLO] got err when send result");
                                            });
                                    }
                                    None => {}
                                };
                            }
                            return;
                        }
                    }
                }
            }
        });
    }

    pub fn new<
        R: AsyncRead + Send + Sync + Unpin + 'static,
        W: AsyncWrite + Send + Sync + Unpin + 'static,
        MkC: MakeCodec<R, W, Encoder = E>,
    >(
        read_half: R,
        write_half: W,
        make_codec: MkC,
        target: Address,
    ) -> Self
    where
        Resp: EntryMessage + Send + 'static,
    {
        tracing::trace!(
            "[VOLO] creating multiplex thrift transport, target: {}",
            target
        );
        let id = TRANSPORT_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (encoder, decoder) = make_codec.make_codec(read_half, write_half);
        let mut read_half = ReadHalf { decoder, id };
        let write_half = WriteHalf { encoder, id };
        #[allow(clippy::type_complexity)]
        let tx_map: Arc<
            TxHashMap<
                oneshot::Sender<
                    crate::Result<Option<(MetaInfo, ClientContext, ThriftMessage<Resp>)>>,
                >,
            >,
        > = Default::default();
        let inner_tx_map = tx_map.clone();
        let write_error = Arc::new(AtomicBool::new(false));
        let inner_write_error = write_error.clone();
        let read_error = Arc::new(AtomicBool::new(false));
        let inner_read_error = read_error.clone();
        let read_closed = Arc::new(AtomicBool::new(false));
        let inner_read_closed = read_closed.clone();
        let queue_cv = Arc::new(Condvar::new());
        let inner_queue_cv = queue_cv.clone();
        //// read loop
        tokio::spawn(async move {
            metainfo::METAINFO
                .scope(RefCell::new(Default::default()), async move {
                    loop {
                        if inner_write_error.load(std::sync::atomic::Ordering::Relaxed) {
                            tracing::trace!(
                                "[VOLO] multiplex write error, break read loop now, target: {}",
                                target
                            );
                            break;
                        }
                        // fake context
                        let mut cx = ClientContext::new(
                            -1,
                            RpcInfo::with_role(Role::Client),
                            pilota::thrift::TMessageType::Call,
                        );
                        let res = read_half.try_next::<Resp>(&mut cx, target.clone()).await;
                        if let Err(e) = res {
                            tracing::error!(
                                "[VOLO] multiplex connection read error: {}, target: {}",
                                e,
                                target
                            );
                            inner_read_error.store(true, std::sync::atomic::Ordering::Relaxed);
                            inner_queue_cv.notify_all();

                            inner_tx_map
                                .for_all_drain(|tx| {
                                    let _ =
                                        tx.send(Err(Error::Application(ApplicationError::new(
                                            ApplicationErrorKind::UNKNOWN,
                                            format!(
                                                "multiplex connection error: {e}, target: {target}"
                                            ),
                                        ))));
                                })
                                .await;
                            return;
                        }
                        // we have checked the error above, so it's safe to unwrap here
                        let res = res.unwrap();
                        if res.is_none() {
                            // the connection is closed
                            if !inner_tx_map.is_empty().await {
                                inner_read_error.store(true, std::sync::atomic::Ordering::Relaxed);
                                inner_tx_map
                                    .for_all_drain(|tx| {
                                        let _ = tx.send(Ok(None));
                                    })
                                    .await;
                            }
                            inner_read_closed.store(true, std::sync::atomic::Ordering::Relaxed);
                            inner_queue_cv.notify_all();
                            return;
                        }
                        // now we get ThriftMessage<Resp>
                        let res = res.unwrap();
                        let seq_id = res.meta.seq_id;
                        if let Some(tx) = inner_tx_map.remove(&seq_id).await {
                            metainfo::METAINFO.with(|mi| {
                                let mi = mi.take();
                                let _ = tx.send(Ok(Some((mi, cx, res))));
                            });
                        } else {
                            tracing::error!(
                                "[VOLO] multiplex connection receive unexpected response, seq_id: \
                                 {}, target: {}",
                                seq_id,
                                target
                            );
                        }
                    }
                })
                .await;
        });
        let mut ret = Self {
            tx_map,
            write_error,
            read_error,
            read_closed,
            batch_queue: Arc::new(BufferQueue::new()),
            _phantom1: PhantomData,
        };
        ret.write_loop(write_half);
        ret
    }
}

impl<E, Req, Resp> Drop for ThriftTransport<E, Req, Resp> {
    fn drop(&mut self) {
        self.close();
    }
}

impl<E, Req, Resp> ThriftTransport<E, Req, Resp>
where
    E: Encoder,
    Resp: EntryMessage,
    Req: EntryMessage,
{
    pub async fn only_send(
        &self,
        cx: &mut ClientContext,
        msg: ThriftMessage<Req>,
        oneway: bool,
    ) -> Result<
        Receiver<Result<Option<(MetaInfo, ClientContext, ThriftMessage<Resp>)>, Error>>,
        Error,
    > {
        // check error and closed
        if self.read_error.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(Error::Application(ApplicationError::new(
                ApplicationErrorKind::UNKNOWN,
                "multiplex connection error".to_string(),
            )));
        }
        if self.read_closed.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(Error::Application(ApplicationError::new(
                ApplicationErrorKind::UNKNOWN,
                "multiplex connection closed".to_string(),
            )));
        }
        if self.write_error.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(Error::Application(ApplicationError::new(
                ApplicationErrorKind::UNKNOWN,
                "multiplex connection error".to_string(),
            )));
        }

        let (tx, rx) = oneshot::channel();
        let seq_id = msg.meta.seq_id;
        if !oneway {
            self.tx_map.insert(seq_id, tx).await;
        }
        self.batch_queue.push(msg).await;
        Ok(rx)
    }

    pub async fn send(
        &self,
        cx: &mut ClientContext,
        msg: ThriftMessage<Req>,
        oneway: bool,
    ) -> Result<Option<ThriftMessage<Resp>>, Error> {
        let rx = self.only_send(cx, msg, oneway).await?;
        if oneway {
            return Ok(None);
        }
        wait(cx, rx).await
    }
}

pub async fn wait<T>(
    cx: &mut ClientContext,
    rx: Receiver<Result<Option<(MetaInfo, ClientContext, ThriftMessage<T>)>, Error>>,
) -> Result<Option<ThriftMessage<T>>, Error> {
    match rx.await {
        Ok(res) => match res {
            Ok(opt) => match opt {
                None => Ok(None),
                Some((mi, new_cx, msg)) => {
                    metainfo::METAINFO.with(|m| {
                        m.borrow_mut().extend(mi);
                    });
                    // TODO: cx extend
                    if let Some(t) = new_cx.common_stats.decode_start_at() {
                        cx.common_stats.set_decode_start_at(t);
                    }
                    if let Some(t) = new_cx.common_stats.decode_end_at() {
                        cx.common_stats.set_decode_end_at(t);
                    }
                    if let Some(t) = new_cx.common_stats.read_start_at() {
                        cx.common_stats.set_read_start_at(t);
                    }
                    if let Some(t) = new_cx.common_stats.read_end_at() {
                        cx.common_stats.set_read_end_at(t);
                    }
                    if let Some(s) = new_cx.common_stats.read_size() {
                        cx.common_stats.set_read_size(s);
                    }
                    Ok(Some(msg))
                }
            },
            Err(e) => Err(e),
        },
        Err(e) => {
            tracing::error!("[VOLO] multiplex connection oneshot recv error: {e}");
            Err(Error::Application(ApplicationError::new(
                ApplicationErrorKind::UNKNOWN,
                format!("multiplex connection oneshot recv error: {e}"),
            )))
        }
    }
}

pub struct ReadHalf<D> {
    decoder: D,
    id: usize,
}

impl<D> ReadHalf<D>
where
    D: Decoder,
{
    pub async fn try_next<T: EntryMessage>(
        &mut self,
        cx: &mut ClientContext,
        target: Address,
    ) -> Result<Option<ThriftMessage<T>>, Error> {
        let thrift_msg = self.decoder.decode(cx).await.map_err(|e| {
            tracing::error!(
                "[VOLO] transport[{}] decode error: {}, target: {}",
                self.id,
                e,
                target
            );
            e
        })?;

        // TODO: move this to recv
        // if let Some(ThriftMessage { meta, .. }) = &thrift_msg {
        //     if meta.seq_id != cx.seq_id {
        //         tracing::error!(
        //             "[VOLO] transport[{}] seq_id not match: {} != {}",
        //             self.id,
        //             meta.seq_id,
        //             cx.seq_id,
        //         );
        //         return Err(Error::Application(ApplicationError::new(
        //             ApplicationErrorKind::BadSequenceId,
        //             "seq_id not match",
        //         )));
        //     }
        // };
        Ok(thrift_msg)
    }
}

pub struct WriteHalf<E> {
    encoder: E,
    id: usize,
}

#[allow(dead_code)]
impl<E> WriteHalf<E>
where
    E: Encoder,
{
    pub async fn send<T: EntryMessage>(
        &mut self,
        cx: &mut impl ThriftContext,
        msg: ThriftMessage<T>,
    ) -> Result<(), Error> {
        self.encoder.send(cx, msg).await.map_err(|mut e| {
            e.append_msg(&format!(", rpcinfo: {:?}", cx.rpc_info()));
            tracing::error!("[VOLO] transport[{}] encode error: {:?}", self.id, e);
            e
        })
    }
    pub async fn reset(&mut self) {
        self.encoder.reset().await;
    }

    pub async fn encode<T: EntryMessage>(
        &mut self,
        cx: &mut impl ThriftContext,
        msg: ThriftMessage<T>,
    ) -> Result<(), Error> {
        self.encoder.encode(cx, msg).await.map_err(|mut e| {
            e.append_msg(&format!(", rpcinfo: {:?}", cx.rpc_info()));
            tracing::error!("[VOLO] transport[{}] encode error: {:?}", self.id, e);
            e
        })
    }

    pub async fn flush(&mut self) -> Result<(), Error> {
        self.encoder.flush().await
    }
}

impl<TTEncoder, Req, Resp> Poolable for Arc<ThriftTransport<TTEncoder, Req, Resp>>
where
    TTEncoder: Encoder,
    Req: EntryMessage + Send + 'static + Sync,
    Resp: EntryMessage + Send + 'static + Sync,
{
    fn reusable(&self) -> bool {
        !self.write_error.load(std::sync::atomic::Ordering::Relaxed)
            && !self.read_error.load(std::sync::atomic::Ordering::Relaxed)
            && !self.read_closed.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn reserve(self) -> Reservation<Self> {
        Reservation::Shared(self.clone(), self)
    }

    fn can_share(&self) -> bool {
        true
    }
}
