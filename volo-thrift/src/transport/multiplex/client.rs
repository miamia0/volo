use std::{collections::VecDeque, io, marker::PhantomData};

use futures::channel::oneshot::channel;
use motore::service::{Service, UnaryService};
use parking_lot::Mutex;
use pilota::thrift::TransportErrorKind;
use volo::net::{dial::MakeTransport, Address};

use crate::{
    codec::MakeCodec,
    context::ClientContext,
    protocol::TMessageType,
    transport::{
        multiplex::thrift_transport::ThriftTransport,
        pool::{Config, PooledMakeTransport},
    },
    EntryMessage, Error, ThriftMessage,
};

pub struct MakeClientTransport<MkT, MkC, Req, Resp>
where
    MkT: MakeTransport,
    MkC: MakeCodec<MkT::ReadHalf, MkT::WriteHalf>,
{
    make_transport: MkT,
    make_codec: MkC,
    _phantom: PhantomData<fn() -> Resp>,
    _phantom1: PhantomData<fn() -> Req>,
}

impl<MkT: MakeTransport, MkC: MakeCodec<MkT::ReadHalf, MkT::WriteHalf>, Req, Resp> Clone
    for MakeClientTransport<MkT, MkC, Req, Resp>
{
    fn clone(&self) -> Self {
        Self {
            make_transport: self.make_transport.clone(),
            make_codec: self.make_codec.clone(),
            _phantom: PhantomData,
            _phantom1: PhantomData,
        }
    }
}

impl<MkT, MkC, Req, Resp> MakeClientTransport<MkT, MkC, Req, Resp>
where
    MkT: MakeTransport,
    MkC: MakeCodec<MkT::ReadHalf, MkT::WriteHalf>,
{
    #[allow(unused)]
    pub fn new(make_transport: MkT, make_codec: MkC) -> Self {
        Self {
            make_transport,
            make_codec,
            _phantom: PhantomData,
            _phantom1: PhantomData,
        }
    }
}

impl<MkT, MkC, Req, Resp> UnaryService<Address> for MakeClientTransport<MkT, MkC, Req, Resp>
where
    MkT: MakeTransport,
    MkC: MakeCodec<MkT::ReadHalf, MkT::WriteHalf> + Sync,
    Resp: EntryMessage + Send + 'static + Sync,
    Req: EntryMessage + Send + 'static + Sync,
{
    type Response = ThriftTransport<MkC::Encoder, Req, Resp>;
    type Error = io::Error;

    async fn call(&self, target: Address) -> Result<Self::Response, Self::Error> {
        let make_transport = self.make_transport.clone();
        let (rh, wh) = make_transport.make_transport(target.clone()).await?;
        Ok(ThriftTransport::new(
            rh,
            wh,
            self.make_codec.clone(),
            target,
        ))
    }
}

pub struct Client<Req, Resp, MkT, MkC>
where
    MkT: MakeTransport,
    MkC: MakeCodec<MkT::ReadHalf, MkT::WriteHalf> + Sync,
    Resp: EntryMessage + Send + 'static + Sync,
    Req: EntryMessage + Send + 'static + Sync,
{
    #[allow(clippy::type_complexity)]
    make_transport: PooledMakeTransport<MakeClientTransport<MkT, MkC, Req, Resp>, Address>,
    _marker: PhantomData<Resp>,
}

impl<Req, Resp, MkT, MkC> Clone for Client<Req, Resp, MkT, MkC>
where
    MkT: MakeTransport,
    MkC: MakeCodec<MkT::ReadHalf, MkT::WriteHalf> + Sync,
    Resp: EntryMessage + Send + 'static + Sync,
    Req: EntryMessage + Send + 'static + Sync,
{
    fn clone(&self) -> Self {
        Self {
            make_transport: self.make_transport.clone(),
            _marker: self._marker,
        }
    }
}

impl<Req, Resp, MkT, MkC> Client<Req, Resp, MkT, MkC>
where
    MkT: MakeTransport,
    MkC: MakeCodec<MkT::ReadHalf, MkT::WriteHalf> + Sync,
    Resp: EntryMessage + Send + 'static + Sync,
    Req: EntryMessage + Send + 'static + Sync,
{
    pub fn new(make_transport: MkT, pool_cfg: Option<Config>, make_codec: MkC) -> Self {
        let make_transport = MakeClientTransport::new(make_transport, make_codec);
        let make_transport = PooledMakeTransport::new(make_transport, pool_cfg);
        Client {
            make_transport,
            _marker: PhantomData,
        }
    }

    // pub fn write_loop(&self) -> Result<()> {
    //     tokio::spawn(async move {
    //         loop {
    //             {
    //                 let mut queue = self.batch_queue.lock();
    //                 while queue.is_empty() {
    //                     // yields current thread and unlock queue
    //                     queue = db.process_batch_sem.wait(queue).unwrap();
    //                 }
    //                 queue.pop_front().unwrap()
    //             }
    //         }
    //     })
    // }
}

impl<Req, Resp, MkT, MkC> Service<ClientContext, ThriftMessage<Req>> for Client<Req, Resp, MkT, MkC>
where
    Req: Send + 'static + EntryMessage + Sync,
    Resp: EntryMessage + Send + 'static + Sync,
    MkT: MakeTransport,
    MkC: MakeCodec<MkT::ReadHalf, MkT::WriteHalf> + Sync,
{
    type Response = Option<ThriftMessage<Resp>>;

    type Error = crate::Error;

    async fn call<'cx, 's>(
        &'s self,
        cx: &'cx mut ClientContext,
        req: ThriftMessage<Req>,
    ) -> Result<Self::Response, Self::Error> {
        let rpc_info = &cx.rpc_info;
        let target = rpc_info.callee().address().ok_or_else(|| {
            let msg = format!("address is required, rpcinfo: {:?}", rpc_info);
            crate::Error::Transport(io::Error::new(io::ErrorKind::InvalidData, msg).into())
        })?;
        let oneway = cx.message_type == TMessageType::OneWay;
        cx.stats.record_make_transport_start_at();
        let transport = self.make_transport.call(target).await?;
        cx.stats.record_make_transport_end_at();
        let resp = transport.send(cx, req, oneway).await;
        if let Ok(None) = resp {
            if !oneway {
                return Err(Error::Transport(pilota::thrift::TransportError::new(
                    TransportErrorKind::EndOfFile,
                    format!("an unexpected end of file from server, cx: {:?}", cx),
                )));
            }
        }
        if cx.transport.should_reuse && resp.is_ok() {
            transport.reuse();
        }
        resp
    }
}
