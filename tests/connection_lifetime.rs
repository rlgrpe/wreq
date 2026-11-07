#![cfg(any(feature = "tokio-rt", feature = "compio-rt"))]

mod support;

use std::{
    convert::Infallible,
    future::{Future, poll_fn},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};

use btls::{
    pkey::PKey,
    ssl::{AlpnError, Ssl, SslAcceptor, SslMethod, select_next_proto},
    x509::X509,
};
use futures_util::future::join;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{Notify, mpsc},
};
use tokio_btls::SslStream;
use wreq::{Client, Version};
use wreq_proto::rt::Timer as _;
#[cfg(all(not(feature = "tokio-rt"), feature = "compio-rt"))]
use wreq_rt::rt::compio::{CompioExecutor as Executor, CompioTimer as Timer};
#[cfg(feature = "tokio-rt")]
use wreq_rt::rt::tokio::{TokioExecutor as Executor, TokioTimer as Timer};

#[derive(Clone, Default)]
struct Tasks {
    active: Arc<AtomicUsize>,
    changed: Arc<Notify>,
}

struct TaskGuard(Tasks);

impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
        self.0.changed.notify_one();
    }
}

impl<F> wreq_proto::rt::Executor<F> for Tasks
where
    F: Future<Output = ()> + Send + 'static,
{
    fn execute(&self, future: F) {
        self.active.fetch_add(1, Ordering::SeqCst);
        let guard = TaskGuard(self.clone());
        wreq_proto::rt::Executor::execute(&Executor::new(), async move {
            let _guard = guard;
            future.await;
        });
    }
}

impl Tasks {
    async fn drained(&self) {
        while self.active.load(Ordering::SeqCst) != 0 {
            self.changed.notified().await;
        }
    }
}

async fn within<F: Future>(future: F) -> F::Output {
    tokio::select! {
        result = future => result,
        _ = Timer::new().sleep(Duration::from_secs(5)) => {
            panic!("connection lifetime test timed out");
        }
    }
}

fn client(server: &support::server::Server, tasks: Tasks) -> wreq::ClientBuilder {
    Client::builder()
        .proxy(wreq::Proxy::all(format!("http://{}", server.addr())).unwrap())
        .tls_cert_verification(false)
        .tls_verify_hostname(false)
        .retry(wreq::retry::Policy::never())
        .executor(tasks)
}

fn acceptor(http2: bool) -> SslAcceptor {
    let mut builder = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls()).unwrap();
    builder
        .set_certificate(&X509::from_der(include_bytes!("support/server.cert")).unwrap())
        .unwrap();
    builder
        .set_private_key(&PKey::private_key_from_der(include_bytes!("support/server.key")).unwrap())
        .unwrap();
    let protocols: &'static [u8] = if http2 { b"\x02h2" } else { b"\x08http/1.1" };
    builder.set_alpn_select_callback(move |_, offered| {
        select_next_proto(protocols, offered).ok_or(AlpnError::NOACK)
    });
    builder.build()
}

async fn tunnel<'a>(
    socket: &'a mut TcpStream,
    acceptor: &SslAcceptor,
) -> SslStream<&'a mut TcpStream> {
    socket
        .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        .await
        .unwrap();
    let mut stream = SslStream::new(Ssl::new(acceptor.context()).unwrap(), socket).unwrap();
    Pin::new(&mut stream).accept().await.unwrap();
    stream
}

async fn read_head<R: AsyncRead + Unpin>(stream: &mut R) -> Vec<u8> {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        head.push(stream.read_u8().await.unwrap());
    }
    head
}

async fn eof(socket: &mut TcpStream) {
    // Read the raw socket so a TLS close_notify does not hide whether TCP closed.
    socket.read_to_end(&mut Vec::new()).await.unwrap();
}

async fn serve_http2(socket: &mut TcpStream, acceptor: &SslAcceptor, requests: Arc<AtomicUsize>) {
    let stream = tunnel(socket, acceptor).await;
    let service =
        hyper::service::service_fn(move |request: http::Request<hyper::body::Incoming>| {
            requests.fetch_add(1, Ordering::SeqCst);
            assert_eq!(request.version(), Version::HTTP_2);
            async { Ok::<_, Infallible>(http::Response::new(wreq::Body::from("ok"))) }
        });
    hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
        .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
        .await
        .unwrap();
    eof(socket).await;
}

#[cfg_attr(feature = "tokio-rt", tokio::test)]
#[cfg_attr(all(not(feature = "tokio-rt"), feature = "compio-rt"), compio::test)]
async fn pooled_connection_cancels_losing_connect() {
    let tasks = Tasks::default();
    let connections = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    let (ready_tx, mut ready_rx) = mpsc::unbounded_channel();
    let (closed_tx, mut closed_rx) = mpsc::unbounded_channel();
    let acceptor = Arc::new(acceptor(false));
    let server = support::server::low_level_with_response({
        let connections = connections.clone();
        let release = release.clone();
        move |request, socket| {
            assert!(request.starts_with(b"CONNECT localhost:443 HTTP/1.1\r\n"));
            let id = connections.fetch_add(1, Ordering::SeqCst);
            let ready_tx = ready_tx.clone();
            let closed_tx = closed_tx.clone();
            let release = release.clone();
            let acceptor = acceptor.clone();
            Box::new(async move {
                if id == 0 {
                    let mut stream = tunnel(socket, &acceptor).await;
                    assert!(read_head(&mut stream).await.starts_with(b"GET /first "));
                    ready_tx.send(0).unwrap();
                    release.notified().await;
                    stream
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                        .await
                        .unwrap();
                    for path in [b"GET /second ".as_slice(), b"GET /third ".as_slice()] {
                        assert!(read_head(&mut stream).await.starts_with(path));
                        stream
                            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                            .await
                            .unwrap();
                    }
                    eof(stream.get_mut()).await;
                } else {
                    assert_eq!(id, 1, "unexpected replacement connection");
                    // The callback runs only after the complete CONNECT head was read.
                    ready_tx.send(1).unwrap();
                    eof(socket).await;
                }
                closed_tx.send(id).unwrap();
            })
        }
    });
    let client = client(&server, tasks.clone()).build().unwrap();
    let mut first = Box::pin(async {
        client
            .get("https://localhost/first")
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });
    within(async {
        tokio::select! {
            _ = &mut first => panic!("first response was not held"),
            ready = ready_rx.recv() => assert_eq!(ready, Some(0)),
        }
    })
    .await;
    let mut second = Box::pin(async {
        client
            .get("https://localhost/second")
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    });
    within(async {
        tokio::select! {
            _ = &mut first => panic!("first response was not held"),
            _ = &mut second => panic!("second CONNECT was not held"),
            ready = ready_rx.recv() => assert_eq!(ready, Some(1)),
        }
    })
    .await;
    release.notify_one();
    let (first, second) = within(join(first, second)).await;
    assert_eq!(first, "ok");
    assert_eq!(second, "ok");
    // The unused connection must close even while the client is still reusable.
    assert_eq!(within(closed_rx.recv()).await, Some(1));
    assert_eq!(
        within(async {
            client
                .get("https://localhost/third")
                .send()
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
        })
        .await,
        "ok"
    );
    drop(client);
    assert_eq!(within(closed_rx.recv()).await, Some(0));
    within(tasks.drained()).await;
    assert_eq!(connections.load(Ordering::SeqCst), 2);
}

#[cfg_attr(feature = "tokio-rt", tokio::test)]
#[cfg_attr(all(not(feature = "tokio-rt"), feature = "compio-rt"), compio::test)]
async fn canceling_connect_closes_socket() {
    let (ready_tx, mut ready_rx) = mpsc::unbounded_channel();
    let (closed_tx, mut closed_rx) = mpsc::unbounded_channel();
    let server = support::server::low_level_with_response(move |request, socket| {
        assert!(request.starts_with(b"CONNECT localhost:443 HTTP/1.1\r\n"));
        let closed_tx = closed_tx.clone();
        ready_tx.send(()).unwrap();
        Box::new(async move {
            eof(socket).await;
            closed_tx.send(()).unwrap();
        })
    });
    for http2 in [false, true] {
        let tasks = Tasks::default();
        let builder = client(&server, tasks.clone());
        let client = if http2 { builder.http2_only() } else { builder }
            .build()
            .unwrap();
        let mut request = Box::pin(client.get("https://localhost/canceled").send());
        within(async {
            tokio::select! {
                _ = &mut request => panic!("CONNECT was not held"),
                ready = ready_rx.recv() => ready.unwrap(),
            }
        })
        .await;
        drop(request);
        within(closed_rx.recv()).await.unwrap();
        drop(client);
        within(tasks.drained()).await;
    }
}

#[cfg_attr(feature = "tokio-rt", tokio::test)]
#[cfg_attr(all(not(feature = "tokio-rt"), feature = "compio-rt"), compio::test)]
async fn canceling_http2_connect_wakes_waiters() {
    // Both builder-level and per-request HTTP/2 must recover an unsent checkout.
    for per_request in [false, true] {
        let tasks = Tasks::default();
        let connections = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(AtomicUsize::new(0));
        let (ready_tx, mut ready_rx) = mpsc::unbounded_channel();
        let (closed_tx, mut closed_rx) = mpsc::unbounded_channel();
        let acceptor = Arc::new(acceptor(true));
        let server = support::server::low_level_with_response({
            let connections = connections.clone();
            let requests = requests.clone();
            move |request, socket| {
                assert!(request.starts_with(b"CONNECT localhost:443 HTTP/1.1\r\n"));
                let id = connections.fetch_add(1, Ordering::SeqCst);
                let closed_tx = closed_tx.clone();
                let requests = requests.clone();
                let acceptor = acceptor.clone();
                ready_tx.send(id).unwrap();
                Box::new(async move {
                    if id == 0 {
                        eof(socket).await;
                    } else {
                        assert_eq!(id, 1, "HTTP/2 waiters opened duplicate connections");
                        serve_http2(socket, &acceptor, requests).await;
                    }
                    closed_tx.send(id).unwrap();
                })
            }
        });
        let builder = client(&server, tasks.clone());
        let client = if per_request {
            builder
        } else {
            builder.http2_only()
        }
        .build()
        .unwrap();
        let send = |path| {
            client
                .get(format!("https://localhost/{path}"))
                .version(Version::HTTP_2)
                .send()
        };
        let mut first = Box::pin(send("canceled"));
        within(async {
            tokio::select! {
                _ = &mut first => panic!("first CONNECT was not held"),
                ready = ready_rx.recv() => assert_eq!(ready, Some(0)),
            }
        })
        .await;
        let mut second = Box::pin(send("second"));
        let mut third = Box::pin(send("third"));
        poll_fn(|cx| {
            assert!(second.as_mut().poll(cx).is_pending());
            assert!(third.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert_eq!(connections.load(Ordering::SeqCst), 1);
        drop(first);
        let (second, third) = within(join(second, third)).await;
        for response in [second.unwrap(), third.unwrap()] {
            assert_eq!(response.version(), Version::HTTP_2);
            assert_eq!(within(response.bytes()).await.unwrap(), "ok");
        }
        assert_eq!(within(closed_rx.recv()).await, Some(0));
        drop(client);
        assert_eq!(within(closed_rx.recv()).await, Some(1));
        within(tasks.drained()).await;
        assert_eq!(connections.load(Ordering::SeqCst), 2);
        assert_eq!(requests.load(Ordering::SeqCst), 2);
    }
}

#[cfg_attr(feature = "tokio-rt", tokio::test)]
#[cfg_attr(all(not(feature = "tokio-rt"), feature = "compio-rt"), compio::test)]
async fn negotiated_http2_connection_serves_concurrent_requests() {
    let tasks = Tasks::default();
    let connections = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(AtomicUsize::new(0));
    let (closed_tx, mut closed_rx) = mpsc::unbounded_channel();
    let acceptor = Arc::new(acceptor(true));
    let server = support::server::low_level_with_response({
        let connections = connections.clone();
        let requests = requests.clone();
        move |request, socket| {
            assert!(request.starts_with(b"CONNECT localhost:443 HTTP/1.1\r\n"));
            assert_eq!(connections.fetch_add(1, Ordering::SeqCst), 0);
            let closed_tx = closed_tx.clone();
            let requests = requests.clone();
            let acceptor = acceptor.clone();
            Box::new(async move {
                serve_http2(socket, &acceptor, requests).await;
                closed_tx.send(()).unwrap();
            })
        }
    });
    let client = client(&server, tasks.clone()).build().unwrap();
    let first = within(client.get("https://localhost/first").send())
        .await
        .unwrap();
    assert_eq!(first.version(), Version::HTTP_2);
    assert_eq!(within(first.bytes()).await.unwrap(), "ok");
    let (second, third) = within(join(
        client.get("https://localhost/second").send(),
        client.get("https://localhost/third").send(),
    ))
    .await;
    for response in [second.unwrap(), third.unwrap()] {
        assert_eq!(response.version(), Version::HTTP_2);
        assert_eq!(within(response.bytes()).await.unwrap(), "ok");
    }
    drop(client);
    within(closed_rx.recv()).await.unwrap();
    within(tasks.drained()).await;
    assert_eq!(connections.load(Ordering::SeqCst), 1);
    assert_eq!(requests.load(Ordering::SeqCst), 3);
}
