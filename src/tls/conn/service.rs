use std::{
    error::Error as StdError,
    fmt::{self, Debug},
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use http::{Uri, uri::Scheme};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_btls::SslStream;
use tower::{BoxError, Service};

use super::{EstablishedConn, HttpsConnector, MaybeHttpsStream};
use crate::{
    conn::{Connection, descriptor::ConnectionDescriptor},
    ext::UriExt,
};

type BoxFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send>>;

/// A failed TLS handshake, with whether the peer certificate failed verification.
///
/// Used by `crate::Error::transport_cause` to distinguish certificate verification
/// failures from other handshake failures.
#[derive(Debug)]
pub(crate) struct HandshakeFailure {
    certificate: bool,
    source: btls::ssl::Error,
}

impl HandshakeFailure {
    #[inline]
    pub(crate) fn is_certificate(&self) -> bool {
        self.certificate
    }
}

impl fmt::Display for HandshakeFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TLS handshake failed")
    }
}

impl StdError for HandshakeFailure {
    #[inline]
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.source)
    }
}

async fn perform_handshake<T>(ssl: btls::ssl::Ssl, conn: T) -> Result<MaybeHttpsStream<T>, BoxError>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut stream = SslStream::new(ssl, conn)?;
    if let Err(source) = Pin::new(&mut stream).connect().await {
        // BoringSSL reports INVALID_CALL until a peer certificate was verified;
        // only a real verification error marks a certificate failure.
        let certificate = matches!(
            stream.ssl().verify_result(),
            Err(error) if error != btls::x509::X509VerifyError::INVALID_CALL
        );
        return Err(HandshakeFailure {
            certificate,
            source,
        }
        .into());
    }
    Ok(MaybeHttpsStream::Https(stream))
}

impl<T, S> Service<Uri> for HttpsConnector<S>
where
    S: Service<Uri, Response = T> + Send,
    S::Error: Into<BoxError>,
    S::Future: Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Connection + Unpin + Debug + Sync + Send + 'static,
{
    type Response = MaybeHttpsStream<T>;
    type Error = BoxError;
    type Future = BoxFuture<Self::Response, Self::Error>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.http.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let connect = self.http.call(uri.clone());
        let tls = self.tls.clone();

        let f = async move {
            let conn = connect.await.map_err(Into::into)?;

            // Early return if it is not a tls scheme
            if uri.scheme() != Some(&Scheme::HTTPS) {
                return Ok(MaybeHttpsStream::Http(conn));
            }

            let ssl = tls.setup_ssl(uri)?;
            perform_handshake(ssl, conn).await
        };

        Box::pin(f)
    }
}

impl<T, S> Service<ConnectionDescriptor> for HttpsConnector<S>
where
    S: Service<Uri, Response = T> + Send,
    S::Error: Into<BoxError>,
    S::Future: Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Connection + Unpin + Debug + Sync + Send + 'static,
{
    type Response = MaybeHttpsStream<T>;
    type Error = BoxError;
    type Future = BoxFuture<Self::Response, Self::Error>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.http.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, descriptor: ConnectionDescriptor) -> Self::Future {
        let uri = descriptor.uri().clone();
        let connect = self.http.call(uri.clone());
        let tls = self.tls.clone();

        let f = async move {
            let conn = connect.await.map_err(Into::into)?;

            // Early return if it is not a tls scheme
            if uri.is_http() {
                return Ok(MaybeHttpsStream::Http(conn));
            }

            let ssl = tls.setup_ssl2(descriptor)?;
            perform_handshake(ssl, conn).await
        };

        Box::pin(f)
    }
}

impl<T, S, IO> Service<EstablishedConn<IO>> for HttpsConnector<S>
where
    S: Service<Uri, Response = T> + Send + Clone + 'static,
    S::Error: Into<BoxError>,
    S::Future: Unpin + Send + 'static,
    T: AsyncRead + AsyncWrite + Connection + Unpin + Debug + Sync + Send + 'static,
    IO: AsyncRead + AsyncWrite + Unpin + Send + Sync + Debug + 'static,
{
    type Response = MaybeHttpsStream<IO>;
    type Error = BoxError;
    type Future = BoxFuture<Self::Response, Self::Error>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.http.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, conn: EstablishedConn<IO>) -> Self::Future {
        let tls = self.tls.clone();
        let fut = async move {
            // Early return if it is not a tls scheme
            if conn.descriptor.uri().is_http() {
                return Ok(MaybeHttpsStream::Http(conn.io));
            }

            let ssl = tls.setup_ssl2(conn.descriptor)?;
            perform_handshake(ssl, conn.io).await
        };

        Box::pin(fut)
    }
}
