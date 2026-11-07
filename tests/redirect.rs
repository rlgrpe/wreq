mod support;
use http::StatusCode;
use http_body_util::BodyExt;
use std::sync::{Arc, Mutex};
use support::server;
use wreq::{
    Body, Client, header,
    redirect::{BeforeRedirect, History, Policy},
};

#[tokio::test]
async fn before_redirect_wait_preserves_total_and_read_timeout_budgets() {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    for read_timeout in [false, true] {
        for network_delay in [Duration::ZERO, Duration::from_millis(150)] {
            let sends = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&sends);
            let server = server::http(move |request| {
                let sends = Arc::clone(&observed);
                async move {
                    sends.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(network_delay).await;
                    if request.uri().path() == "/source" {
                        http::Response::builder()
                            .status(307)
                            .header(header::LOCATION, "/destination")
                            .body(Body::default())
                            .unwrap()
                    } else {
                        http::Response::new(Body::default())
                    }
                }
            });
            let builder = Client::builder().no_proxy().redirect(Policy::limited(10));
            let client = if read_timeout {
                builder.read_timeout(Duration::from_millis(250))
            } else {
                builder.timeout(Duration::from_millis(250))
            }
            .build()
            .unwrap();
            let mut request = client
                .get(format!("http://{}/source", server.addr()))
                .build()
                .unwrap();
            request
                .extensions_mut()
                .insert(BeforeRedirect::new(|_| async {
                    tokio::time::sleep(Duration::from_millis(350)).await;
                }));
            let result = client.execute(request).await;
            if network_delay.is_zero() {
                assert_eq!(result.unwrap().status(), StatusCode::OK);
            } else {
                assert!(result.unwrap_err().is_timeout());
            }
            assert_eq!(
                sends.load(Ordering::SeqCst),
                2,
                "callback wait cannot spend the transport budget"
            );
        }
    }
}

#[cfg(feature = "stream")]
#[tokio::test]
async fn before_redirect_multiple_pauses_preserve_remaining_response_body_deadline() {
    use std::{convert::Infallible, time::Duration};

    let server = server::http(|request| async move {
        let location = match request.uri().path() {
            "/source" => Some("/middle"),
            "/middle" => Some("/destination"),
            "/destination" => None,
            path => panic!("unexpected path: {path}"),
        };
        if let Some(location) = location {
            tokio::time::sleep(Duration::from_millis(500)).await;
            http::Response::builder()
                .status(307)
                .header(header::LOCATION, location)
                .body(Body::default())
                .unwrap()
        } else {
            let body = Body::wrap_stream(futures_util::stream::once(async {
                tokio::time::sleep(Duration::from_millis(1500)).await;
                Ok::<_, Infallible>("Hello")
            }));
            http::Response::builder()
                .header(header::CONTENT_LENGTH, "5")
                .body(body)
                .unwrap()
        }
    });
    let client = Client::builder()
        .no_proxy()
        .redirect(Policy::limited(10))
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let (entered, mut callbacks) = tokio::sync::mpsc::channel(1);
    let release = Arc::new(tokio::sync::Notify::new());
    let callback_release = Arc::clone(&release);
    let mut request = client
        .get(format!("http://{}/source", server.addr()))
        .build()
        .unwrap();
    request
        .extensions_mut()
        .insert(BeforeRedirect::new(move |request| {
            let path = request.uri().path().to_owned();
            let entered = entered.clone();
            let release = Arc::clone(&callback_release);
            async move {
                entered.send(path).await.unwrap();
                release.notified().await;
            }
        }));

    let release_callbacks = async {
        for expected in ["/middle", "/destination"] {
            assert_eq!(callbacks.recv().await.as_deref(), Some(expected));
            // Each callback waits longer than the entire transport budget.
            // Both waits must be excluded, without replacing the first pause
            // adjustment or forgetting either 500ms network wait.
            tokio::time::sleep(Duration::from_millis(2500)).await;
            release.notify_one();
        }
    };
    let (response, ()) = tokio::time::timeout(Duration::from_secs(12), async {
        tokio::join!(client.execute(request), release_callbacks)
    })
    .await
    .expect("both redirect callbacks must be reached and released");
    let response = response.expect("headers fit the remaining transport budget after both pauses");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.uri().path(), "/destination");

    // The two network waits leave at most one second for the 1.5s body.
    // Resetting the budget at either hop or at body creation would succeed.
    let error = tokio::time::timeout(Duration::from_secs(3), response.text())
        .await
        .expect("the adjusted total deadline must also bound the body")
        .expect_err("the body exceeds the remaining, not a fresh, transport budget");
    assert!(error.is_body() && error.is_timeout());
}

#[tokio::test]
async fn before_redirect_concurrent_request_clones_have_independent_pause_state() {
    use std::time::Duration;

    let timed_at_destination = Arc::new(tokio::sync::Notify::new());
    let observed_destination = Arc::clone(&timed_at_destination);
    let server = server::http(move |request| {
        let timed_at_destination = Arc::clone(&observed_destination);
        async move {
            if request.uri().path() == "/source" {
                http::Response::builder()
                    .status(307)
                    .header(header::LOCATION, "/destination")
                    .body(Body::default())
                    .unwrap()
            } else {
                assert_eq!(request.uri().path(), "/destination");
                if request.headers()["x-request"] == "timed" {
                    timed_at_destination.notify_one();
                    std::future::pending::<()>().await;
                }
                http::Response::new(Body::default())
            }
        }
    });
    let client = Client::builder()
        .no_proxy()
        .redirect(Policy::limited(10))
        .timeout(Duration::from_secs(1))
        .build()
        .unwrap();
    let blocked_callback = Arc::new(tokio::sync::Notify::new());
    let entered = Arc::clone(&blocked_callback);
    let release = Arc::new(tokio::sync::Notify::new());
    let callback_release = Arc::clone(&release);
    let mut original = client
        .get(format!("http://{}/source", server.addr()))
        .header("x-request", "blocked")
        .build()
        .unwrap();
    original
        .extensions_mut()
        .insert(BeforeRedirect::new(move |request| {
            let blocked = request.headers()["x-request"] == "blocked";
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&callback_release);
            async move {
                if blocked {
                    entered.notify_one();
                    release.notified().await;
                }
            }
        }));
    let mut cloned = original
        .try_clone()
        .expect("buffered request can be cloned");
    cloned
        .headers_mut()
        .insert("x-request", header::HeaderValue::from_static("timed"));

    let timed = client.execute(cloned);
    tokio::pin!(timed);
    tokio::select! {
        () = timed_at_destination.notified() => {},
        result = &mut timed => panic!("cloned request must reach its destination: {result:?}"),
    }
    let blocked = client.execute(original);
    tokio::pin!(blocked);
    tokio::select! {
        () = blocked_callback.notified() => {},
        result = &mut timed => panic!("cloned request ended before the other callback blocked: {result:?}"),
        result = &mut blocked => panic!("original request ended before its callback blocked: {result:?}"),
    }

    let error = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::select! {
            result = &mut timed => result.expect_err("the silent destination must time out"),
            result = &mut blocked => panic!("the other request's callback is still blocked: {result:?}"),
        }
    })
    .await
    .expect("one clone's callback must not pause another execution's deadline");
    assert!(error.is_timeout());

    release.notify_one();
    let response = tokio::time::timeout(Duration::from_secs(3), blocked)
        .await
        .expect("releasing the callback must resume the original request")
        .expect("the original request retains its own transport budget");
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn dropping_request_drops_pending_redirect_callback() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct MarkDropped(Arc<AtomicBool>);
    impl Drop for MarkDropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let destinations = Arc::new(AtomicUsize::new(0));
    let observed_destinations = Arc::clone(&destinations);
    let server = server::http(move |request| {
        let destinations = Arc::clone(&observed_destinations);
        async move {
            if request.uri().path() == "/source" {
                http::Response::builder()
                    .status(307)
                    .header(header::LOCATION, "/destination")
                    .body(Body::default())
                    .unwrap()
            } else {
                destinations.fetch_add(1, Ordering::SeqCst);
                http::Response::new(Body::default())
            }
        }
    });
    let entered = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let callback_entered = Arc::clone(&entered);
    let callback_dropped = Arc::clone(&dropped);
    let client = Client::builder()
        .no_proxy()
        .redirect(Policy::limited(10))
        .build()
        .unwrap();
    let mut request = client
        .get(format!("http://{}/source", server.addr()))
        .build()
        .unwrap();
    request
        .extensions_mut()
        .insert(BeforeRedirect::new(move |_| {
            let entered = Arc::clone(&callback_entered);
            let guard = MarkDropped(Arc::clone(&callback_dropped));
            async move {
                let _guard = guard;
                entered.notify_one();
                std::future::pending::<()>().await;
            }
        }));
    {
        let send = client.execute(request);
        tokio::pin!(send);
        tokio::select! {
            () = entered.notified() => {},
            result = &mut send => panic!("request completed before cancellation: {result:?}"),
        }
    }
    assert!(dropped.load(Ordering::SeqCst));
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(destinations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn before_redirect_observes_prepared_head_and_is_awaited() {
    for (code, method, destination_method, retains_body) in [
        (301u16, http::Method::POST, "GET", false),
        (302, http::Method::POST, "GET", false),
        (303, http::Method::POST, "GET", false),
        (307, http::Method::POST, "POST", true),
        (308, http::Method::POST, "POST", true),
        (301, http::Method::OPTIONS, "OPTIONS", true),
        (302, http::Method::OPTIONS, "OPTIONS", true),
        (303, http::Method::OPTIONS, "GET", false),
    ] {
        let events = Arc::new(Mutex::new(Vec::new()));
        let destination_events = Arc::clone(&events);
        let destination = server::http(move |req| {
            let events = Arc::clone(&destination_events);
            async move {
                events.lock().unwrap().push("destination");
                assert_eq!(req.method(), destination_method);
                assert!(!req.headers().contains_key(header::AUTHORIZATION));
                assert!(!req.headers().contains_key(header::COOKIE));
                assert_eq!(
                    req.headers().contains_key(header::CONTENT_TYPE),
                    retains_body
                );
                let body = req.into_body().collect().await.unwrap().to_bytes();
                assert_eq!(
                    body.as_ref(),
                    if retains_body {
                        b"hello".as_slice()
                    } else {
                        b""
                    }
                );
                http::Response::new(Body::default())
            }
        });
        let destination_uri = format!("http://{}/destination", destination.addr());
        let location = destination_uri.clone();
        let source_events = Arc::clone(&events);
        let source = server::http(move |_| {
            let location = location.clone();
            let events = Arc::clone(&source_events);
            async move {
                events.lock().unwrap().push("source");
                http::Response::builder()
                    .status(code)
                    .header(header::LOCATION, location)
                    .body(Body::default())
                    .unwrap()
            }
        });
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let callback_entered = Arc::clone(&entered);
        let callback_release = Arc::clone(&release);
        let callback_events = Arc::clone(&events);
        let hook = BeforeRedirect::new(move |request| {
            assert_eq!(request.uri(), destination_uri.as_str());
            assert_eq!(request.method(), destination_method);
            for name in [header::AUTHORIZATION, header::COOKIE] {
                assert!(!request.headers().contains_key(name));
            }
            for name in [
                header::CONTENT_TYPE,
                header::CONTENT_LENGTH,
                header::CONTENT_ENCODING,
            ] {
                assert_eq!(request.headers().contains_key(name), retains_body);
            }
            assert_eq!(request.headers()["x-explicit"], "retained");
            let entered = Arc::clone(&callback_entered);
            let release = Arc::clone(&callback_release);
            let events = Arc::clone(&callback_events);
            async move {
                events.lock().unwrap().push("callback");
                entered.notify_one();
                release.notified().await;
            }
        });
        let client = Client::builder()
            .no_proxy()
            .redirect(Policy::limited(10))
            .build()
            .unwrap();
        let mut request = client
            .request(method, format!("http://{}/source", source.addr()))
            .header(header::AUTHORIZATION, "Bearer example")
            .header(header::COOKIE, "example=value")
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::CONTENT_ENCODING, "identity")
            .header(header::CONTENT_LENGTH, "5")
            .header("x-explicit", "retained")
            .body("hello")
            .build()
            .unwrap();
        request.extensions_mut().insert(hook);
        let send = client.execute(request);
        tokio::pin!(send);
        tokio::select! {
            () = entered.notified() => {},
            result = &mut send => panic!("request completed before callback release: {result:?}"),
        }
        assert_eq!(*events.lock().unwrap(), ["source", "callback"]);
        release.notify_one();
        assert_eq!(send.await.unwrap().status(), StatusCode::OK);
        assert_eq!(
            *events.lock().unwrap(),
            ["source", "callback", "destination"]
        );
    }
}

#[tokio::test]
async fn before_redirect_respects_request_policy_and_limit() {
    let server = server::http(|_| async {
        http::Response::builder()
            .status(307)
            .header(header::LOCATION, "/next")
            .body(Body::default())
            .unwrap()
    });
    let client = Client::builder()
        .no_proxy()
        .redirect(Policy::limited(10))
        .build()
        .unwrap();
    for (policy, expected_callbacks, is_error) in [
        (Policy::none(), 0, false),
        (Policy::limited(0), 0, true),
        (Policy::limited(1), 1, true),
    ] {
        let calls = Arc::new(Mutex::new(0));
        let callback_calls = Arc::clone(&calls);
        let mut request = client
            .get(format!("http://{}/source", server.addr()))
            .redirect(policy)
            .build()
            .unwrap();
        request
            .extensions_mut()
            .insert(BeforeRedirect::new(move |_| {
                *callback_calls.lock().unwrap() += 1;
                std::future::ready(())
            }));
        let result = client.execute(request).await;
        if is_error {
            assert!(result.unwrap_err().is_redirect());
        } else {
            assert_eq!(result.unwrap().status(), StatusCode::TEMPORARY_REDIRECT);
        }
        assert_eq!(*calls.lock().unwrap(), expected_callbacks);
    }
}

#[tokio::test]
async fn test_redirect_301_and_302_and_303_changes_post_to_get() {
    let codes = [301u16, 302, 303];

    for &code in &codes {
        let redirect = server::http(move |req| async move {
            if req.method() == "POST" {
                assert_eq!(req.uri(), &*format!("/{code}"));
                http::Response::builder()
                    .status(code)
                    .header(header::LOCATION, "/dst")
                    .header(header::SERVER, "test-redirect")
                    .body(Body::default())
                    .unwrap()
            } else {
                assert_eq!(req.method(), "GET");

                http::Response::builder()
                    .header(header::SERVER, "test-dst")
                    .body(Body::default())
                    .unwrap()
            }
        });

        let url = format!("http://{}/{}", redirect.addr(), code);
        let dst = format!("http://{}/{}", redirect.addr(), "dst");
        let res = wreq::post(&url)
            .redirect(Policy::default())
            .send()
            .await
            .unwrap();
        assert_eq!(res.uri(), dst.as_str());
        assert_eq!(res.status(), wreq::StatusCode::OK);
        assert_eq!(res.headers().get(header::SERVER).unwrap(), &"test-dst");
    }
}

#[tokio::test]
async fn test_redirect_307_and_308_tries_to_get_again() {
    let codes = [307u16, 308];
    for &code in &codes {
        let redirect = server::http(move |req| async move {
            assert_eq!(req.method(), "GET");
            if req.uri() == &*format!("/{code}") {
                http::Response::builder()
                    .status(code)
                    .header(header::LOCATION, "/dst")
                    .header(header::SERVER, "test-redirect")
                    .body(Body::default())
                    .unwrap()
            } else {
                assert_eq!(req.uri(), "/dst");

                http::Response::builder()
                    .header(header::SERVER, "test-dst")
                    .body(Body::default())
                    .unwrap()
            }
        });

        let url = format!("http://{}/{}", redirect.addr(), code);
        let dst = format!("http://{}/{}", redirect.addr(), "dst");
        let res = wreq::get(&url)
            .redirect(Policy::default())
            .send()
            .await
            .unwrap();
        assert_eq!(res.uri(), dst.as_str());
        assert_eq!(res.status(), wreq::StatusCode::OK);
        assert_eq!(res.headers().get(header::SERVER).unwrap(), &"test-dst");
    }
}

#[tokio::test]
async fn test_redirect_307_and_308_tries_to_post_again() {
    let _ = pretty_env_logger::env_logger::try_init();

    let codes = [307u16, 308];
    for &code in &codes {
        let redirect = server::http(move |mut req| async move {
            assert_eq!(req.method(), "POST");
            assert_eq!(req.headers()[header::CONTENT_LENGTH], "5");

            let data = req
                .body_mut()
                .frame()
                .await
                .unwrap()
                .unwrap()
                .into_data()
                .unwrap();
            assert_eq!(&*data, b"Hello");

            if req.uri() == &*format!("/{code}") {
                http::Response::builder()
                    .status(code)
                    .header(header::LOCATION, "/dst")
                    .header(header::SERVER, "test-redirect")
                    .body(Body::default())
                    .unwrap()
            } else {
                assert_eq!(req.uri(), "/dst");

                http::Response::builder()
                    .header(header::SERVER, "test-dst")
                    .body(Body::default())
                    .unwrap()
            }
        });

        let url = format!("http://{}/{}", redirect.addr(), code);
        let dst = format!("http://{}/{}", redirect.addr(), "dst");
        let res = wreq::post(&url)
            .redirect(Policy::default())
            .body("Hello")
            .send()
            .await
            .unwrap();
        assert_eq!(res.uri(), dst.as_str());
        assert_eq!(res.status(), wreq::StatusCode::OK);
        assert_eq!(res.headers().get(header::SERVER).unwrap(), &"test-dst");
    }
}

#[tokio::test]
async fn test_redirect_removes_sensitive_headers() {
    let end_server = server::http(move |req| async move {
        match req.uri().path() {
            "/middle" => {
                assert_eq!(req.headers().get(header::COOKIE), None);
                http::Response::builder()
                    .status(302)
                    .header(header::LOCATION, "/end")
                    .body(Body::default())
                    .unwrap()
            }
            "/end" => {
                assert_eq!(req.headers().get(header::COOKIE), None);
                http::Response::default()
            }
            path => panic!("unexpected redirect path: {path}"),
        }
    });

    let end_addr = end_server.addr();

    let start_server = server::http(move |req| async move {
        assert_eq!(req.headers()[header::COOKIE], "foo=bar");
        http::Response::builder()
            .status(302)
            .header(header::LOCATION, format!("http://{end_addr}/middle"))
            .body(Body::default())
            .unwrap()
    });

    let response = Client::builder()
        .redirect(Policy::default())
        .build()
        .unwrap()
        .get(format!("http://{}/sensitive", start_server.addr()))
        .header(header::COOKIE, header::HeaderValue::from_static("foo=bar"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.uri().path(), "/end");
}

#[tokio::test]
async fn test_redirect_referrer_policy() {
    let server = server::http(move |req| async move {
        match req.uri().path() {
            "/direct-start" => http::Response::builder()
                .status(302)
                .header(header::LOCATION, "/direct-end")
                .body(Body::default())
                .unwrap(),
            "/direct-end" => {
                assert_eq!(req.headers().get(header::REFERER), None);
                http::Response::default()
            }
            "/no-referrer-start" => http::Response::builder()
                .status(302)
                .header(header::LOCATION, "/no-referrer-end")
                .header(header::REFERRER_POLICY, "no-referrer")
                .body(Body::default())
                .unwrap(),
            "/no-referrer-end" => {
                assert_eq!(req.headers().get(header::REFERER), None);
                http::Response::default()
            }
            "/origin-start" => http::Response::builder()
                .status(302)
                .header(header::LOCATION, "/origin-end")
                .header(
                    header::REFERRER_POLICY,
                    "unknown-policy, origin-when-cross-origin",
                )
                .body(Body::default())
                .unwrap(),
            "/origin-end" => {
                assert_eq!(req.headers()[header::REFERER], "http://source.example/");
                http::Response::default()
            }
            "/clear-start" => http::Response::builder()
                .status(302)
                .header(header::LOCATION, "/clear-middle")
                .header(header::REFERRER_POLICY, "no-referrer")
                .body(Body::default())
                .unwrap(),
            "/clear-middle" => {
                assert_eq!(req.headers().get(header::REFERER), None);
                http::Response::builder()
                    .status(302)
                    .header(header::LOCATION, "/clear-end")
                    .header(header::REFERRER_POLICY, "unsafe-url")
                    .body(Body::default())
                    .unwrap()
            }
            "/clear-end" => {
                assert_eq!(req.headers().get(header::REFERER), None);
                http::Response::default()
            }
            path => panic!("unexpected redirect path: {path}"),
        }
    });

    let client = Client::builder()
        .redirect(Policy::default())
        .build()
        .unwrap();
    let base = format!("http://{}", server.addr());

    let cases = [
        ("/direct-start", "/direct-end", None),
        (
            "/no-referrer-start",
            "/no-referrer-end",
            Some("https://source.example/private?q=1"),
        ),
        (
            "/origin-start",
            "/origin-end",
            Some("http://source.example/private?q=1"),
        ),
        (
            "/clear-start",
            "/clear-end",
            Some("http://source.example/private"),
        ),
    ];

    for (start, end, referer) in cases {
        let mut request = client.get(format!("{base}{start}"));
        if let Some(referer) = referer {
            request = request.header(header::REFERER, referer);
        }

        let response = request.send().await.unwrap();
        assert_eq!(response.uri().path(), end, "case: {start}");
    }
}

#[tokio::test]
async fn test_redirect_policy_can_return_errors() {
    let server = server::http(move |req| async move {
        assert_eq!(req.uri(), "/loop");
        http::Response::builder()
            .status(302)
            .header(header::LOCATION, "/loop")
            .body(Body::default())
            .unwrap()
    });

    let url = format!("http://{}/loop", server.addr());
    let err = wreq::get(&url)
        .redirect(Policy::default())
        .send()
        .await
        .unwrap_err();
    assert!(err.is_redirect());
}

#[tokio::test]
async fn test_redirect_policy_can_stop_redirects_without_an_error() {
    let server = server::http(move |req| async move {
        assert_eq!(req.uri(), "/no-redirect");
        http::Response::builder()
            .status(302)
            .header(header::LOCATION, "/dont")
            .body(Body::default())
            .unwrap()
    });

    let url = format!("http://{}/no-redirect", server.addr());

    let res = Client::builder()
        .redirect(Policy::none())
        .build()
        .unwrap()
        .get(&url)
        .send()
        .await
        .unwrap();

    assert_eq!(res.uri(), url.as_str());
    assert_eq!(res.status(), wreq::StatusCode::FOUND);
}

#[tokio::test]
async fn test_referer_is_preserved_if_disabled() {
    let server = server::http(move |req| async move {
        if req.uri() == "/no-refer" {
            http::Response::builder()
                .status(302)
                .header(header::LOCATION, "/dst")
                .body(Body::default())
                .unwrap()
        } else {
            assert_eq!(req.uri(), "/dst");
            assert_eq!(
                req.headers()[header::REFERER],
                "https://source.example/private?q=1"
            );

            http::Response::default()
        }
    });

    let response = Client::builder()
        .redirect(Policy::default())
        .referer(false)
        .build()
        .unwrap()
        .get(format!("http://{}/no-refer", server.addr()))
        .header(
            header::REFERER,
            header::HeaderValue::from_static("https://source.example/private?q=1"),
        )
        .send()
        .await
        .unwrap();

    assert_eq!(response.uri().path(), "/dst");
}

#[tokio::test]
async fn test_invalid_location_stops_redirect_gh484() {
    let server = server::http(move |_req| async move {
        http::Response::builder()
            .status(302)
            .header(header::LOCATION, "http://www.yikes{KABOOM}")
            .body(Body::default())
            .unwrap()
    });

    let url = format!("http://{}/yikes", server.addr());

    let res = wreq::get(&url).send().await.unwrap();

    assert_eq!(res.uri(), url.as_str());
    assert_eq!(res.status(), wreq::StatusCode::FOUND);
}

#[tokio::test]
async fn test_invalid_scheme_is_rejected() {
    let server = server::http(move |_req| async move {
        http::Response::builder()
            .status(302)
            .header(header::LOCATION, "htt://www.yikes.com/")
            .body(Body::default())
            .unwrap()
    });

    let url = format!("http://{}/yikes", server.addr());

    let err = wreq::get(&url)
        .redirect(Policy::default())
        .send()
        .await
        .unwrap_err();
    assert!(err.is_builder());
}

#[cfg(feature = "cookies")]
#[tokio::test]
async fn test_redirect_302_with_set_cookies() {
    let code = 302;
    let server = server::http(move |req| async move {
        if req.uri() == "/302" {
            http::Response::builder()
                .status(302)
                .header(header::LOCATION, "/dst")
                .header(header::SET_COOKIE, "key=value")
                .body(Body::default())
                .unwrap()
        } else {
            assert_eq!(req.uri(), "/dst");
            assert_eq!(req.headers()[header::COOKIE], "key=value");
            http::Response::default()
        }
    });

    let url = format!("http://{}/{}", server.addr(), code);
    let dst = format!("http://{}/{}", server.addr(), "dst");

    let client = Client::builder()
        .cookie_store(true)
        .redirect(Policy::default())
        .build()
        .unwrap();
    let res = client.get(&url).send().await.unwrap();

    assert_eq!(res.uri(), dst.as_str());
    assert_eq!(res.status(), wreq::StatusCode::OK);
}

#[tokio::test]
async fn test_redirect_limit_to_1() {
    let server = server::http(move |req| async move {
        let i: i32 = req
            .uri()
            .path()
            .rsplit('/')
            .next()
            .unwrap()
            .parse::<i32>()
            .unwrap();
        assert!(req.uri().path().ends_with(&format!("/redirect/{i}")));
        http::Response::builder()
            .status(302)
            .header(header::LOCATION, format!("/redirect/{}", i + 1))
            .body(Body::default())
            .unwrap()
    });
    // The number at the end of the uri indicates the total number of redirections
    let url = format!("http://{}/redirect/0", server.addr());

    let client = Client::builder()
        .redirect(Policy::limited(1))
        .build()
        .unwrap();
    let res = client.get(&url).send().await.unwrap_err();
    // If the maximum limit is 1, then the final uri should be /redirect/1
    assert_eq!(
        res.uri().unwrap().to_string(),
        format!("http://{}/redirect/1", server.addr()).as_str()
    );
    assert!(res.is_redirect());
}

#[tokio::test]
async fn test_scheme_only_check_after_policy_return_follow() {
    let server = server::http(move |_| async move {
        http::Response::builder()
            .status(302)
            .header(header::LOCATION, "htt://www.yikes.com/")
            .body(Body::default())
            .unwrap()
    });

    let url = format!("http://{}/yikes", server.addr());
    let res = Client::builder()
        .redirect(Policy::custom(|attempt| attempt.stop()))
        .build()
        .unwrap()
        .get(&url)
        .send()
        .await;

    assert!(res.is_ok());
    assert_eq!(res.unwrap().status(), wreq::StatusCode::FOUND);

    let res = Client::builder()
        .redirect(Policy::custom(|attempt| attempt.follow()))
        .build()
        .unwrap()
        .get(&url)
        .send()
        .await;

    assert!(res.is_err());
    assert!(res.unwrap_err().is_builder());
}

#[tokio::test]
async fn test_redirect_301_302_303_empty_payload_headers() {
    let codes = [301u16, 302, 303];
    for &code in &codes {
        let redirect = server::http(move |mut req| async move {
            if req.method() == "POST" {
                let data = req
                    .body_mut()
                    .frame()
                    .await
                    .unwrap()
                    .unwrap()
                    .into_data()
                    .unwrap();

                assert_eq!(&*data, b"Hello");
                if req.headers().get(header::CONTENT_LENGTH).is_some() {
                    assert_eq!(req.headers()[header::CONTENT_LENGTH], "5");
                }
                assert_eq!(req.uri(), &*format!("/{code}"));

                http::Response::builder()
                    .header(header::LOCATION, "/dst")
                    .header(header::SERVER, "test-dst")
                    .status(code)
                    .body(Body::default())
                    .unwrap()
            } else {
                assert_eq!(req.method(), "GET");
                assert!(req.headers().get(header::CONTENT_TYPE).is_none());
                assert!(req.headers().get(header::CONTENT_LENGTH).is_none());
                assert!(req.headers().get(header::CONTENT_ENCODING).is_none());
                http::Response::builder()
                    .header(header::SERVER, "test-dst")
                    .body(Body::default())
                    .unwrap()
            }
        });

        let url = format!("http://{}/{}", redirect.addr(), code);
        let dst = format!("http://{}/{}", redirect.addr(), "dst");
        let res = wreq::post(&url)
            .redirect(Policy::default())
            .body("Hello")
            .header(header::CONTENT_TYPE, "text/plain")
            .header(header::CONTENT_LENGTH, "5")
            .header(header::CONTENT_ENCODING, "identity")
            .send()
            .await
            .unwrap();
        assert_eq!(res.uri(), dst.as_str());
        assert_eq!(res.status(), 200);
        assert_eq!(res.headers().get(header::SERVER).unwrap(), &"test-dst");
    }
}

#[tokio::test]
async fn test_redirect_history() {
    let redirect = server::http(move |req| async move {
        if req.uri() == "/first" {
            http::Response::builder()
                .status(302)
                .header(header::LOCATION, "/second")
                .body(Body::default())
                .unwrap()
        } else if req.uri() == "/second" {
            http::Response::builder()
                .status(302)
                .header(header::LOCATION, "/dst")
                .body(Body::default())
                .unwrap()
        } else {
            assert_eq!(req.uri(), "/dst");

            http::Response::builder()
                .header(header::SERVER, "test-dst")
                .body(Body::default())
                .unwrap()
        }
    });

    let url = format!("http://{}/first", redirect.addr());
    let dst = format!("http://{}/{}", redirect.addr(), "dst");

    let client = Client::builder()
        .redirect(Policy::default())
        .build()
        .unwrap();

    let res = client.get(&url).send().await.unwrap();
    assert_eq!(res.uri(), dst.as_str());
    assert_eq!(res.status(), wreq::StatusCode::OK);
    assert_eq!(res.headers().get(header::SERVER).unwrap(), &"test-dst");

    let mut history = res.extensions().get::<History>().unwrap().into_iter();

    let next1 = history.next().unwrap();
    assert_eq!(next1.status, 302);
    assert_eq!(next1.previous.path(), "/first");
    assert_eq!(next1.uri.path(), "/second");
    assert_eq!(next1.headers[header::LOCATION], "/second");

    let next2 = history.next().unwrap();
    assert_eq!(next2.status, 302);
    assert_eq!(next2.previous.path(), "/second");
    assert_eq!(next2.uri.path(), "/dst");
    assert_eq!(next2.headers[header::LOCATION], "/dst");

    assert!(history.next().is_none());
}

#[cfg(feature = "cookies")]
#[tokio::test]
async fn test_redirect_applies_set_cookie_from_redirect() {
    let server = server::http(move |req| async move {
        match req.uri().path() {
            "/start" => http::Response::builder()
                .status(302)
                .header(header::LOCATION, "/dst")
                .header(header::SET_COOKIE, "session=abc; Path=/")
                .body(Body::default())
                .unwrap(),
            "/dst" => {
                assert_eq!(req.headers()[header::COOKIE], "session=abc");
                http::Response::builder()
                    .status(200)
                    .body(Body::default())
                    .unwrap()
            }
            _ => http::Response::builder()
                .status(404)
                .body(Body::default())
                .unwrap(),
        }
    });

    let start = format!("http://{}/start", server.addr());
    let dst = format!("http://{}/dst", server.addr());

    let client = Client::builder()
        .cookie_store(true)
        .redirect(Policy::default())
        .build()
        .unwrap();

    let res = client.get(&start).send().await.unwrap();
    assert_eq!(res.uri(), dst.as_str());
    assert_eq!(res.status(), wreq::StatusCode::OK);
}

#[tokio::test]
async fn test_redirect_async_pending_follow() {
    let server = server::http(move |req| async move {
        if req.uri() == "/async-redirect" {
            http::Response::builder()
                .status(302)
                .header(header::LOCATION, "/dst")
                .body(Body::default())
                .unwrap()
        } else {
            assert_eq!(req.uri(), "/dst");
            http::Response::builder()
                .header(header::SERVER, "test-dst")
                .body(Body::default())
                .unwrap()
        }
    });

    let url = format!("http://{}/async-redirect", server.addr());
    let dst = format!("http://{}/dst", server.addr());

    let client = Client::builder()
        .redirect(Policy::custom(|attempt| {
            attempt.pending(|attempt| async move {
                // Simulate async decision-making
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                attempt.follow()
            })
        }))
        .build()
        .unwrap();

    let res = client.get(&url).send().await.unwrap();
    assert_eq!(res.uri(), dst.as_str());
    assert_eq!(res.status(), wreq::StatusCode::OK);
    assert_eq!(res.headers().get(header::SERVER).unwrap(), &"test-dst");
}

#[tokio::test]
async fn test_redirect_utf8_location_is_encoded() {
    let server = server::http(move |req| async move {
        if req.uri() == "/start" {
            http::Response::builder()
                .status(302)
                .header(
                    header::LOCATION,
                    header::HeaderValue::from_bytes("/café".as_bytes()).unwrap(),
                )
                .body(Body::default())
                .unwrap()
        } else {
            assert_eq!(req.uri(), "/caf%C3%A9");
            http::Response::builder()
                .status(StatusCode::OK)
                .body(Body::from("OK"))
                .unwrap()
        }
    });

    let url = format!("http://{}/start", server.addr());
    let dst = format!("http://{}/caf%C3%A9", server.addr());

    let res = wreq::get(&url)
        .redirect(Policy::default())
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.uri(), dst.as_str());
    assert_eq!(res.text().await.unwrap(), "OK");
}

#[tokio::test]
async fn test_redirect_location_is_encoded() {
    let server = server::http(move |req| async move {
        if req.uri() == "/start" {
            http::Response::builder()
                .status(302)
                .header(header::LOCATION, "/dst path")
                .body(wreq::Body::default())
                .unwrap()
        } else {
            assert_eq!(req.uri().path(), "/dst%20path");
            http::Response::builder()
                .status(StatusCode::OK)
                .body(wreq::Body::default())
                .unwrap()
        }
    });

    let url = format!("http://{}/start", server.addr());
    let dst = format!("http://{}/dst%20path", server.addr());

    let client = Client::builder()
        .redirect(Policy::default())
        .build()
        .unwrap();

    let res = client.get(&url).send().await.unwrap();
    assert_eq!(res.uri(), dst.as_str());
    assert_eq!(res.status(), StatusCode::OK);
}
