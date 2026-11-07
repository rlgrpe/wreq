# wreq

[![CI](https://github.com/0x676e67/wreq/actions/workflows/ci.yml/badge.svg)](https://github.com/0x676e67/wreq/actions/workflows/ci.yml)
[![Crates.io License](https://img.shields.io/crates/l/wreq)](https://github.com/0x676e67/wreq/blob/main/LICENSE)
[![Crates.io MSRV](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fcrates.io%2Fapi%2Fv1%2Fcrates%2Fwreq%2Fversions%3Fsort%3Ddate%26per_page%3D1&query=%24.versions%5B0%5D.rust_version&suffix=.0&label=MSRV&color=blue&logo=rust)](https://crates.io/crates/wreq)
[![crates.io](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fcrates.io%2Fapi%2Fv1%2Fcrates%2Fwreq&query=%24.crate.max_stable_version&prefix=v&label=crates.io&color=orange&logo=rust)](https://crates.io/crates/wreq)
[![Discord chat][discord-badge]][discord-url]

[discord-badge]: https://img.shields.io/discord/1486741856397164788.svg?logo=discord
[discord-url]: https://discord.gg/rfbvyFkgq3

> 🚀 Help me work seamlessly with open source sharing by [sponsoring me on GitHub](https://github.com/0x676e67/0x676e67/blob/main/SPONSOR.md)

An ergonomic and modular Rust HTTP Client for high-fidelity protocol matching, featuring customizable TLS, JA3/JA4, and HTTP/2 signature capabilities.

## Features

- Plain bodies, JSON, urlencoded, multipart
- HTTP Trailer
- Cookie Store
- Redirect Policy
- Original Header
- Rotating Proxies
- Tower Middleware
- WebSocket Upgrade
- HTTPS via BoringSSL
- HTTP/2 over TLS Parity
- Certificate Store (CAs & mTLS)
- Multiple Runtime (Tokio, Compio)

## Example

This example uses Tokio and the tagged fork without a consumer `[patch]` table:

```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
wreq = { version = "0.16.1", git = "https://github.com/rlgrpe/wreq", tag = "v0.16.1+rlgrpe.1" }
```

And then the code:

```rust
use wreq::Client;

#[tokio::main]
async fn main() -> wreq::Result<()> {
    // Build a client
    let client = Client::builder().build()?;

    // Use the API you're already familiar with
    let resp = client.get("https://pingly.us.kg/api/all").send().await?;
    println!("{}", resp.text().await?);
    Ok(())
}
```

## Fork provenance and dependencies

This patch retains package version `0.16.1` and builds on upstream commit
`d2268c4d54ee40a518066c92f30bab9516764663`. It adds typed transport causes,
cross-origin authorization handling, and request-owned connection setup.

The manifest directly pins `http2` to `rlgrpe/http2` tag `v0.5.20+rlgrpe.1`
and both `wreq-proto` and `wreq-rt` to `rlgrpe/wreq-proto` tag
`v0.2.6+rlgrpe.1`. The three TLS packages use the same upstream btls revision,
`0f5416d1f61eb4f64a9f2f4d8b867495305d61c1`. Their sibling dependencies share
those sources without relying on root patches.

An emulation utility used with this fork must depend on the same wreq Git source
and tag. A registry wreq dependency is a distinct source even when its package
version is identical; do not mix their public types.

The lower-level releases document accepted Rust 1.98 Clippy baselines:
[30 http2 diagnostics](https://github.com/rlgrpe/http2/blob/v0.5.20%2Brlgrpe.1/CHANGELOG.md)
and [five protocol diagnostics](https://github.com/rlgrpe/wreq-proto/blob/v0.2.6%2Brlgrpe.1/wreq-proto/CHANGELOG.md).
Their strict lint checks did not pass. All reported statements were compared
against the corresponding upstream or published source, and CI and lint rules
remain unchanged. This exception does not cover new diagnostics.

## Behavior

- **HTTP/1 over TLS**

In the Rust ecosystem, most HTTP clients rely on the [http](https://github.com/hyperium/http) library, which performs well but does not preserve header case. This causes some **WAFs** to reject **HTTP/1** requests with lowercase headers (see [discussion](https://github.com/seanmonstar/reqwest/discussions/2227)). **wreq** addresses this by fully supporting **HTTP/1** header case sensitivity.

- **HTTP/2 over TLS**

Due to the complexity of **TLS** encryption and the widespread adoption of **HTTP/2**, browser fingerprints such as **JA3**, **JA4**, and **Akamai** cannot be reliably emulated using simple fingerprint strings. Instead of parsing and emulating these string-based fingerprints, **wreq** provides fine-grained control over **TLS** and **HTTP/2** extensions and settings for precise browser behavior emulation.

- **Device Emulation**

**TLS** and **HTTP/2** fingerprints are often identical across various browser models because these underlying protocols evolve slower than browser release cycles. **100+ browser device emulation profiles** are maintained in [wreq-util](https://github.com/0x676e67/wreq-util).

- **Connection setup cancellation**

Connection setup belongs to the request waiting for it. If a pooled connection becomes available first, the unused connection attempt is canceled rather than completed in the background. Canceling a request during setup also cancels that attempt. Other requests waiting for the same HTTP/2 connection can resume connection acquisition without replaying a sent request. This applies to both Tokio and Compio and does not require a configured connection timeout.

## Building

Compiling alongside **openssl-sys** can cause symbol conflicts with **boringssl** that lead to [link failures](https://github.com/cloudflare/boring/issues/197), and on **Linux** and **Android** this can be avoided by enabling the **`prefix-symbols`** feature.

Install [BoringSSL build dependencies](https://github.com/google/boringssl/blob/master/BUILDING.md#build-prerequisites) and build with:

```bash
sudo apt-get install build-essential cmake perl pkg-config libclang-dev musl-tools git -y
cargo build --release
```

This GitHub Actions [workflow](.github/compilation-guide/build.yml) can be used to compile the project on **Linux**, **Windows**, and **macOS**.

## Services

Help sustain the ongoing development of this open-source project by reaching out for [commercial support](mailto:gngppz@gmail.com). Receive private guidance, expert reviews, or direct access to the maintainer, with personalized technical assistance tailored to your needs.

## License

Licensed under either of Apache License, Version 2.0 ([LICENSE](./LICENSE) or http://www.apache.org/licenses/LICENSE-2.0).

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the [Apache-2.0](./LICENSE) license, shall be licensed as above, without any additional terms or conditions.

## Sponsors

<a href="https://captcha.fun/?utm_source=github&utm_medium=readme&utm_campaign=wreq" target="_blank"><img src="https://www.captcha.fun/banner.jpg" height="47" width="149"></a>

**Solve reCAPTCHA in less than 2 seconds**

**[Captcha.fun](https://captcha.fun/?utm_source=github&utm_medium=readme&utm_campaign=wreq)** delivers fast, reliable CAPTCHA solving built for automation at scale.

With simple API integration, consistent performance, and competitive pricing, it's an easy way to keep your workflows moving without delays—use code **`WREQ`** for **10% bonus credits**.

**[Dashboard](https://dash.captcha.fun/)** | **[Docs](http://docs.captcha.fun/)** | **[Discord](https://discord.gg/captchafun)**

---

<a href="https://byteful.com/?utm_source=github_rust&utm_medium=github-sponsor&utm_campaign=wreq_github_sponsorship" target="_blank"><img src="./.github/assets/byteful-logo.svg" width="149" height="47" alt="Byteful"></a>

**35M+ residential IPs with 99.9% success rates**

UK-based **[Byteful](https://byteful.com/?utm_source=github_rust&utm_medium=github-sponsor&utm_campaign=wreq_github_sponsorship)** provides ethically sourced residential, mobile, static residential (ISP), and datacenter proxies. Its API-first scraping, data collection, and AI automation tools handle tens of billions of monthly requests.

With free geo-targeting and ~0.5s response times, Byteful helps you collect data at scale. Use code **`WREQ10`** for **10% off residential proxies**.

**[Dashboard](https://dashboard.byteful.com/)** | **[Docs](https://documentation.byteful.com/)** | **[Discord](https://discord.com/invite/ping-proxies-584021352940568578)**

---

<a href="https://hypersolutions.co/?utm_source=github&utm_medium=readme&utm_campaign=wreq" target="_blank"><img src="https://raw.githubusercontent.com/0x676e67/wreq/main/.github/assets/hypersolutions.jpg" height="47" width="149"></a>

TLS fingerprinting alone isn't enough for modern bot protection. **[Hyper Solutions](https://hypersolutions.co?utm_source=github&utm_medium=readme&utm_campaign=wreq)** provides the missing piece - API endpoints that generate valid antibot tokens for:

**Akamai** • **DataDome** • **Kasada** • **Incapsula**

No browser automation. Just simple API calls that return the exact cookies and headers these systems require.

**[Dashboard](https://hypersolutions.co?utm_source=github&utm_medium=readme&utm_campaign=wreq)** | **[Docs](https://docs.justhyped.dev)** | **[Discord](https://discord.gg/akamai)**

## Accolades

A hard fork of [reqwest](https://github.com/seanmonstar/reqwest).
