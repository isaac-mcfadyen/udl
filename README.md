# udl - Upload DownLoad

A simple tool to upload and download files from Cloudflare R2.

In contrast to tools like `rclone`, this CLI needs little-to-no setup.
A remote Worker is deployed to Cloudflare and linked to an R2 bucket.
After installing this tool, simply provide the URL and the auth token to send or receive files.

One use-case is for quick transfers from ephemeral environments, like a short-lived LambdaLabs VM or even a GitHub Actions runner.

## Usage

First, deploy the Worker.

```sh
# Clone and install dependencies.
git clone git@github.com:isaac-mcfadyen/udl.git && cd udl/worker && npm i
# Modify the wrangler.json to add your own Worker name and zone, then...
npm run deploy
# Save the auth key, you'll need it later.
npx wrangler secret put AUTH_KEY
```

Then, on any machine that needs to access files, [install Rust](https://www.rust-lang.org/tools/install) and install the CLI:

```bash
cargo install --locked --git https://github.com/isaac-mcfadyen/udl.git
```

To upload a file:

```bash
udl --url <WORKER URL> --key <AUTH KEY> upload <NAME> <PATH>
```

To download a file:

```bash
udl --url <WORKER URL> --key <AUTH KEY> download <NAME> <PATH>
```
