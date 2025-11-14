mod publisher;
mod subscriber;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: {} <publisher|subscriber>", args[0]);
        eprintln!();
        eprintln!("To reproduce the bug:");
        eprintln!("  Terminal 1: cargo run -p moq-relay");
        eprintln!("  Terminal 2: cargo run -p bug-reproducer publisher");
        eprintln!("  Terminal 3: cargo run -p bug-reproducer subscriber");
        eprintln!();
        eprintln!("The subscriber will disconnect on UNPATCHED relay.");
        std::process::exit(1);
    }

    match args[1].as_str() {
        "publisher" => publisher::run().await,
        "subscriber" => subscriber::run().await,
        _ => {
            eprintln!("Error: argument must be 'publisher' or 'subscriber'");
            std::process::exit(1);
        }
    }
}
