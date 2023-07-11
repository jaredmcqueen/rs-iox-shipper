use influxdb_iox_client::connection::Builder;
use influxdb_iox_client::write::Client;
use std::time::Duration;
use tokio::signal;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::time::interval;
use tokio::time::Instant;

struct IoxShipper {
    client: Client,
}

impl IoxShipper {
    async fn send_batch(&mut self, batch: &Vec<String>) {
        // let start = SystemTime::now();
        let start = Instant::now();
        self.client
            .write_lp("algo_data", batch.join("\n"))
            .await
            .expect("failed to write to IOx");
        // let end = SystemTime::now().duration_since(start);
        println!(
            "sent {} in {} millis",
            batch.len(),
            start.elapsed().as_millis() // end.unwrap().as_millis()
        );
    }

    async fn new() -> Self {
        let connection = Builder::default()
            // .build("http://127.0.0.1:8080")
            .build("http://10.0.0.6:8080")
            .await
            .unwrap();

        let client = Client::new(connection);
        IoxShipper { client }
    }

    async fn batcher(&mut self, mut rx: Receiver<String>) {
        let mut tick_count = 0;
        let mut batch = vec![];
        let mut tick_timer = interval(Duration::from_secs(1));
        let mut tick_time_start = Instant::now();

        loop {
            tokio::select! {

                Some(tick) = rx.recv() => {
                    tick_count += 1;
                    batch.push(tick);


                    // if tick_count >= 100  {
                    if tick_count >= 50_000 || tick_time_start.elapsed() >= Duration::from_secs(1) {
                        self.send_batch(&batch).await;
                        tick_count =0;
                        batch.clear();
                    }

                },
              _ = tick_timer.tick() => {
                    // Send the batch if the time interval has elapsed
                    if !batch.is_empty() {
                        self.send_batch(&batch).await;
                        // Reset counters
                        tick_count = 0;
                        batch.clear();
                    }
                    tick_time_start = Instant::now();
                }

            }
        }
    }
}

#[tokio::main]
async fn main() {
    let (tx, rx) = mpsc::channel(100);

    let mut my_iox = IoxShipper::new().await;
    tokio::spawn(async move { my_iox.batcher(rx).await });

    let start = Instant::now();
    for i in 0..1_000_000 {
        let data = format!("test-data,symbol=AAPL p={} {}", i, i);
        if tx.send(data).await.is_err() {
            eprintln!("bad");
        }
        // sleep(Duration::from_millis(10)).await;
    }

    println!("sent in {}", start.elapsed().as_millis());
    match signal::ctrl_c().await {
        Ok(()) => {}
        Err(err) => {
            eprintln!("Unable to listen for shutdown signal: {}", err);
            // we also shut down in case of error
        }
    }
}
