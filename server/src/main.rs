use tokio::net::TcpListener;
use tokio::io::{AsyncBufReadExt, BufReader};
use serde::Deserialize;
use reqwest::Client;

#[derive(Deserialize, Debug)]
struct SensorData {
    timestamp: String,
    sensor_id: String,
    location: String,
    process_stage: String,
    temperature_celsius: f32,
    humidity_percent: f32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:9000").await?;
    let influx_url = "http://localhost:8086/api/v2/write?org=percobaan&bucket=monitoring&precision=s";
    let token = "R34N4zE4idIagbLqE7SOgv01XzmwwxFUyWTwZvILbH0_nWdBIfe_0AXIaz29emjH246t0v1twZtNpLzfFMTj9g==";
    let client = Client::new();

    println!("🚪 TCP Server listening on port 9000...");

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("🔌 Koneksi masuk dari {}", addr);

        let client = client.clone();
        let influx_url = influx_url.to_string();
        let token = token.to_string();

        tokio::spawn(async move {
            let reader = BufReader::new(socket);
            let mut lines = reader.lines();

            while let Ok(Some(line)) = lines.next_line().await {
                match serde_json::from_str::<SensorData>(&line) {
                    Ok(data) => {
                        println!("📥 Data diterima: {:?}", data);

                        // Line Protocol format: measurement,tag1=value1 field1=val1,field2=val2 timestamp
                        let line = format!(
                            "monitoring,sensor_id={},location={},stage={} temperature={},humidity={} {}",
                            data.sensor_id.replace(" ", "\\ "),
                            data.location.replace(" ", "\\ "),
                            data.process_stage.replace(" ", "\\ "),
                            data.temperature_celsius,
                            data.humidity_percent,
                            chrono::DateTime::parse_from_rfc3339(&data.timestamp)
                                .unwrap()
                                .timestamp()
                        );

                        // Kirim ke InfluxDB
                        let res = client.post(&influx_url)
                            .header("Authorization", format!("Token {}", token))
                            .header("Content-Type", "text/plain")
                            .body(line)
                            .send()
                            .await;

                        match res {
                            Ok(resp) if resp.status().is_success() => {
                                println!("✅ Data dikirim ke InfluxDB");
                            },
                            Ok(resp) => {
                                println!("⚠️ Gagal kirim ke InfluxDB: {}", resp.status());
                            },
                            Err(e) => {
                                println!("❌ HTTP Error: {}", e);
                            }
                        }
                    },
                    Err(e) => println!("❌ Format JSON tidak valid: {}", e),
                }
            }
        });
    }
}

