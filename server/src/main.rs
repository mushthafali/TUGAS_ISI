use tokio::net::TcpListener;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use serde::Deserialize;
use reqwest::Client;
use chrono::TimeZone;
use std::fs::OpenOptions;
use std::io::Write;
use std::env;

#[derive(Deserialize, Debug)]
struct SensorData {
    timestamp: String,
    sensor_id: String,
    location: String,
    process_stage: String,
    temperature_celsius: f32,
    humidity_percent: f32,
}

fn escape_tag(value: &str) -> String {
    value
        .replace('\\', "\\\\") // Escape backslash terlebih dahulu
        .replace(' ', "\\ ")
        .replace(',', "\\,")
        .replace('=', "\\=")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Ambil token dari environment variable
    let token = match env::var("INFLUX_TOKEN") {
        Ok(token) => token,
        Err(e) => {
            eprintln!("❌ Gagal membaca INFLUX_TOKEN dari environment: {}", e);
            return Err("INFLUX_TOKEN tidak ditemukan di environment.".into());
        }
    };

    let influx_url = "http://localhost:8086/api/v2/write?org=ITS&bucket=KOPI&precision=ns";

    let listener = TcpListener::bind("0.0.0.0:9000").await?;
    let client = Client::new();

    println!("🚪 TCP Server listening on port 9000...");

    let mut log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("server.log")?;

    loop {
        let (socket, addr) = listener.accept().await?;
        let wib = chrono::FixedOffset::east_opt(7 * 3600).unwrap();
        let timestamp = wib.from_utc_datetime(&chrono::Utc::now().naive_utc()).to_rfc3339();
        println!("[{}] 🔌 Koneksi masuk dari {}", timestamp, addr);
        writeln!(log_file, "[{}] Koneksi masuk dari {}", timestamp, addr)?;

        let client = client.clone();
        let influx_url = influx_url.to_string();
        let token = token.clone();
        let mut log_file = log_file.try_clone()?;

        // Split socket menjadi reader dan writer agar bisa baca & tulis secara bersamaan
        let (reader, mut writer) = socket.into_split();
        let mut lines = BufReader::new(reader).lines();

        tokio::spawn(async move {
            while let Ok(Some(line)) = lines.next_line().await {
                let timestamp_log = chrono::FixedOffset::east_opt(7 * 3600)
                    .unwrap()
                    .from_utc_datetime(&chrono::Utc::now().naive_utc())
                    .to_rfc3339();

                match serde_json::from_str::<SensorData>(&line) {
                    Ok(data) => {
                        println!("[{}] 📥 Data diterima: {:?}", timestamp_log, data);
                        writeln!(log_file, "[{}] Data diterima: {:?}", timestamp_log, data).ok();

                        // Validasi nilai sensor
                        if data.temperature_celsius < -40.0 || data.temperature_celsius > 125.0 ||
                            data.humidity_percent < 0.0 || data.humidity_percent > 100.0 {
                            println!(
                                "[{}] ⚠️ Data tidak valid: Suhu {:.1} °C, Kelembapan {:.1} %",
                                timestamp_log, data.temperature_celsius, data.humidity_percent
                            );
                            writeln!(
                                log_file,
                                "[{}] Data tidak valid: Suhu {:.1} °C, Kelembapan {:.1} %",
                                timestamp_log, data.temperature_celsius, data.humidity_percent
                            ).ok();
                            continue;
                        }

                        // Parse timestamp sensor
                        let parsed_ts = match chrono::DateTime::parse_from_rfc3339(&data.timestamp) {
                            Ok(dt) => dt.timestamp_nanos_opt().unwrap_or_else(|| {
                                println!("[{}] ⚠️ Timestamp sensor invalid, pakai waktu sekarang", timestamp_log);
                                chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
                            }),
                            Err(e) => {
                                println!("[{}] ⚠️ Gagal parse timestamp '{}': {}", timestamp_log, data.timestamp, e);
                                writeln!(log_file, "[{}] Gagal parse timestamp '{}': {}", timestamp_log, data.timestamp, e).ok();
                                chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
                            }
                        };

                        // Buat Line Protocol untuk InfluxDB (tanpa "f" di float)
                        let influx_line = format!(
                            "SHT20,sensor_id={},location={},process_stage={} temperature_celsius={},humidity_percent={} {}",
                            escape_tag(&data.sensor_id),
                            escape_tag(&data.location),
                            escape_tag(&data.process_stage),
                            data.temperature_celsius,
                            data.humidity_percent,
                            parsed_ts
                        );

                        println!("📡 Line Protocol: {}", influx_line);

                        // Kirim ke InfluxDB
                        let res = client.post(&influx_url)
                            .header("Authorization", format!("Token {}", token))
                            .header("Content-Type", "text/plain; charset=utf-8")
                            .body(influx_line.clone())
                            .send()
                            .await;

                        match res {
                            Ok(resp) if resp.status().is_success() => {
                                println!("[{}] ✅ Data dikirim ke InfluxDB", timestamp_log);
                                writeln!(log_file, "[{}] Data dikirim ke InfluxDB: {}", timestamp_log, influx_line).ok();
                            },
                            Ok(resp) => {
                                let status = resp.status();
                                let body = resp.text().await.unwrap_or_default();
                                println!("[{}] ⚠️ Gagal kirim ke InfluxDB: {} - {}", timestamp_log, status, body);
                                writeln!(log_file, "[{}] Gagal kirim ke InfluxDB: {} - {}", timestamp_log, status, body).ok();
                            },
                            Err(e) => {
                                println!("[{}] ❌ HTTP Error: {}", timestamp_log, e);
                                writeln!(log_file, "[{}] HTTP Error: {}", timestamp_log, e).ok();
                            }
                        }

                        // Kirim balik data JSON ke client TCP (Qt)
                        let json_line = format!("{}\n", line);
                        if let Err(e) = writer.write_all(json_line.as_bytes()).await {
                            println!("[{}] ⚠️ Gagal kirim data ke client: {}", timestamp_log, e);
                        } else {
                            println!("[{}] ✅ Data dikirim balik ke client: {}", timestamp_log, json_line.trim());
                        }
                        if let Err(e) = writer.flush().await {
                            println!("[{}] ⚠️ Gagal flush data ke client: {}", timestamp_log, e);
                        }
                    },
                    Err(e) => {
                        println!("[{}] ❌ Format JSON tidak valid: {}", timestamp_log, e);
                        writeln!(log_file, "[{}] Format JSON tidak valid: {}", timestamp_log, e).ok();
                    }
                }
            }
            Ok::<(), Box<dyn std::error::Error>>(()).ok();
        });
    }
}
