use tokio_modbus::{client::rtu, prelude::*};
use tokio_serial::{SerialPortBuilderExt, Parity, StopBits, DataBits};
use tokio::net::TcpStream;
use tokio::io::AsyncWriteExt;
use serde::Serialize;
use chrono::{Utc, TimeZone};
use std::error::Error;
use tokio::time::{sleep, Duration};
use std::fs::OpenOptions;
use std::io::Write;

#[derive(Serialize, Debug)]
struct SensorData {
    timestamp: String,
    sensor_id: String,
    location: String,
    process_stage:String,
    temperature_celsius: f32,
    humidity_percent: f32,
}

async fn read_sensor(slave: u8) -> Result<Vec<u16>, Box<dyn Error>> {
    let builder = tokio_serial::new("/dev/ttyUSB0", 9600)
        .parity(Parity::None)
        .stop_bits(StopBits::One)
        .data_bits(DataBits::Eight)
        .timeout(std::time::Duration::from_secs(2));

    let port = builder.open_native_async()?;
    let mut ctx = rtu::connect_slave(port, Slave(slave)).await?;

    let response = ctx.read_input_registers(1, 2).await?;
    Ok(response)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("client.log")?;

    println!("Program dimulai pada: {}", Utc::now().to_rfc3339());

    loop {
        println!("Memulai loop baru...");
        let wib = chrono::offset::FixedOffset::east_opt(7 * 3600).ok_or("Invalid timezone")?;
        let timestamp = wib.from_utc_datetime(&Utc::now().naive_utc()).to_rfc3339();

        match read_sensor(1).await {
            Ok(response) if response.len() == 2 => {
                let temp = response[0] as f32 / 100.0;
                let rh = response[1] as f32 / 100.0;

                if temp < -40.0 || temp > 125.0 || rh < 0.0 || rh > 100.0 {
                    println!("[{}] ⚠️ Bacaan tidak valid: Suhu {:.1} °C, Kelembapan {:.1} %", timestamp, temp, rh);
                    writeln!(log_file, "[{}] Bacaan tidak valid: Suhu {:.1} °C, Kelembapan {:.1} %", timestamp, temp, rh)?;
                } else {
                    println!("[{}] 📡 Suhu: {:.1} °C | Kelembapan: {:.1} %", timestamp, temp, rh);
                    writeln!(log_file, "[{}] Suhu: {:.1} °C | Kelembapan: {:.1} %", timestamp, temp, rh)?;

                    let data = SensorData {
                        timestamp: timestamp.clone(),
                        sensor_id: "SHT20-PascaPanen-001".into(),
                        location: "Gudang Fermentasi 1".into(),
                         process_stage: "Fermentasi".into(),
                        temperature_celsius: temp,
                        humidity_percent: rh,
                    };
                    let json = serde_json::to_string(&data)?;
                    println!("[{}] 📤 Mengirim: {}", timestamp, json);
                    writeln!(log_file, "[{}] Mengirim: {}", timestamp, json)?;

                    match TcpStream::connect("127.0.0.1:9000").await {
                        Ok(mut stream) => {
                            stream.write_all(json.as_bytes()).await?;
                            stream.write_all(b"\n").await?;
                            println!("[{}] ✅ Data dikirim ke TCP server", timestamp);
                            writeln!(log_file, "[{}] Data dikirim ke TCP server", timestamp)?;
                        },
                        Err(e) => {
                            println!("[{}] ❌ Gagal konek ke TCP server: {}", timestamp, e);
                            writeln!(log_file, "[{}] Gagal konek ke TCP server: {}", timestamp, e)?;
                        }
                    }
                }
            },
            Ok(other) => {
                println!("[{}] ⚠️ Data tidak lengkap: {:?}", timestamp, other);
                writeln!(log_file, "[{}] Data tidak lengkap: {:?}", timestamp, other)?;
            },
            Err(e) => {
                println!("[{}] ❌ Gagal baca sensor: {}", timestamp, e);
                writeln!(log_file, "[{}] Gagal baca sensor: {}", timestamp, e)?;
            }
        }

        sleep(Duration::from_secs(2)).await;
    }
}
