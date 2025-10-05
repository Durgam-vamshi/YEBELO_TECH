use rdkafka::Message;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::broadcast;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::config::ClientConfig;
use serde::Deserialize;
use serde_json::json;
use warp::Filter;
use futures_util::{StreamExt, SinkExt};
use warp::ws::{Message as WsMessage, WebSocket};

#[derive(Deserialize, Debug, Clone)]
struct Trade {
    block_time: String,
    token_address: String,
    price_in_sol: f64,
}

#[derive(serde::Serialize, Clone)]
struct TokenData {
    token: String,
    price: f64,
    rsi: f64,
    block_time: String,
}

fn calculate_rsi(prices: &[f64]) -> f64 {
    if prices.len() < 14 {
        return 0.0;
    }

    let mut gains = 0.0;
    let mut losses = 0.0;

    for i in 1..prices.len() {
        let diff = prices[i] - prices[i - 1];
        if diff > 0.0 {
            gains += diff;
        } else {
            losses -= diff;
        }
    }

    let avg_gain = gains / 14.0;
    let avg_loss = losses / 14.0;

    if avg_loss == 0.0 {
        return 100.0;
    }

    let rs = avg_gain / avg_loss;
    100.0 - (100.0 / (1.0 + rs))
}

async fn handle_ws(ws: WebSocket, tx: broadcast::Sender<TokenData>) {
    let mut rx = tx.subscribe();
    let (mut ws_tx, _) = ws.split();

    tokio::spawn(async move {
        while let Ok(data) = rx.recv().await {
            if let Ok(msg) = serde_json::to_string(&data) {
                let _ = ws_tx.send(WsMessage::text(msg)).await;
            }
        }
    });
}

#[tokio::main]
async fn main() {
    println!("Starting RSI service...");

    let (tx_ws, _) = broadcast::channel::<TokenData>(100);
    let tx_ws_for_ws = tx_ws.clone();
    let tx_ws_filter = warp::any().map(move || tx_ws_for_ws.clone());

    let ws_route = warp::path("ws")
        .and(warp::ws())
        .and(tx_ws_filter)
        .map(|ws: warp::ws::Ws, tx_ws: broadcast::Sender<TokenData>| {
            ws.on_upgrade(move |socket| handle_ws(socket, tx_ws))
        });

    tokio::spawn(async move {
        warp::serve(ws_route).run(([0, 0, 0, 0], 4040)).await;
    });

    let mut price_history: HashMap<String, Vec<f64>> = HashMap::new();

    // Kafka consumer
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "rsi_service_group")
        .set("bootstrap.servers", "localhost:19092,localhost:29092,localhost:39092")
        .create()
        .expect("Consumer creation failed");

    consumer.subscribe(&["trade-data"]).expect("Can't subscribe");

    // Kafka producer
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:19092,localhost:29092,localhost:39092")
        .create()
        .expect("Producer creation failed");

    let mut message_stream = consumer.stream();

while let Some(message) = message_stream.next().await {
    match message {
        Ok(m) => {
            if let Some(payload) = m.payload() {
                if let Ok(payload_str) = std::str::from_utf8(payload) {
                    if let Ok(trade) = serde_json::from_str::<Trade>(payload_str) {
                        let prices = price_history
                            .entry(trade.token_address.clone())
                            .or_insert(Vec::new());
                        prices.push(trade.price_in_sol);

                        if prices.len() > 14 {
                            prices.remove(0);
                        }

                        let rsi = calculate_rsi(prices);

                        let token_data = TokenData {
                            token: trade.token_address.clone(),
                            price: trade.price_in_sol,
                            rsi,
                            block_time: trade.block_time.clone(),
                        };

                        // Broadcast via WebSocket
                        let _ = tx_ws.send(token_data.clone());

                        // Send RSI to Kafka topic
                        let rsi_payload = serde_json::to_string(&json!( {
                            "token_address": trade.token_address,
                            "rsi": rsi,
                            "block_time": trade.block_time
                        }))
                        .unwrap();

                        let _ = producer
                            .send(
                                FutureRecord::to("rsi-data")
                                    .payload(&rsi_payload)
                                    .key(&trade.token_address),
                                Duration::from_secs(0),
                            )
                            .await;

                        // 📌 Console log for RSI ranges
                        if rsi > 70.0 {
                            println!(
                                "📌 {} RSI: {:.2} (Overbought!)",
                                trade.token_address, rsi
                            );
                        } else if rsi < 30.0 {
                            println!(
                                "📌 {} RSI: {:.2} (Oversold!)",
                                trade.token_address, rsi
                            );
                        } else {
                            println!("📌 {} RSI: {:.2}", trade.token_address, rsi);
                        }
                    } else {
                        eprintln!("Failed to parse trade JSON: {}", payload_str);
                    }
                } else {
                    eprintln!("Invalid UTF-8 payload");
                }
            }
        }
        Err(e) => eprintln!("Error receiving message: {:?}", e),
    }
}
}