
use std::{
    collections::{HashMap, HashSet},
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

#[derive(Clone, Debug)]
struct Message {
    id: u64,
    origin: char,
    sender: char,
    text: String,
}

struct Peer {
    id: char,
    address: String,
    neighbors: Vec<(char, String)>,

    // Messages this peer knows.
    messages: Mutex<HashMap<u64, Message>>,

    // Neighbors to which this peer has attempted to send
    // each message.
    delivered: Mutex<HashMap<u64, HashSet<char>>>,
}

fn send_message(address: &str, message: &Message) {
    if let Ok(mut stream) = TcpStream::connect(address) {
        let data = format!(
            "{}|{}|{}|{}\n",
            message.id,
            message.origin,
            message.sender,
            message.text
        );

        let _ = stream.write_all(data.as_bytes());
    }
}

fn handle_connection(stream: TcpStream, peer: Arc<Peer>) {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();

    if reader.read_line(&mut line).is_err() {
        return;
    }

    let parts: Vec<&str> = line.trim().splitn(4, '|').collect();

    if parts.len() != 4 {
        return;
    }

    let id: u64 = match parts[0].parse() {
        Ok(id) => id,
        Err(_) => return,
    };

    let origin = match parts[1].chars().next() {
        Some(c) => c,
        None => return,
    };

    let sender = match parts[2].chars().next() {
        Some(c) => c,
        None => return,
    };

    let message = Message {
        id,
        origin,
        sender,
        text: parts[3].to_string(),
    };

    // Accept only the first copy of a message.
    {
        let mut messages = peer.messages.lock().unwrap();

        if messages.contains_key(&message.id) {
            println!(
                "[{}] ignored duplicate message {} from {}",
                peer.id, message.id, message.sender
            );
            return;
        }

        messages.insert(message.id, message.clone());
    }

    // Remember the peer that sent this copy.
    {
        let mut delivered = peer.delivered.lock().unwrap();

        delivered
            .entry(message.id)
            .or_default()
            .insert(message.sender);
    }

    println!(
        "[{}] received message {} from {} (origin: {}): {}",
        peer.id,
        message.id,
        message.sender,
        message.origin,
        message.text
    );
}

fn listener(peer: Arc<Peer>) {
    let listener = TcpListener::bind(&peer.address).unwrap();

    println!("[{}] listening on {}", peer.id, peer.address);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let peer = Arc::clone(&peer);

                thread::spawn(move || {
                    handle_connection(stream, peer);
                });
            }
            Err(error) => {
                eprintln!("[{}] connection error: {}", peer.id, error);
            }
        }
    }
}

fn gossip_loop(peer: Arc<Peer>) {
    loop {
        thread::sleep(Duration::from_secs(2));

        let messages: Vec<Message> = {
            let messages = peer.messages.lock().unwrap();
            messages.values().cloned().collect()
        };

        for message in messages {
            // Find one neighbor we haven't attempted to send
            // this message to.
            let target = {
                let delivered = peer.delivered.lock().unwrap();

                let already_sent_to = delivered.get(&message.id);

                peer.neighbors
                    .iter()
                    .find(|(neighbor_id, _)| {
                        match already_sent_to {
                            Some(sent_to) => !sent_to.contains(neighbor_id),
                            None => true,
                        }
                    })
                    .cloned()
            };

            let Some((target_id, target_address)) = target else {
                continue;
            };

            // Mark before sending, to avoid duplicate attempts
            // from this peer's gossip loop.
            {
                let mut delivered = peer.delivered.lock().unwrap();

                delivered
                    .entry(message.id)
                    .or_default()
                    .insert(target_id);
            }

            println!(
                "[{}] gossiping message {} -> {}",
                peer.id, message.id, target_id
            );

            // Update the sender to identify this hop.
            let mut forwarded = message.clone();
            forwarded.sender = peer.id;

            thread::spawn(move || {
                send_message(&target_address, &forwarded);
            });
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() != 2 {
        println!("Usage: cargo run -- A|B|C|D");
        return;
    }

    let id = match args[1].chars().next() {
        Some(id @ ('A' | 'B' | 'C' | 'D' | 'E')) if args[1].len() == 1 => id,
        _ => {
            println!("Peer must be A, B, C or D");
            return;
        }
    };

    // Topology:
    //
    //       B
    //      / \
    //     A   D
    //      \ /
    //       C

    let (address, neighbors) = match id {
        'A' => (
            "127.0.0.1:8000",
            vec![
                ('B', "127.0.0.1:8001".to_string()),
                ('C', "127.0.0.1:8002".to_string()),
            ],
        ),
        'B' => (
            "127.0.0.1:8001",
            vec![
                ('A', "127.0.0.1:8000".to_string()),
                ('D', "127.0.0.1:8003".to_string()),
            ],
        ),
        'C' => (
            "127.0.0.1:8002",
            vec![
                ('A', "127.0.0.1:8000".to_string()),
                ('D', "127.0.0.1:8003".to_string()),
            ],
        ),
        'D' => (
            "127.0.0.1:8003",
            vec![
                ('B', "127.0.0.1:8001".to_string()),
                ('C', "127.0.0.1:8002".to_string()),
                   ('E', "127.0.0.1:8004".to_string()),
            ],
        ),
        'E' => (
    "127.0.0.1:8004",
    vec![
        ('D', "127.0.0.1:8003".to_string()),
    ],
),
        _ => unreachable!(),
    };

    let peer = Arc::new(Peer {
        id,
        address: address.to_string(),
        neighbors,
        messages: Mutex::new(HashMap::new()),
        delivered: Mutex::new(HashMap::new()),
    });

    // Start the TCP listener.
    {
        let peer = Arc::clone(&peer);
        thread::spawn(move || listener(peer));
    }

    // Start the gossip loop.
    {
        let peer = Arc::clone(&peer);
        thread::spawn(move || gossip_loop(peer));
    }

    println!("[{}] started", peer.id);

    if peer.id == 'A' {
        println!("Type a message:");

        let stdin = std::io::stdin();
        let mut id = 0_u64;

        for line in stdin.lock().lines() {
            let Ok(text) = line else {
                continue;
            };

            if text.is_empty() {
                continue;
            }

            id += 1;

            let message = Message {
                id,
                origin: 'A',
                sender: 'A',
                text,
            };

            peer.messages
                .lock()
                .unwrap()
                .insert(message.id, message.clone());

            println!(
                "[A] created message {}: {}",
                message.id, message.text
            );
        }
    } else {
        loop {
            thread::sleep(Duration::from_secs(60));
        }
    }
}

