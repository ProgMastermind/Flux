use std::io::{Read, Write};
use std::net::TcpStream;
use std::convert::TryInto;
use std::thread;
use std::time::Duration;

fn simulate_client(client_id: u32) {
    println!("Client {}: Connecting to server...", client_id);

    // Connect to the server
    let mut stream = TcpStream::connect("127.0.0.1:9092").unwrap();
    println!("Client {}: Connected successfully", client_id);

    // Send 3 requests from this client
    for request_num in 1..=3 {
        let correlation_id = ((client_id * 1000) + request_num) as i32;

        println!("Client {}: Sending request {} (correlation_id: {})",
                 client_id, request_num, correlation_id);

        // API Versions request (v4)
        let api_key: i16 = 18; // API_VERSIONS
        let api_version: i16 = 4;

        // Client ID: length(1) + client-specific ID
        let client_id_str = format!("client{}", client_id);
        let client_id_length: u8 = client_id_str.len() as u8;
        let client_id_bytes = client_id_str.as_bytes();

        // Client software version: length(1) + "1.0.0"(5 bytes)
        let client_sw_length: u8 = 5;
        let client_sw = b"1.0.0";

        // Build request body
        let mut request_body = Vec::new();
        request_body.extend_from_slice(&api_key.to_be_bytes());
        request_body.extend_from_slice(&api_version.to_be_bytes());
        request_body.extend_from_slice(&correlation_id.to_be_bytes());
        request_body.extend_from_slice(&[client_id_length]);
        request_body.extend_from_slice(client_id_bytes);
        request_body.extend_from_slice(&[client_sw_length]);
        request_body.extend_from_slice(client_sw);
        request_body.extend_from_slice(&[0u8]); // tag buffer

        // Calculate message size
        let message_size = request_body.len() as i32;

        // Build final request
        let mut request = Vec::new();
        request.extend_from_slice(&message_size.to_be_bytes());
        request.extend_from_slice(&request_body);

        stream.write_all(&request).unwrap();

        // Read response
        let mut buffer = [0u8; 1024];
        let bytes_read = stream.read(&mut buffer).unwrap();

        // Parse response correlation ID
        if bytes_read >= 8 {
            let response_correlation_id = i32::from_be_bytes(buffer[4..8].try_into().unwrap());
            if response_correlation_id == correlation_id {
                println!("Client {}: ✅ Response {} received correctly (correlation_id: {})",
                         client_id, request_num, response_correlation_id);
            } else {
                println!("Client {}: ❌ Correlation ID mismatch! Expected: {}, Got: {}",
                         client_id, correlation_id, response_correlation_id);
            }
        }

        // Small delay between requests from same client
        thread::sleep(Duration::from_millis(50));
    }

    println!("Client {}: Closing connection", client_id);
    drop(stream);
}

fn main() {
    println!("Testing concurrent client handling with 3 clients...");

    // Spawn 3 concurrent clients
    let mut handles = vec![];

    for client_id in 1..=3 {
        let handle = thread::spawn(move || {
            simulate_client(client_id);
        });
        handles.push(handle);

        // Small stagger between client starts
        thread::sleep(Duration::from_millis(100));
    }

    // Wait for all clients to complete
    for handle in handles {
        handle.join().unwrap();
    }

    println!("\n--- Concurrent Test Complete ---");
    println!("All clients completed successfully!");
}
