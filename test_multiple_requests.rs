use std::io::{Read, Write};
use std::net::TcpStream;
use std::convert::TryInto;

fn main() {
    println!("Testing multiple sequential APIVersions requests...");

    // Connect to the server
    let mut stream = TcpStream::connect("127.0.0.1:9092").unwrap();
    println!("Connected to server");

    // Send 3 APIVersions requests sequentially
    for request_num in 1..=3 {
        println!("\n--- Sending Request {} ---", request_num);

        // API Versions request (v4)
        let api_key: i16 = 18; // API_VERSIONS
        let api_version: i16 = 4;
        let correlation_id: i32 = 1000 + request_num; // Different correlation ID for each request

        // Client ID: length(1) + "test"(4 bytes)
        let client_id_length: u8 = 4;
        let client_id = b"test";

        // Client software version: length(1) + "1.0.0"(5 bytes)
        let client_sw_length: u8 = 5;
        let client_sw = b"1.0.0";

        // Build request body
        let mut request_body = Vec::new();
        request_body.extend_from_slice(&api_key.to_be_bytes());
        request_body.extend_from_slice(&api_version.to_be_bytes());
        request_body.extend_from_slice(&correlation_id.to_be_bytes());
        request_body.extend_from_slice(&[client_id_length]);
        request_body.extend_from_slice(client_id);
        request_body.extend_from_slice(&[client_sw_length]);
        request_body.extend_from_slice(client_sw);
        request_body.extend_from_slice(&[0u8]); // tag buffer

        // Calculate message size
        let message_size = request_body.len() as i32;

        // Build final request
        let mut request = Vec::new();
        request.extend_from_slice(&message_size.to_be_bytes());
        request.extend_from_slice(&request_body);

        println!("Sending request with correlation_id: {}", correlation_id);
        stream.write_all(&request).unwrap();

        // Read response
        let mut buffer = [0u8; 1024];
        let bytes_read = stream.read(&mut buffer).unwrap();

        println!("Received {} bytes", bytes_read);

        if bytes_read >= 8 {
            let response_correlation_id = i32::from_be_bytes(buffer[4..8].try_into().unwrap());
            println!("Response correlation_id: {} (expected: {})",
                     response_correlation_id, correlation_id);

            if response_correlation_id == correlation_id {
                println!("✅ Correlation ID matches!");
            } else {
                println!("❌ Correlation ID mismatch!");
            }

            if bytes_read >= 10 {
                let error_code = i16::from_be_bytes(buffer[8..10].try_into().unwrap());
                println!("Error code: {}", error_code);

                if error_code == 0 {
                    println!("✅ Error code is SUCCESS!");
                } else {
                    println!("❌ Error code is not SUCCESS!");
                }
            }
        }

        // Small delay between requests
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    println!("\n--- Test Complete ---");
    println!("Closing connection...");

    // Close the connection
    drop(stream);
    println!("Connection closed");
}
