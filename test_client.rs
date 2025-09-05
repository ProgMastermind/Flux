use std::io::{Read, Write};
use std::net::TcpStream;
use std::convert::TryInto;

fn main() {
    // Connect to the server
    let mut stream = TcpStream::connect("127.0.0.1:9092").unwrap();

    // API Versions request (v4)
    // Format: message_size(4) + api_key(2) + api_version(2) + correlation_id(4) + client_id + client_software_version + tag_buffer

    let api_key: i16 = 18; // API_VERSIONS
    let api_version: i16 = 4;
    let correlation_id: i32 = 12345;

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

    println!("Sending API Versions request...");
    stream.write_all(&request).unwrap();

    // Read response
    let mut buffer = [0u8; 1024];
    let bytes_read = stream.read(&mut buffer).unwrap();

    println!("Received {} bytes", bytes_read);

    // Parse response
    if bytes_read >= 4 {
        let response_message_size = i32::from_be_bytes(buffer[0..4].try_into().unwrap());
        println!("Response message size: {}", response_message_size);

        if bytes_read >= 8 {
            let response_correlation_id = i32::from_be_bytes(buffer[4..8].try_into().unwrap());
            println!("Response correlation ID: {}", response_correlation_id);

            if bytes_read >= 10 {
                let error_code = i16::from_be_bytes(buffer[8..10].try_into().unwrap());
                println!("Error code: {}", error_code);

                if bytes_read >= 11 {
                    let array_length = buffer[10];
                    println!("Array length: {}", array_length);

                    // Parse API versions
                    let mut offset = 11;
                    for i in 0..3 {
                        if offset + 7 <= bytes_read {
                            let api_key = i16::from_be_bytes(buffer[offset..offset+2].try_into().unwrap());
                            let min_version = i16::from_be_bytes(buffer[offset+2..offset+4].try_into().unwrap());
                            let max_version = i16::from_be_bytes(buffer[offset+4..offset+6].try_into().unwrap());
                            println!("API {}: key={}, min={}, max={}", i+1, api_key, min_version, max_version);
                            offset += 7;
                        }
                    }
                }
            }
        }
    }
}
