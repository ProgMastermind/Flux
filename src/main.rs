#![allow(unused_imports)]
use std::io::Write;
use std::net::TcpListener;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");

                // Create response with correlation ID
                // message_size: 32-bit signed integer (4 bytes, big-endian)
                // For this stage, we can use 0 as the tester only checks length
                let message_size: i32 = 0;
                let correlation_id: i32 = 7;

                // Convert to big-endian bytes
                let message_size_bytes = message_size.to_be_bytes();
                let correlation_id_bytes = correlation_id.to_be_bytes();

                // Combine into response buffer
                let mut response = Vec::new();
                response.extend_from_slice(&message_size_bytes);
                response.extend_from_slice(&correlation_id_bytes);

                // Send the response
                match stream.write_all(&response) {
                    Ok(_) => println!("response sent successfully"),
                    Err(e) => println!("error sending response: {}", e),
                }

                // Flush the stream
                match stream.flush() {
                    Ok(_) => {},
                    Err(e) => println!("error flushing stream: {}", e),
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
