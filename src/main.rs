#![allow(unused_imports)]
use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::unix::io::AsRawFd;

extern crate libc;

fn disable_nagle_algorithm(stream: &std::net::TcpStream) -> Result<(), std::io::Error> {
    // Get the raw file descriptor
    let fd = stream.as_raw_fd();

    // TCP_NODELAY = 1 disables Nagle's algorithm
    let value: libc::c_int = 1;

    // Set the socket option
    let result = unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            &value as *const _ as *const libc::c_void,
            std::mem::size_of_val(&value) as libc::socklen_t,
        )
    };

    if result == -1 {
        return Err(std::io::Error::last_os_error());
    }

    Ok(())
}

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

                // Disable Nagle's algorithm for low-latency responses
                if let Err(e) = disable_nagle_algorithm(&stream) {
                    println!("Warning: Failed to disable Nagle's algorithm: {}", e);
                }

                // Read the incoming request
                let mut buffer = [0u8; 1024]; // Buffer to read request
                let bytes_read = match stream.read(&mut buffer) {
                    Ok(n) => n,
                    Err(e) => {
                        println!("error reading from stream: {}", e);
                        continue;
                    }
                };

                if bytes_read < 12 {
                    println!("request too short, expected at least 12 bytes, got {}", bytes_read);
                    continue;
                }

                // Parse correlation_id from request header (offset 8-11)
                // Request structure: message_size(0-3) + api_key(4-5) + api_version(6-7) + correlation_id(8-11)
                let correlation_id_bytes = &buffer[8..12];
                let correlation_id = i32::from_be_bytes(correlation_id_bytes.try_into().unwrap());

                println!("Received correlation_id: {}", correlation_id);

                // Create response with correlation ID
                // message_size: 32-bit signed integer (4 bytes, big-endian)
                let message_size: i32 = 0;

                // Convert to big-endian bytes
                let message_size_bytes = message_size.to_be_bytes();
                let correlation_id_bytes = correlation_id.to_be_bytes();

                // Combine into response buffer
                let mut response = Vec::new();
                response.extend_from_slice(&message_size_bytes);
                response.extend_from_slice(&correlation_id_bytes);

                // Send the response (no flush needed with Nagle's disabled)
                match stream.write_all(&response) {
                    Ok(_) => println!("response sent successfully"),
                    Err(e) => println!("error sending response: {}", e),
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
