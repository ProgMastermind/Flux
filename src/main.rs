#![allow(unused_imports)]
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::io::AsRawFd;
use std::thread;

extern crate libc;

fn handle_client(mut stream: TcpStream) {
    println!("accepted new connection");

    // Disable Nagle's algorithm for low-latency responses
    if let Err(e) = disable_nagle_algorithm(&stream) {
        println!("Warning: Failed to disable Nagle's algorithm: {}", e);
    }

    // Handle multiple sequential requests on the same connection
    loop {
        // Read the incoming request
        let mut buffer = [0u8; 1024]; // Buffer to read request
        let bytes_read = match stream.read(&mut buffer) {
            Ok(n) => n,
            Err(e) => {
                println!("error reading from stream: {}", e);
                break; // Exit the loop on read error
            }
        };

        // If client closed the connection (0 bytes read), exit the loop
        if bytes_read == 0 {
            println!("client closed the connection");
            break;
        }

        if bytes_read < 12 {
            println!("request too short, expected at least 12 bytes, got {}", bytes_read);
            continue; // Continue to next request instead of breaking
        }

        // Parse request header fields
        // Request structure: message_size(0-3) + api_key(4-5) + api_version(6-7) + correlation_id(8-11)

        // Parse message size (offset 0-3, 4 bytes, INT32, big-endian)
        let message_size_bytes = &buffer[0..4];
        let message_size = i32::from_be_bytes(message_size_bytes.try_into().unwrap());

        // Parse api_key (offset 4-5, 2 bytes, INT16, big-endian)
        let api_key_bytes = &buffer[4..6];
        let api_key = i16::from_be_bytes(api_key_bytes.try_into().unwrap());

        // Parse request_api_version (offset 6-7, 2 bytes, INT16, big-endian)
        let api_version_bytes = &buffer[6..8];
        let request_api_version = i16::from_be_bytes(api_version_bytes.try_into().unwrap());

        // Parse correlation_id (offset 8-11, 4 bytes, INT32, big-endian)
        let correlation_id_bytes = &buffer[8..12];
        let correlation_id = i32::from_be_bytes(correlation_id_bytes.try_into().unwrap());

        println!("Received message_size: {}, api_key: {}, correlation_id: {}, api_version: {}",
                 message_size, api_key, correlation_id, request_api_version);

        // Validate API version (broker supports versions 0-4)
        // UNSUPPORTED_VERSION = 35, SUCCESS = 0
        let error_code = if request_api_version >= 0 && request_api_version <= 4 {
            0i16  // SUCCESS
        } else {
            35i16 // UNSUPPORTED_VERSION
        };

        println!("Response error_code: {}", error_code);

        // Create ApiVersions response according to the specified format
        // Response format:
        // message_size(4) + correlation_id(4) + error_code(2) + array_length(1) +
        // api_version_1(2+2+2+1) + api_version_2(2+2+2+1) + api_version_3(2+2+2+1) +
        // throttle_time(4) + tag_buffer(1)

        // API Versions to return (API key 18 = API_VERSIONS)
        // API Version 1: API key 18, min version 0, max version 4
        // API Version 2: API key 1 (Fetch), min version 0, max version 16
        // API Version 3: API key 2 (Offsets), min version 0, max version 8

        let array_length: u8 = 0x04; // varint for 3 API versions + 1

        // API Version entries
        let mut api_version_1 = Vec::new();
        api_version_1.extend_from_slice(&18i16.to_be_bytes()); // API key 18 (API_VERSIONS)
        api_version_1.extend_from_slice(&0i16.to_be_bytes());  // Min version 0
        api_version_1.extend_from_slice(&4i16.to_be_bytes());  // Max version 4
        api_version_1.extend_from_slice(&[0u8]);              // Tag buffer

        let mut api_version_2 = Vec::new();
        api_version_2.extend_from_slice(&1i16.to_be_bytes());  // API key 1 (Fetch)
        api_version_2.extend_from_slice(&0i16.to_be_bytes());  // Min version 0
        api_version_2.extend_from_slice(&16i16.to_be_bytes()); // Max version 16
        api_version_2.extend_from_slice(&[0u8]);              // Tag buffer

        let mut api_version_3 = Vec::new();
        api_version_3.extend_from_slice(&2i16.to_be_bytes());  // API key 2 (Offsets)
        api_version_3.extend_from_slice(&0i16.to_be_bytes());  // Min version 0
        api_version_3.extend_from_slice(&8i16.to_be_bytes());  // Max version 8
        api_version_3.extend_from_slice(&[0u8]);              // Tag buffer

        let throttle_time: i32 = 0; // No throttling

        // Build response body
        let mut response_body = Vec::new();
        response_body.extend_from_slice(&correlation_id.to_be_bytes()); // correlation_id (4 bytes)
        response_body.extend_from_slice(&error_code.to_be_bytes());     // error_code (2 bytes)
        response_body.extend_from_slice(&[array_length]);              // array_length (1 byte)
        response_body.extend_from_slice(&api_version_1);               // API version 1
        response_body.extend_from_slice(&api_version_2);               // API version 2
        response_body.extend_from_slice(&api_version_3);               // API version 3
        response_body.extend_from_slice(&throttle_time.to_be_bytes()); // throttle_time (4 bytes)
        response_body.extend_from_slice(&[0u8]);                       // tag_buffer (1 byte)

        // Calculate message size (total response size minus the message_size field itself)
        let message_size = (response_body.len()) as i32;

        // Build final response
        let mut response = Vec::new();
        response.extend_from_slice(&message_size.to_be_bytes()); // message_size (4 bytes)
        response.extend_from_slice(&response_body);             // response body

        // Send the response (no flush needed with Nagle's disabled)
        match stream.write_all(&response) {
            Ok(_) => println!("response sent successfully"),
            Err(e) => {
                println!("error sending response: {}", e);
                break; // Exit the loop on write error
            }
        }
    }

    println!("connection closed");
}

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
            Ok(stream) => {
                // Spawn a new thread for each client connection to handle concurrent requests
                thread::spawn(move || {
                    handle_client(stream);
                });
            }
            Err(e) => {
                println!("error accepting connection: {}", e);
            }
        }
    }
}
