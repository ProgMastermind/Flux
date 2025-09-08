#![allow(unused_imports)]
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::io::AsRawFd;
use std::thread;

extern crate libc;

// Helper functions for safe byte parsing
fn read_exact_bytes(stream: &mut TcpStream, buffer: &mut [u8]) -> Result<(), std::io::Error> {
    let mut bytes_read = 0;
    while bytes_read < buffer.len() {
        match stream.read(&mut buffer[bytes_read..]) {
            Ok(0) => return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Connection closed")),
            Ok(n) => bytes_read += n,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn read_i32_be(buffer: &[u8], offset: usize) -> Result<i32, &'static str> {
    if offset + 4 > buffer.len() {
        return Err("Buffer too short for i32");
    }
    let bytes: [u8; 4] = buffer[offset..offset + 4].try_into().unwrap();
    Ok(i32::from_be_bytes(bytes))
}

fn read_i16_be(buffer: &[u8], offset: usize) -> Result<i16, &'static str> {
    if offset + 2 > buffer.len() {
        return Err("Buffer too short for i16");
    }
    let bytes: [u8; 2] = buffer[offset..offset + 2].try_into().unwrap();
    Ok(i16::from_be_bytes(bytes))
}

fn read_compact_string(buffer: &[u8], offset: usize) -> Result<(String, usize), &'static str> {
    if offset >= buffer.len() {
        return Err("Buffer too short for string length");
    }
    let length = buffer[offset] as usize;
    if offset + 1 + length > buffer.len() {
        return Err("Buffer too short for string content");
    }
    let string_bytes = &buffer[offset + 1..offset + 1 + length];
    let string = String::from_utf8(string_bytes.to_vec()).map_err(|_| "Invalid UTF-8")?;
    Ok((string, offset + 1 + length))
}

struct ParsedTopic {
    name: String,
}

fn parse_describe_topic_partitions_request(buffer: &[u8], mut offset: usize) -> Result<Vec<ParsedTopic>, &'static str> {
    // Skip client_id: length (2 bytes) + content
    let client_id_length = read_i16_be(buffer, offset)? as usize;
    offset += 2 + client_id_length;
    
    // Skip request header tag buffer (1 byte)
    if offset >= buffer.len() {
        return Err("Buffer too short for header tag buffer");
    }
    offset += 1;
    
    // Read topics array length (1 byte - but this is a COMPACT_ARRAY, so length-1)
    if offset >= buffer.len() {
        return Err("Buffer too short for topics array length");
    }
    let topics_count_raw = buffer[offset] as usize;
    offset += 1;
    
    // For COMPACT_ARRAY, actual count = raw_value - 1 (unless raw_value is 0)
    let topics_count = if topics_count_raw == 0 { 0 } else { topics_count_raw - 1 };
    
    println!("Raw topics count: {}, Actual topics count: {}", topics_count_raw, topics_count);
    
    let mut topics = Vec::new();
    
    for i in 0..topics_count {
        // Read topic name
        let (topic_name, new_offset) = read_compact_string(buffer, offset)?;
        offset = new_offset;
        
        println!("Topic {}: '{}'", i, topic_name);
        
        // Skip topic tag buffer (1 byte)
        if offset >= buffer.len() {
            return Err("Buffer too short for topic tag buffer");
        }
        offset += 1;
        
        topics.push(ParsedTopic { name: topic_name });
    }
    
    Ok(topics)
}

fn create_describe_topic_partitions_response(correlation_id: i32, topics: &[ParsedTopic]) -> Vec<u8> {
    let mut response_body = Vec::new();
    
    // Response header
    response_body.extend_from_slice(&correlation_id.to_be_bytes()); // correlation_id (4 bytes)
    response_body.push(0); // tag buffer (1 byte)
    
    // Response body
    response_body.extend_from_slice(&0i32.to_be_bytes()); // throttle_time_ms (4 bytes)
    
    // Topics array length (1 byte) - COMPACT_ARRAY format: actual_length + 1
    let compact_array_length = if topics.is_empty() { 0 } else { topics.len() + 1 };
    response_body.push(compact_array_length as u8);
    
    for topic in topics {
        // Error code: UNKNOWN_TOPIC_OR_PARTITION = 3
        response_body.extend_from_slice(&3i16.to_be_bytes()); // error_code (2 bytes)
        
        // Topic name: length + content
        response_body.push(topic.name.len() as u8); // topic name length (1 byte)
        response_body.extend_from_slice(topic.name.as_bytes()); // topic name content
        
        // Topic ID: 16 bytes of zeros (00000000-0000-0000-0000-000000000000)
        response_body.extend_from_slice(&[0u8; 16]);
        
        // Is internal: false
        response_body.push(0); // is_internal (1 byte)
        
        // Partitions array: empty
        response_body.push(0); // partitions array length (1 byte)
        
        // Topic authorized operations: none
        response_body.extend_from_slice(&0i32.to_be_bytes()); // topic_authorized_operations (4 bytes)
        
        // Topic tag buffer
        response_body.push(0); // tag buffer (1 byte)
    }
    
    // Next cursor: null/empty cursor (COMPACT_STRING with length 1 for empty string + partition_index + tag_buffer)
    response_body.push(1); // topic_name length = 1 (which means 0 characters in COMPACT_STRING format)
    response_body.extend_from_slice(&(-1i32).to_be_bytes()); // partition_index = -1 (no cursor)
    response_body.push(0); // cursor tag buffer
    
    // Response body tag buffer
    response_body.push(0); // tag buffer (1 byte)
    
    // Prepend message size
    let message_size = response_body.len() as i32;
    let mut response = Vec::new();
    response.extend_from_slice(&message_size.to_be_bytes()); // message_size (4 bytes)
    response.extend_from_slice(&response_body);
    
    response
}

fn handle_client(mut stream: TcpStream) {
    println!("accepted new connection");

    // Disable Nagle's algorithm for low-latency responses
    if let Err(e) = disable_nagle_algorithm(&stream) {
        println!("Warning: Failed to disable Nagle's algorithm: {}", e);
    }

    // Handle multiple sequential requests on the same connection
    loop {
        // Read message size first (4 bytes)
        let mut size_buffer = [0u8; 4];
        if let Err(e) = read_exact_bytes(&mut stream, &mut size_buffer) {
            println!("error reading message size: {}", e);
            break;
        }
        
        let message_size = i32::from_be_bytes(size_buffer);
        if message_size < 8 || message_size > 10000 {
            println!("invalid message size: {}", message_size);
            break;
        }
        
        // Read the full message
        let mut message_buffer = vec![0u8; message_size as usize];
        if let Err(e) = read_exact_bytes(&mut stream, &mut message_buffer) {
            println!("error reading message body: {}", e);
            break;
        }

        // Parse request header
        let api_key = match read_i16_be(&message_buffer, 0) {
            Ok(key) => key,
            Err(e) => {
                println!("error parsing api_key: {}", e);
                continue;
            }
        };
        
        let api_version = match read_i16_be(&message_buffer, 2) {
            Ok(version) => version,
            Err(e) => {
                println!("error parsing api_version: {}", e);
                continue;
            }
        };
        
        let correlation_id = match read_i32_be(&message_buffer, 4) {
            Ok(id) => id,
            Err(e) => {
                println!("error parsing correlation_id: {}", e);
                continue;
            }
        };

        println!("Received api_key: {}, api_version: {}, correlation_id: {}", 
                 api_key, api_version, correlation_id);

        // Route based on API key
        let response = match api_key {
            18 => {
                // ApiVersions request
                handle_api_versions_request(correlation_id, api_version)
            },
            75 => {
                // DescribeTopicPartitions request
                if api_version != 0 {
                    // Unsupported version
                    create_error_response(correlation_id, 35) // UNSUPPORTED_VERSION
                } else {
                    handle_describe_topic_partitions_request(correlation_id, &message_buffer)
                }
            },
            _ => {
                println!("unsupported api_key: {}", api_key);
                continue;
            }
        };

        // Send the response
        match stream.write_all(&response) {
            Ok(_) => println!("response sent successfully"),
            Err(e) => {
                println!("error sending response: {}", e);
                break;
            }
        }
    }

    println!("connection closed");
}

fn handle_api_versions_request(correlation_id: i32, api_version: i16) -> Vec<u8> {
    // Validate API version (broker supports versions 0-4)
    let error_code = if api_version >= 0 && api_version <= 4 {
        0i16  // SUCCESS
    } else {
        35i16 // UNSUPPORTED_VERSION
    };

    let array_length: u8 = 0x05; // varint for 4 API versions + 1

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

    let mut api_version_4 = Vec::new();
    api_version_4.extend_from_slice(&75i16.to_be_bytes()); // API key 75 (DescribeTopicPartitions)
    api_version_4.extend_from_slice(&0i16.to_be_bytes());  // Min version 0
    api_version_4.extend_from_slice(&0i16.to_be_bytes());  // Max version 0
    api_version_4.extend_from_slice(&[0u8]);              // Tag buffer

    let throttle_time: i32 = 0; // No throttling

    // Build response body
    let mut response_body = Vec::new();
    response_body.extend_from_slice(&correlation_id.to_be_bytes()); // correlation_id (4 bytes)
    response_body.extend_from_slice(&error_code.to_be_bytes());     // error_code (2 bytes)
    response_body.extend_from_slice(&[array_length]);              // array_length (1 byte)
    response_body.extend_from_slice(&api_version_1);               // API version 1
    response_body.extend_from_slice(&api_version_2);               // API version 2
    response_body.extend_from_slice(&api_version_3);               // API version 3
    response_body.extend_from_slice(&api_version_4);               // API version 4
    response_body.extend_from_slice(&throttle_time.to_be_bytes()); // throttle_time (4 bytes)
    response_body.extend_from_slice(&[0u8]);                       // tag_buffer (1 byte)

    // Calculate message size and build final response
    let message_size = response_body.len() as i32;
    let mut response = Vec::new();
    response.extend_from_slice(&message_size.to_be_bytes());
    response.extend_from_slice(&response_body);
    response
}

fn handle_describe_topic_partitions_request(correlation_id: i32, message_buffer: &[u8]) -> Vec<u8> {
    // Parse the request starting after the request header (api_key, api_version, correlation_id)
    match parse_describe_topic_partitions_request(message_buffer, 8) {
        Ok(topics) => {
            println!("Parsed {} topics", topics.len());
            for topic in &topics {
                println!("Topic: {}", topic.name);
            }
            create_describe_topic_partitions_response(correlation_id, &topics)
        },
        Err(e) => {
            println!("error parsing DescribeTopicPartitions request: {}", e);
            create_error_response(correlation_id, 2) // INVALID_REQUEST
        }
    }
}

fn create_error_response(correlation_id: i32, error_code: i16) -> Vec<u8> {
    let mut response_body = Vec::new();
    response_body.extend_from_slice(&correlation_id.to_be_bytes());
    response_body.extend_from_slice(&error_code.to_be_bytes());
    
    let message_size = response_body.len() as i32;
    let mut response = Vec::new();
    response.extend_from_slice(&message_size.to_be_bytes());
    response.extend_from_slice(&response_body);
    response
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
