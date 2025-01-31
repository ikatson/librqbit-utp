// I used this script to create a canary file with a known sequence of bytes
// that we can use to detect bugs.

use std::io::{BufWriter, Write};

fn main() {
    let mut args = std::env::args().skip(1);
    let name = args.next().expect("first arg should be filename");
    let size = args.next().expect("second arg should be size in megabytes");
    let size: u64 = size.parse().expect("invalid size");
    let size = size * 1024 * 1024;
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&name)
        .expect("cannot open file");

    let mut content = [0u8; 256];
    for (idx, byte) in (0..=u8::MAX).enumerate() {
        content[idx] = byte;
    }

    let mut file = BufWriter::new(file);
    let mut remaining = size;
    while remaining > 0 {
        let len = remaining.min(content.len() as u64);
        let buf = &content[..len as usize];
        file.write_all(buf).expect("error writing");
        remaining -= len;
    }
}
