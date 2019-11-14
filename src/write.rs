//! [`Write`] stream to create bgzip format file.
//!
//! [`Write`]: https://doc.rust-lang.org/std/io/trait.Write.html
//!
//! # Example
//! ```
//! use bgzip::write::BGzWriter;
//! use std::fs;
//! use std::io;
//! use std::io::prelude::*;
//!
//! # fn main() { let _ = run(); }
//! # fn run() -> io::Result<()> {
//! let data = b"0123456789ABCDEF";
//! let mut writer = BGzWriter::new(fs::File::create("tmp/test2.gz")?);
//!
//! for _ in 0..30000 {
//!     writer.write(&data[..])?;
//! }
//! # Ok(())
//! # }
//! ```

use crossbeam_channel::{bounded, Receiver, Sender};
use flate2::write::DeflateEncoder;
use flate2::{Compression, CrcWriter};
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::collections::HashMap;
use std::io;
use std::io::prelude::*;

const DEFAULT_BUFFER: usize = 65280;

#[derive(Debug)]
pub struct BGzWriter {
    buffer: Vec<u8>,
    pool: ThreadPool,
    channels: Option<(
        Receiver<io::Result<Vec<u8>>>,
        Sender<io::Result<Vec<u8>>>,
        Sender<(u64, io::Result<(usize, Vec<u8>, u32)>)>,
    )>,
    r_end: Receiver<()>,
    seq: u64,
}

impl BGzWriter {
    pub fn new<R: io::Write + Sync + Send + 'static>(
        mut writer: R,
        more_threads: usize,
    ) -> BGzWriter {
        let pool = ThreadPoolBuilder::new()
            .num_threads(2 + more_threads)
            .build()
            .unwrap();
        let (s_buf, r_buf) = bounded(2 + 2 * more_threads);
        let (s_out, r_out) = bounded(1 + more_threads);
        let (s_end, r_end) = bounded(1);
        for _ in 0..2 + 2 * more_threads {
            s_buf.send(Ok(Vec::with_capacity(DEFAULT_BUFFER))).unwrap();
        }
        let send_buf = s_buf.clone();
        pool.spawn(move || {
            let mut write_block = |block: io::Result<(usize, Vec<u8>, u32)>| {
                let (buflen, mut compressed, crc): (usize, Vec<u8>, u32) = block?;
                let compressed_len = compressed.len() + 19 + 6;
                let header = [
                    0x1f,
                    0x8b,
                    0x08,
                    0x04,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x02,
                    0xff,
                    0x06,
                    0x00,
                    66,
                    67,
                    0x02,
                    0x00,
                    (compressed_len & 0xff) as u8,
                    ((compressed_len >> 8) & buflen) as u8,
                ];
                let wrote_bytes = writer.write(&header[..])?;
                if wrote_bytes != header.len() {
                    return Err(io::Error::new(io::ErrorKind::Other, "Cannot write header"));
                }

                let wrote_bytes = writer.write(&compressed[..])?;
                if wrote_bytes != compressed.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Cannot write compressed data",
                    ));
                }

                let crc_bytes = super::bytes_le_u32(crc);
                let wrote_bytes = writer.write(&crc_bytes[..])?;
                if wrote_bytes != crc_bytes.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Cannot write CRC data",
                    ));
                }

                let buflen_bytes = super::bytes_le_u32(buflen as u32);
                let wrote_bytes = writer.write(&buflen_bytes[..])?;
                if wrote_bytes != buflen_bytes.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Cannot write CRC data",
                    ));
                }

                compressed.clear();
                Ok(compressed)
            };
            let mut seq = 0u64;
            let mut queue = HashMap::new();
            for (i, block) in r_out.iter() {
                if i == seq {
                    send_buf.send(write_block(block)).unwrap();
                    seq += 1;
                    while let Some(block) = queue.remove(&seq) {
                        send_buf.send(write_block(block)).unwrap();
                        seq += 1;
                    }
                } else {
                    queue.insert(i, block);
                }
            }
            let mut func = move || {
                let eof_bytes = [
                    0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0x06, 0x00, 0x42,
                    0x43, 0x02, 0x00, 0x1b, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00,
                ];
                writer.write(&eof_bytes[..])?;
                writer.flush()?;
                Ok(vec![])
            };
            send_buf.send(func()).unwrap();
            s_end.send(()).unwrap();
        });
        BGzWriter {
            buffer: Vec::with_capacity(DEFAULT_BUFFER),
            pool,
            channels: Some((r_buf, s_buf, s_out)),
            r_end,
            seq: 0,
        }
    }
}

impl io::Write for BGzWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.buffer.len() + buf.len() >= DEFAULT_BUFFER {
            self.flush()?;
        }
        self.buffer.extend(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        let (r_buf, s_buf, s_out) = self.channels.as_mut().unwrap();
        let mut buffer: Vec<u8> = r_buf.recv().unwrap()?;
        let mut compressed = r_buf.recv().unwrap()?;
        let s_buf = s_buf.clone();
        let s_out = s_out.clone();
        std::mem::swap(&mut self.buffer, &mut buffer);
        let seq = self.seq;
        self.pool.spawn(move || {
            let func = || {
                let crc = {
                    let mut encoder =
                        CrcWriter::new(DeflateEncoder::new(&mut compressed, Compression::best()));
                    encoder.write(&buffer)?;
                    encoder.crc().sum()
                };
                let buflen = buffer.len();
                buffer.clear();
                s_buf.send(Ok(buffer)).unwrap();
                Ok((buflen, compressed, crc))
            };
            s_out.send((seq, func())).unwrap();
        });
        self.seq += 1;
        Ok(())
    }
}

impl Drop for BGzWriter {
    fn drop(&mut self) {
        self.flush().unwrap();
        {
            let r_buf = {
                let mut tmp = None;
                std::mem::swap(&mut self.channels, &mut tmp);
                tmp.unwrap().0
            };
            // ensure we've received all the outstanding buffers
            let _ = r_buf.iter().collect::<Vec<_>>();
        }
        // ensure the writer thread has finished
        let _ = self.r_end.recv().unwrap();
    }
}

#[cfg(test)]
mod test {
    use std::fs;
    use std::io;
    use std::io::prelude::*;

    #[test]
    fn test_writer() -> io::Result<()> {
        {
            let data = b"0123456789ABCDEF";
            let mut writer = super::BGzWriter::new(fs::File::create("tmp/test.gz")?, 3);
            for _ in 0..30000 {
                writer.write(&data[..])?;
            }
        }
        {
            let f = io::BufReader::new(fs::File::open("tmp/test.gz").unwrap());
            let mut reader = ::read::BGzReader::new(f).unwrap();

            let mut data = [0; 10];
            reader.seek(io::SeekFrom::Start(100)).unwrap();
            assert_eq!(10, reader.read(&mut data).unwrap());
            assert_eq!(b"456789ABCD", &data);

            // end of block
            reader.seek(io::SeekFrom::Start(65270)).unwrap();
            assert_eq!(10, reader.read(&mut data).unwrap());
            assert_eq!(b"6789ABCDEF", &data);

            // start of block
            reader.seek(io::SeekFrom::Start(65280)).unwrap();
            assert_eq!(10, reader.read(&mut data).unwrap());
            assert_eq!(b"0123456789", &data);

            // inter-block
            reader.seek(io::SeekFrom::Start(65275)).unwrap();
            assert_eq!(10, reader.read(&mut data).unwrap());
            assert_eq!(b"BCDEF01234", &data);

            // inter-block
            reader.seek(io::SeekFrom::Start(195835)).unwrap();
            assert_eq!(10, reader.read(&mut data).unwrap());
            assert_eq!(b"BCDEF01234", &data);

            // inter-block
            reader.seek(io::SeekFrom::Start(65270)).unwrap();
            assert_eq!(10, reader.read(&mut data).unwrap());
            assert_eq!(b"6789ABCDEF", &data);
            assert_eq!(10, reader.read(&mut data).unwrap());
            assert_eq!(b"0123456789", &data);

            // end of bgzip
            reader.seek(io::SeekFrom::Start(479995)).unwrap();
            assert_eq!(5, reader.read(&mut data).unwrap());
            assert_eq!(&b"BCDEF"[..], &data[..5]);

            let eof = reader.read(&mut data);
            assert_eq!(0, eof.unwrap());
            //assert_eq!(eof.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
        }

        Ok(())
    }
}
