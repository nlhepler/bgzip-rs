//! [`Write`] stream to create bgzip format file.
//!
//! [`Write`]: https://doc.rust-lang.org/std/io/trait.Write.html
//!
//! # Example
//! ```
//! use bgzip::write::BGzBuilder;
//! use std::fs;
//! use std::io;
//! use std::io::prelude::*;
//!
//! # fn main() { let _ = run(); }
//! # fn run() -> io::Result<()> {
//! let data = b"0123456789ABCDEF";
//! let mut writer = BGzBuilder::new().write(fs::File::create("tmp/test2.gz")?);
//!
//! for _ in 0..30000 {
//!     writer.write(&data[..])?;
//! }
//! writer.finish()?;
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

pub struct BGzBuilder {
    lvl: Compression,
    more_threads: usize,
}

impl BGzBuilder {
    pub fn new() -> Self {
        BGzBuilder {
            lvl: Compression::default(),
            more_threads: 0,
        }
    }

    pub fn lvl(mut self, lvl: Compression) -> Self {
        self.lvl = lvl;
        self
    }

    pub fn more_threads(mut self, more_threads: usize) -> Self {
        self.more_threads = more_threads;
        self
    }

    pub fn write<W: io::Write + Sync + Send + 'static>(self, w: W) -> BGzWriter {
        let BGzBuilder { lvl, more_threads } = self;
        BGzWriter::new(w, more_threads, lvl)
    }
}

#[derive(Debug)]
pub struct BGzWriter {
    buffer: Vec<u8>,
    pool: ThreadPool,
    channels: Option<(
        Receiver<io::Result<Vec<u8>>>,
        Sender<io::Result<Vec<u8>>>,
        Sender<(u64, Vec<u8>)>,
        Sender<(u64, io::Result<(usize, Vec<u8>, u32)>)>,
        Receiver<()>,
    )>,
    seq: u64,
}

impl BGzWriter {
    fn new<R: io::Write + Sync + Send + 'static>(
        mut writer: R,
        more_threads: usize,
        lvl: Compression,
    ) -> BGzWriter {
        let pool = ThreadPoolBuilder::new()
            .num_threads(2 + more_threads)
            .build()
            .unwrap();
        // 2 each for compressors and main, 1 for writer
        let (s_buf, r_buf): (Sender<io::Result<Vec<u8>>>, Receiver<io::Result<Vec<u8>>>) =
            bounded(3 + 2 * more_threads);
        // 1 for each compressor
        let (s_enc, r_enc): (Sender<(u64, Vec<u8>)>, Receiver<(u64, Vec<u8>)>) =
            bounded(1 + more_threads);
        let (s_out, r_out): (
            Sender<(u64, io::Result<(usize, Vec<u8>, u32)>)>,
            Receiver<(u64, io::Result<(usize, Vec<u8>, u32)>)>,
        ) = bounded(1 + more_threads);
        let (s_end, r_end): (Sender<()>, Receiver<()>) = bounded(2 + more_threads);
        for _ in 0..3 + more_threads {
            s_buf.send(Ok(Vec::with_capacity(DEFAULT_BUFFER))).unwrap();
        }
        for _ in 0..1 + more_threads {
            let r_enc = r_enc.clone();
            let s_out = s_out.clone();
            let s_end = s_end.clone();
            pool.spawn(move || {
                let mut encoder =
                    Some(DeflateEncoder::new(Vec::with_capacity(DEFAULT_BUFFER), lvl));
                let mut compress = move |mut buffer: Vec<u8>| {
                    let mut inner = None;
                    std::mem::swap(&mut inner, &mut encoder);
                    let mut crc = CrcWriter::new(inner.unwrap());
                    crc.write(&buffer)?;
                    let cksum = crc.crc().sum();
                    inner = Some(crc.into_inner());
                    std::mem::swap(&mut inner, &mut encoder);
                    let buflen = buffer.len();
                    buffer.clear();
                    let compressed = encoder.as_mut().unwrap().reset(buffer)?;
                    Ok((buflen, compressed, cksum))
                };
                for (i, buffer) in r_enc.iter() {
                    let compressed_result = compress(buffer);
                    s_out.send((i, compressed_result)).unwrap();
                }
                s_end.send(()).unwrap();
            });
        }
        {
            let s_buf = s_buf.clone();
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
                    writer.write_all(&header[..])?;
                    writer.write_all(&compressed[..])?;

                    let crc_bytes = super::bytes_le_u32(crc);
                    writer.write_all(&crc_bytes[..])?;

                    let buflen_bytes = super::bytes_le_u32(buflen as u32);
                    writer.write_all(&buflen_bytes[..])?;

                    compressed.clear();
                    Ok(compressed)
                };
                let mut seq = 0u64;
                let mut queue = HashMap::new();
                for (i, block) in r_out.iter() {
                    if i == seq {
                        let buffer = write_block(block);
                        s_buf.send(buffer).unwrap();
                        seq += 1;
                        while let Some(block) = queue.remove(&seq) {
                            let buffer = write_block(block);
                            s_buf.send(buffer).unwrap();
                            seq += 1;
                        }
                    } else {
                        queue.insert(i, block);
                    }
                }
                let mut func = move || {
                    let eof_bytes = [
                        0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0x06, 0x00,
                        0x42, 0x43, 0x02, 0x00, 0x1b, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00,
                    ];
                    writer.write(&eof_bytes[..])?;
                    writer.flush()?;
                    Ok(vec![])
                };
                s_buf.send(func()).unwrap();
                s_end.send(()).unwrap();
            });
        }
        BGzWriter {
            buffer: Vec::with_capacity(DEFAULT_BUFFER),
            pool,
            channels: Some((r_buf, s_buf, s_enc, s_out, r_end)),
            seq: 0,
        }
    }

    pub fn finish(&mut self) -> io::Result<()> {
        if self.channels.is_some() {
            self.flush()?;
            let r_end = {
                let (r_buf, r_end) = {
                    let mut tmp = None;
                    std::mem::swap(&mut self.channels, &mut tmp);
                    let (r_buf, _, _, _, r_end) = tmp.unwrap();
                    (r_buf, r_end)
                };
                // ensure we've received all the outstanding buffers
                for x in r_buf.iter() {
                    x?;
                }
                r_end
            };
            // ensure the compressor and writer thread have finished
            for _ in r_end.iter() {}
        }
        Ok(())
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
        if !self.buffer.is_empty() {
            let (r_buf, _, s_enc, _, _) = self.channels.as_mut().unwrap();
            let mut buffer: Vec<u8> = r_buf.recv().unwrap()?;
            std::mem::swap(&mut self.buffer, &mut buffer);
            s_enc.send((self.seq, buffer)).unwrap();
            self.seq += 1;
        }
        Ok(())
    }
}

impl Drop for BGzWriter {
    fn drop(&mut self) {
        // ideally the user calls finish() before this drops
        self.finish().unwrap();
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
            let mut writer = super::BGzBuilder::new()
                .more_threads(3)
                .write(fs::File::create("tmp/test.gz")?);

            let data = b"0123456789ABCDEF";
            for _ in 0..30000 {
                writer.write(&data[..])?;
            }
            writer.finish()?;
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
