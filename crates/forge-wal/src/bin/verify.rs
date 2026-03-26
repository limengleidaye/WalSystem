use std::fs::File;
use std::io::{BufReader, Read};

use anyhow::Ok;

fn main() -> anyhow::Result<()> {
    let slab_capacity: usize = 256 * 1024;

    let file = File::open("wal.log")?;
    let mut reader = BufReader::new(file);
    let mut slab_buf = vec![0u8; slab_capacity];

    let mut total_records: u64 = 0;
    let mut last_seq: u32 = 0;
    let mut inversions: u64 = 0;
    let mut sum_errors: u64 = 0;
    let mut slab_index: usize = 0;

    loop {
        let n = reader.read(&mut slab_buf)?;
        if n == 0 {
            break;
        }
        if n < slab_capacity {
            slab_buf[n..].fill(0);
        }
        let mut off = 0usize;
        loop {
            if off + 2 > slab_capacity {
                break;
            }
            let count = u16::from_le_bytes(slab_buf[off..off + 2].try_into()?);
            if count == 0 {
                break;
            }
            off += 2;

            let record_size = 2 + count as usize * 4 + 4 + 4;
            if off - 2 + record_size > slab_capacity {
                eprintln!(
                    "slab {slab_index} offset {}: truncated record, count={count}",
                    off - 2
                );
                break;
            }

            let mut computed_sum: u32 = 0;
            for _ in 0..count {
                let v = u32::from_le_bytes(slab_buf[off..off + 4].try_into()?);
                computed_sum = computed_sum.wrapping_add(v);
                off += 4;
            }

            let sum = u32::from_le_bytes(slab_buf[off..off + 4].try_into()?);
            off += 4;

            let seq = u32::from_le_bytes(slab_buf[off..off + 4].try_into()?);
            off += 4;
            if sum != computed_sum {
                sum_errors += 1;
                eprintln!(
                    "record {total_records}: sum mismatch, expected={computed_sum}, got={sum}"
                );
            }

            if total_records > 0 && seq <= last_seq {
                inversions += 1;
                eprintln!("record {total_records}: seq inversion, last={last_seq}, got={seq}");
            }

            last_seq = seq;
            total_records += 1;
        }
    }

    println!("total records:   {total_records}");
    println!("sum errors:      {sum_errors}");
    println!("seq inversions:  {inversions}");
    println!("last seq:        {last_seq}");

    if sum_errors == 0 && inversions == 0 {
        println!("PASS");
    } else {
        println!("FAIL");
    }
    Ok(())
}
