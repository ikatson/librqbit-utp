use std::{cmp::Ordering, future::Future, task::Waker};

use anyhow::bail;
use smoltcp::storage::RingBuffer;

pub fn spawn_print_error(f: impl Future<Output = anyhow::Result<()>> + Send + 'static) {
    tokio::spawn(async move {
        if let Err(e) = f.await {
            tracing::debug!("error: {e:#}");
        }
    });
}

pub fn update_optional_waker(waker: &mut Option<Waker>, cx: &std::task::Context<'_>) {
    match waker.as_mut() {
        Some(w) => {
            if w.will_wake(cx.waker()) {
                return;
            }
            *w = cx.waker().clone();
        }
        None => {
            waker.replace(cx.waker().clone());
        }
    }
}

pub fn seq_nr_offset(new: u16, old: u16, wrap_tolerance: u16) -> isize {
    match new.cmp(&old) {
        Ordering::Less => {
            // new is less. check if "new" wrapped within tolerance
            if new.wrapping_sub(old) <= wrap_tolerance {
                return new.wrapping_sub(old) as isize;
            }
            -((old - new) as isize)
        }
        Ordering::Equal => 0,
        Ordering::Greater => {
            // old is less. check if "old" wrapped within tolerance
            if old.wrapping_sub(new) <= wrap_tolerance {
                return -(old.wrapping_sub(new) as isize);
            }
            (new - old) as isize
        }
    }
}

pub fn fill_buffer_from_rb(
    out_buf: &mut [u8],
    rb: &RingBuffer<'static, u8>,
    offset: usize,
    len: usize,
) -> anyhow::Result<()> {
    if out_buf.len() < len {
        bail!(
            "too small buffer: out_buf.len() < len ({} < {})",
            out_buf.len(),
            len
        )
    }

    let mut out_buf = out_buf;
    let mut current_offset = offset;
    let mut remaining = len;

    while remaining > 0 {
        let chunk = rb.get_allocated(current_offset, remaining);
        if chunk.is_empty() {
            bail!(
                "not enough data in ring buffer. rb.len={} requested_offset={} requested_len={}",
                rb.len(),
                offset,
                len
            );
        }
        out_buf[..chunk.len()].copy_from_slice(chunk);
        out_buf = &mut out_buf[chunk.len()..];
        remaining -= chunk.len();
        current_offset += chunk.len()
    }

    Ok(())
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_seq_nr_offset() {
        use super::seq_nr_offset;

        // no wraps
        assert_eq!(seq_nr_offset(2, 1, 1024), 1);
        assert_eq!(seq_nr_offset(1, 1, 1024), 0);
        assert_eq!(seq_nr_offset(0, 1, 1024), -1);

        // new wraps within tolerance
        assert_eq!(seq_nr_offset(0, u16::MAX, 1024), 1);
        assert_eq!(seq_nr_offset(1023, u16::MAX, 1024), 1024);

        // old wraps within tolerance
        assert_eq!(seq_nr_offset(u16::MAX, 0, 1024), -1);
        assert_eq!(seq_nr_offset(u16::MAX, 1023, 1024), -1024);

        // new wraps outside tolerance
        assert_eq!(
            seq_nr_offset(1024, u16::MAX, 1024),
            -(u16::MAX as isize - 1024)
        );

        // old wraps outside tolerance
        assert_eq!(
            seq_nr_offset(u16::MAX, 1024, 1024),
            u16::MAX as isize - 1024
        );
    }
}
