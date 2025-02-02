use std::{cmp::Ordering, collections::VecDeque, future::Future, task::Waker};

use anyhow::bail;
use tokio::sync::mpsc::{UnboundedSender, WeakUnboundedSender};
use tracing::{error, info, trace, warn, Instrument, Level};

pub fn spawn_print_error(
    span: tracing::Span,
    f: impl Future<Output = anyhow::Result<()>> + Send + 'static,
) {
    tokio::spawn(
        async move {
            if let Err(e) = f.await {
                tracing::debug!("error: {e:#}");
            }
        }
        .instrument(span),
    );
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

fn fill_buffer_from_slices(
    out_buf: &mut [u8],
    offset: usize,
    len: usize,
    first: &[u8],
    second: &[u8],
) -> anyhow::Result<()> {
    if out_buf.len() < len {
        bail!(
            "too small buffer: out_buf.len() < len ({} < {})",
            out_buf.len(),
            len
        )
    }

    let mut offset = offset;
    let mut out_buf = out_buf;
    let mut remaining = len;

    for mut chunk in [first, second] {
        // Advance until offset is found
        if offset > 0 {
            if chunk.len() < offset {
                offset -= chunk.len();
                continue;
            } else {
                chunk = &chunk[offset..];
                offset = 0;
            }
        }

        assert_eq!(offset, 0);
        let advance = remaining.min(chunk.len());
        out_buf[..advance].copy_from_slice(&chunk[..advance]);
        out_buf = &mut out_buf[advance..];
        remaining -= advance;
    }

    if remaining > 0 {
        bail!("not enough data in ring buffer");
    }

    Ok(())
}

pub fn fill_buffer_from_rb(
    out_buf: &mut [u8],
    rb: &VecDeque<u8>,
    offset: usize,
    len: usize,
) -> anyhow::Result<()> {
    let (first, second) = rb.as_slices();
    fill_buffer_from_slices(out_buf, offset, len, first, second)
}

pub(crate) struct DropGuardSendBeforeDeath<Msg> {
    msg: Option<Msg>,
    tx: WeakUnboundedSender<Msg>,
}

impl<Msg> DropGuardSendBeforeDeath<Msg> {
    pub fn new(msg: Msg, tx: &UnboundedSender<Msg>) -> Self {
        Self {
            msg: Some(msg),
            tx: tx.downgrade(),
        }
    }

    pub fn disarm(&mut self) {
        self.msg = None;
    }
}

impl<Msg> Drop for DropGuardSendBeforeDeath<Msg> {
    fn drop(&mut self) {
        if let Some(msg) = self.msg.take() {
            if let Some(tx) = self.tx.upgrade() {
                let _ = tx.send(msg);
            }
        }
    }
}

#[inline(always)]
pub fn run_before_and_after_if_changed<
    'a,
    Object: 'a,
    Value: PartialEq + Copy + std::fmt::Debug + 'static,
    ChangeResult,
>(
    obj: &mut Object,
    calc: impl Fn(&Object) -> Value,
    maybe_change: impl FnOnce(&mut Object) -> ChangeResult,
    callback: impl FnOnce(&Object, &Value, &Value),
) -> ChangeResult {
    let before = calc(obj);
    let result = maybe_change(obj);
    let after = calc(obj);
    if before != after {
        callback(obj, &before, &after);
    }
    result
}

#[inline(always)]
pub fn log_before_and_after_if_changed<
    'a,
    Object: 'a,
    Value: PartialEq + Copy + std::fmt::Debug + 'static,
    ChangeResult,
>(
    name: &'static str,
    obj: &mut Object,
    calc: impl Fn(&Object) -> Value,
    maybe_change: impl FnOnce(&mut Object) -> ChangeResult,
    calc_level: impl Fn(&Value, &Value) -> Level,
) -> ChangeResult {
    let before = calc(obj);
    let result = maybe_change(obj);
    let after = calc(obj);
    if before != after {
        let level = calc_level(&before, &after);
        if level == Level::TRACE {
            trace!(?before, ?after, "{name} changed");
        } else if level == Level::DEBUG {
            trace!(?before, ?after, "{name} changed");
        } else if level == Level::INFO {
            info!(?before, ?after, "{name} changed");
        } else if level == Level::WARN {
            warn!(?before, ?after, "{name} changed");
        } else if level == Level::ERROR {
            error!(?before, ?after, "{name} changed");
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::fill_buffer_from_slices;

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

    #[test]
    fn test_fill_buffer_from_rb() {
        const E: u8 = 255u8;
        let mut buf = [E; 10];

        fill_buffer_from_slices(&mut buf, 0, 0, &[], &[]).unwrap();
        assert_eq!(buf, [E; 10]);

        assert!(fill_buffer_from_slices(&mut buf, 0, 1, &[], &[]).is_err());

        buf = [E; 10];
        assert!(fill_buffer_from_slices(&mut buf, 0, 1, &[1, 2, 3], &[4, 5, 6]).is_ok());
        assert_eq!(buf, [1, E, E, E, E, E, E, E, E, E]);

        buf = [E; 10];
        assert!(fill_buffer_from_slices(&mut buf, 0, 3, &[1, 2, 3], &[4, 5, 6]).is_ok());
        assert_eq!(buf, [1, 2, 3, E, E, E, E, E, E, E]);

        buf = [E; 10];
        assert!(fill_buffer_from_slices(&mut buf, 0, 4, &[1, 2, 3], &[4, 5, 6]).is_ok());
        assert_eq!(buf, [1, 2, 3, 4, E, E, E, E, E, E]);

        buf = [E; 10];
        assert!(fill_buffer_from_slices(&mut buf, 0, 4, &[1, 2, 3], &[4, 5, 6]).is_ok());
        assert_eq!(buf, [1, 2, 3, 4, E, E, E, E, E, E]);

        buf = [E; 10];
        assert!(fill_buffer_from_slices(&mut buf, 1, 3, &[1, 2, 3], &[4, 5, 6]).is_ok());
        assert_eq!(buf, [2, 3, 4, E, E, E, E, E, E, E]);

        buf = [E; 10];
        assert!(fill_buffer_from_slices(&mut buf, 1, 4, &[1, 2, 3], &[4, 5, 6]).is_ok());
        assert_eq!(buf, [2, 3, 4, 5, E, E, E, E, E, E]);

        buf = [E; 10];
        assert!(fill_buffer_from_slices(&mut buf, 1, 5, &[1, 2, 3], &[4, 5, 6]).is_ok());
        assert_eq!(buf, [2, 3, 4, 5, 6, E, E, E, E, E]);

        buf = [E; 10];
        assert!(fill_buffer_from_slices(&mut buf, 1, 6, &[1, 2, 3], &[4, 5, 6]).is_err());

        buf = [E; 10];
        assert!(fill_buffer_from_slices(&mut buf, 3, 0, &[1, 2, 3], &[4, 5, 6]).is_ok());
        assert_eq!(buf, [E, E, E, E, E, E, E, E, E, E]);

        buf = [E; 10];
        assert!(fill_buffer_from_slices(&mut buf, 3, 1, &[1, 2, 3], &[4, 5, 6]).is_ok());
        assert_eq!(buf, [4, E, E, E, E, E, E, E, E, E]);

        buf = [E; 10];
        assert!(fill_buffer_from_slices(&mut buf, 3, 3, &[1, 2, 3], &[4, 5, 6]).is_ok());
        assert_eq!(buf, [4, 5, 6, E, E, E, E, E, E, E]);

        buf = [E; 10];
        assert!(fill_buffer_from_slices(&mut buf, 5, 1, &[1, 2, 3], &[4, 5, 6]).is_ok());
        assert_eq!(buf, [6, E, E, E, E, E, E, E, E, E]);

        buf = [E; 10];
        assert!(fill_buffer_from_slices(&mut buf, 5, 2, &[1, 2, 3], &[4, 5, 6]).is_err());

        buf = [E; 10];
        assert!(fill_buffer_from_slices(&mut buf, 5, 2, &[1, 2, 3], &[4, 5, 6]).is_err());

        buf = [E; 10];
        assert!(fill_buffer_from_slices(&mut buf, 6, 0, &[1, 2, 3], &[4, 5, 6]).is_ok());
        assert_eq!(buf, [E, E, E, E, E, E, E, E, E, E]);

        buf = [E; 10];
        assert!(fill_buffer_from_slices(&mut buf, 6, 1, &[1, 2, 3], &[4, 5, 6]).is_err());
    }
}
