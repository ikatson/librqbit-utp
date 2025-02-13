use std::{cmp::Ordering, task::Waker};

use anyhow::bail;
use tokio::sync::mpsc::{UnboundedSender, WeakUnboundedSender};

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

pub fn fill_buffer_from_slices(
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

    let [first, second] = prepare_2_ioslices(first, second, offset, len)?;
    out_buf[..first.len()].copy_from_slice(first);
    out_buf[first.len()..first.len() + second.len()].copy_from_slice(second);

    Ok(())
}

pub fn prepare_2_ioslices<'a>(
    first: &'a [u8],
    second: &'a [u8],
    offset: usize,
    len: usize,
) -> anyhow::Result<[&'a [u8]; 2]> {
    if len == 0 {
        return Ok([&[], &[]]);
    }

    let total_len = first.len() + second.len();
    if offset >= total_len {
        bail!("offset beyond buffer bounds");
    }
    if offset + len > total_len {
        bail!("requested length exceeds buffer bounds");
    }

    // Handle offset
    let (first_slice, second_slice) = if offset >= first.len() {
        // Offset points into second slice
        let second_offset = offset - first.len();
        (&[][..], &second[second_offset..])
    } else {
        // Offset points into first slice
        (&first[offset..], second)
    };

    // Handle length
    let remaining_in_first = first_slice.len();
    if len <= remaining_in_first {
        // All data can be taken from first slice
        Ok([&first_slice[..len], &[]])
    } else {
        // Need to take data from both slices
        let needed_from_second = len - remaining_in_first;
        Ok([first_slice, &second_slice[..needed_from_second]])
    }
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

pub struct FnDropGuard<F: FnOnce()> {
    f: Option<F>,
}

impl<F: FnOnce()> FnDropGuard<F> {
    pub fn new(f: F) -> Self {
        Self { f: Some(f) }
    }

    pub fn disarm(&mut self) {
        self.f = None;
    }
}

impl<F: FnOnce()> Drop for FnDropGuard<F> {
    fn drop(&mut self) {
        if let Some(f) = self.f.take() {
            f();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::prepare_2_ioslices;

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

    #[test]
    fn test_prepare_2_ioslices() {
        // Test empty request
        let result = prepare_2_ioslices(&[1, 2, 3], &[4, 5, 6], 0, 0).unwrap();
        assert_eq!(result, [&[][..], &[][..]]);

        // Test single slice scenarios
        let result = prepare_2_ioslices(&[1, 2, 3], &[4, 5, 6], 0, 2).unwrap();
        assert_eq!(result, [&[1, 2][..], &[][..]]);

        let result = prepare_2_ioslices(&[1, 2, 3], &[4, 5, 6], 1, 2).unwrap();
        assert_eq!(result, [&[2, 3][..], &[][..]]);

        // Test cross-slice scenarios
        let result = prepare_2_ioslices(&[1, 2, 3], &[4, 5, 6], 2, 2).unwrap();
        assert_eq!(result, [&[3][..], &[4][..]]);

        let result = prepare_2_ioslices(&[1, 2, 3], &[4, 5, 6], 3, 2).unwrap();
        assert_eq!(result, [&[][..], &[4, 5][..]]);

        // Test full length
        let result = prepare_2_ioslices(&[1, 2, 3], &[4, 5, 6], 0, 6).unwrap();
        assert_eq!(result, [&[1, 2, 3][..], &[4, 5, 6][..]]);

        // Test error cases
        assert!(prepare_2_ioslices(&[1, 2, 3], &[4, 5, 6], 7, 1).is_err()); // offset too large
        assert!(prepare_2_ioslices(&[1, 2, 3], &[4, 5, 6], 0, 7).is_err()); // length too large
        assert!(prepare_2_ioslices(&[1, 2, 3], &[4, 5, 6], 5, 2).is_err()); // offset + length too large
    }
}
