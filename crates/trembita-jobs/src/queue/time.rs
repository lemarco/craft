use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub(super) fn unix_ms_now() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

pub(super) fn unix_ms_from_instant(deadline: Instant) -> u64 {
    let now_ms = unix_ms_now();
    let now = Instant::now();
    if deadline <= now {
        now_ms
    } else {
        now_ms.saturating_add(
            u64::try_from(deadline.duration_since(now).as_millis()).unwrap_or(u64::MAX),
        )
    }
}

pub(super) fn instant_from_unix_ms(ms: u64) -> Instant {
    let now_ms = unix_ms_now();
    let now = Instant::now();
    if ms <= now_ms {
        now
    } else {
        now + Duration::from_millis(ms - now_ms)
    }
}
