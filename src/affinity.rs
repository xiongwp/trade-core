//! CPU affinity: pin matching shard threads to cores.
//!
//! Pinning a shard to a core keeps its books hot in that core's cache and avoids
//! scheduler migrations — a meaningful win for a busy matching thread.
//!
//! Platform reality, stated plainly:
//!
//! * **Linux**: `pthread_setaffinity_np` gives hard pinning. Fully supported.
//! * **macOS**: there is no hard-pinning API. `thread_policy_set` with
//!   `THREAD_AFFINITY_POLICY` is an advisory *affinity tag* hint, and Apple
//!   Silicon kernels reject it (`KERN_NOT_SUPPORTED`). We attempt it and report
//!   the result honestly; the system still runs fine unpinned.
//!
//! No external crates: the needed C symbols are declared directly.

/// Pin the *current* thread to `core`. Returns `Ok` on success or a
/// human-readable reason it did not apply.
pub fn pin_current_thread(core: usize) -> Result<(), String> {
    imp::pin_current_thread(core)
}

#[cfg(target_os = "linux")]
mod imp {
    // Minimal declarations of the pthread affinity API (glibc/musl).
    #[repr(C)]
    struct CpuSet {
        bits: [u64; 16], // 1024 CPUs, matches glibc cpu_set_t
    }
    extern "C" {
        fn pthread_self() -> usize;
        fn pthread_setaffinity_np(thread: usize, cpusetsize: usize, cpuset: *const CpuSet) -> i32;
    }

    pub fn pin_current_thread(core: usize) -> Result<(), String> {
        if core >= 1024 {
            return Err(format!("core {core} out of range"));
        }
        let mut set = CpuSet { bits: [0; 16] };
        set.bits[core / 64] |= 1u64 << (core % 64);
        let rc =
            unsafe { pthread_setaffinity_np(pthread_self(), std::mem::size_of::<CpuSet>(), &set) };
        if rc == 0 {
            Ok(())
        } else {
            Err(format!("pthread_setaffinity_np failed: {rc}"))
        }
    }
}

#[cfg(target_os = "macos")]
mod imp {
    // Advisory affinity tag via Mach thread policy. Threads sharing a tag are
    // hinted to share an L2; distinct tags hint separation. Apple Silicon
    // returns KERN_NOT_SUPPORTED (46); Intel Macs accept it.
    const THREAD_AFFINITY_POLICY: u32 = 4;
    const THREAD_AFFINITY_POLICY_COUNT: u32 = 1;

    extern "C" {
        fn mach_thread_self() -> u32;
        fn thread_policy_set(thread: u32, flavor: u32, policy_info: *mut i32, count: u32) -> i32;
    }

    pub fn pin_current_thread(core: usize) -> Result<(), String> {
        // Tag 0 means "no affinity", so offset by 1.
        let mut tag: i32 = core as i32 + 1;
        let kr = unsafe {
            thread_policy_set(
                mach_thread_self(),
                THREAD_AFFINITY_POLICY,
                &mut tag as *mut i32,
                THREAD_AFFINITY_POLICY_COUNT,
            )
        };
        match kr {
            0 => Ok(()),
            46 => Err(
                "macOS kernel does not support thread affinity on this hardware \
                       (KERN_NOT_SUPPORTED — expected on Apple Silicon); running unpinned"
                    .to_string(),
            ),
            other => Err(format!("thread_policy_set failed: kern_return {other}")),
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod imp {
    pub fn pin_current_thread(_core: usize) -> Result<(), String> {
        Err("CPU pinning not implemented for this platform".to_string())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn pin_attempt_does_not_crash() {
        // Success or a graceful, descriptive refusal — never a crash.
        match super::pin_current_thread(0) {
            Ok(()) => {}
            Err(reason) => assert!(!reason.is_empty()),
        }
    }
}
