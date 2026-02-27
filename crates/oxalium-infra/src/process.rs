use bytesize::ByteSize;

#[inline]
pub fn get_process_peak_memory_usage() -> ByteSize {
    ByteSize::b(imp::get_process_peak_memory_usage())
}

#[cfg(target_os = "windows")]
mod imp {
    use std::mem::zeroed;

    use windows_sys::Win32::System::{
        ProcessStatus::GetProcessMemoryInfo, Threading::GetCurrentProcess,
    };

    use crate::convert::IntoLossy;

    pub(super) fn get_process_peak_memory_usage() -> u64 {
        unsafe {
            let mut pmcs = zeroed();
            GetProcessMemoryInfo(GetCurrentProcess(), &mut pmcs, 0x48);
            pmcs.PeakWorkingSetSize.into_lossy()
        }
    }
}

#[cfg(target_os = "linux")]
mod imp {
    use std::fs::read_to_string;

    use libc::getpid;

    pub(super) fn get_process_peak_memory_usage() -> u64 {
        unsafe {
            let mut value = 0;
            if let Ok(contents) =
                read_to_string(format!("/proc/{}/status", getpid().cast_unsigned()))
            {
                for line in contents.lines() {
                    if line.starts_with("VmHWM:") {
                        if let Ok(v) = line.strip_suffix("kB").unwrap_or(line)[6..].trim().parse() {
                            value = v;
                            break;
                        }
                    }
                }
            }
            1024 * value
        }
    }
}
