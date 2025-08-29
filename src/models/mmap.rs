use std::fs::File;
use std::io::{self, Error, ErrorKind};
use std::os::unix::io::AsRawFd;
use std::ptr;

#[derive(Debug)]
pub struct MemMap<const ALIGN: usize> {
    pub ptr: *mut u8,
    pub len: usize
}

// SAFETY: MemMap is safe to send between threads because the mmap is read-only
unsafe impl<const ALIGN: usize> Send for MemMap<ALIGN> {}

// SAFETY: MemMap is safe to share between threads because the mmap is read-only
unsafe impl<const ALIGN: usize> Sync for MemMap<ALIGN> {}

impl<const ALIGN: usize> MemMap<{ ALIGN }> {
    pub fn open(path: &str, offset: usize, len: usize) -> io::Result<Self> {
        if offset % ALIGN != 0 {
            return Err(Error::new(ErrorKind::InvalidInput, "File offset must be 64-byte aligned"));
        }
        let file = File::open(path)?;
        let fd = file.as_raw_fd();

        // mmap must use an offset that's a multiple of page size
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
        let map_offset = offset & !(page_size - 1);
        let offset_in_page = offset - map_offset;
        let map_len = offset_in_page + len;

        let ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                map_len,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                fd,
                map_offset as libc::off_t
            )
        };

        if ptr == libc::MAP_FAILED {
            return Err(Error::last_os_error());
        }

        let region_ptr = unsafe { (ptr as *mut u8).add(offset_in_page) };
        // Confirm alignment
        if (region_ptr as usize) % ALIGN != 0 {
            // Unmap and error
            unsafe { libc::munmap(ptr, map_len) };
            return Err(Error::new(
                ErrorKind::Other,
                format!("MMAP region is not {ALIGN}-byte aligned (ptr = {:p})", region_ptr)
            ));
        }

        Ok(Self { ptr: region_ptr, len })
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl<const ALIGN: usize> AsRef<[u8]> for MemMap<{ ALIGN }> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<const ALIGN: usize> std::ops::Deref for MemMap<{ ALIGN }> {
    type Target = [u8];
    
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<const ALIGN: usize> Drop for MemMap<{ ALIGN }> {
    fn drop(&mut self) {
        // Compute the base pointer for unmapping
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
        let ptr_val = self.ptr as usize;
        let page_base = ptr_val & !(page_size - 1);
        let offset_in_page = ptr_val - page_base;
        let map_len = offset_in_page + self.len;
        unsafe {
            libc::munmap(page_base as *mut libc::c_void, map_len);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALIGN: usize = 64;

    fn pseudo_rand() -> u32 {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let nanos = now.as_nanos() as u64;
        ((nanos ^ (nanos >> 32)) as u32).wrapping_mul(1664525).wrapping_add(1013904223)
    }
    
    fn create_temp_file_with_data(data: &[u8], pad: usize) -> std::path::PathBuf {
        use std::fs::File;
        use std::io::Write;
        use std::env::temp_dir;
    
        let mut path = temp_dir();
        path.push(format!("mmap_test_{}.bin", pseudo_rand()));
    
        let mut file = File::create(&path).expect("Failed to create temp file");
    
        if pad > 0 {
            let pad_bytes = vec![0u8; pad];
            file.write_all(&pad_bytes).expect("Pad write failed");
        }
        file.write_all(data).expect("Write failed");
        file.sync_all().unwrap();
        path
    }

    #[test]
    fn test_mmap_64_aligned() {
        let data = b"abcdefghijklmnopqrstuvwxyz0123456789";
        let path = create_temp_file_with_data(data, 0);

        // offset 0, length = data.len()
        let map = MemMap::<ALIGN>::open(path.to_str().unwrap(), 0, data.len()).expect("mmap failed");
        assert_eq!(map.as_slice(), data);
        assert_eq!((map.as_slice().as_ptr() as usize) % ALIGN, 0);

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_mmap_nonzero_aligned_offset() {
        let data = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        let pad = 128; // ensure >1 page and 64-aligned
        let path = create_temp_file_with_data(data, pad);

        let offset = pad;
        assert_eq!(offset % ALIGN, 0, "Offset must be 64-aligned for test");

        let map = MemMap::<ALIGN>::open(path.to_str().unwrap(), offset, data.len()).expect("mmap failed");
        assert_eq!(map.as_slice(), data);
        assert_eq!((map.as_slice().as_ptr() as usize) % ALIGN, 0);

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_mmap_unaligned_offset_error() {
        let data = b"hello world";
        let pad = 11; // not 64 aligned
        let path = create_temp_file_with_data(data, pad);

        let offset = pad;
        assert_ne!(offset % ALIGN, 0);

        let res = MemMap::<ALIGN>::open(path.to_str().unwrap(), offset, data.len());
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_mmap_alignment_is_enforced() {
        let data = b"12345678";
        let path = create_temp_file_with_data(data, 0);

        // Should succeed for 64
        let m = MemMap::<64>::open(path.to_str().unwrap(), 0, data.len()).unwrap();
        assert_eq!((m.as_slice().as_ptr() as usize) % 64, 0);

        // Should succeed for 8
        let m8 = MemMap::<8>::open(path.to_str().unwrap(), 0, data.len()).unwrap();
        assert_eq!((m8.as_slice().as_ptr() as usize) % 8, 0);

        std::fs::remove_file(path).unwrap();
    }
}
