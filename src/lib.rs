//!
//!  The API is designed around the following basic LVM objects:
//!  1) Physical Volume (pv_t) 2) Volume Group (vg_t) 3) Logical Volume (lv_t).
//!
//!  The library provides functions to list the objects in a system,
//!  get and set object properties (such as names, UUIDs, and sizes), as well
//!  as create/remove objects and perform more complex operations and
//!  transformations. Each object instance is represented by a handle, and
//!  handles are passed to and from the functions to perform the operations.
//!
//!  A central object in the library is the Volume Group, represented by the
//!  VG handle, vg_t. Performing an operation on a PV or LV object first
//!  requires obtaining a VG handle. Once the vg_t has been obtained, it can
//!  be used to enumerate the pv_t and lv_t objects within that vg_t. Attributes
//!  of these objects can then be queried or changed.
//!
//!  A volume group handle may be obtained with read or write permission.
//!  Any attempt to change a property of a pv_t, vg_t, or lv_t without
//!  obtaining write permission on the vg_t will fail with EPERM.
//!
//!  An application first opening a VG read-only, then later wanting to change
//!  a property of an object must first close the VG and re-open with write
//!  permission. Currently liblvm provides no mechanism to determine whether
//!  the VG has changed on-disk in between these operations - this is the
//!  application's responsiblity. One way the application can ensure the VG
//!  has not changed is to save the "vg_seqno" field after opening the VG with
//!  READ permission. If the application later needs to modify the VG, it can
//!  close the VG and re-open with WRITE permission. It should then check
//!  whether the original "vg_seqno" obtained with READ permission matches
//!  the new one obtained with WRITE permission.

extern crate errno;
extern crate lvm_sys;

use std::error::Error as err;
use std::ffi::{CStr, CString, NulError};
use std::fmt;
use std::io::Error as IOError;
use std::ptr;

use errno::Errno;
use lvm_sys::*;

pub type LvmResult<T> = Result<T, LvmError>;

/// Custom error handling
#[derive(Debug)]
pub enum LvmError {
    Error((Errno, String)),
    IoError(IOError),
    NulError(NulError),
}

impl fmt::Display for LvmError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.description())
    }
}

impl err for LvmError {
    fn description(&self) -> &str {
        match *self {
            LvmError::Error(ref e) => &e.1,
            LvmError::IoError(ref e) => e.description(),
            LvmError::NulError(ref e) => e.description(),
        }
    }
    fn cause(&self) -> Option<&err> {
        match *self {
            LvmError::Error(_) => None,
            LvmError::IoError(ref e) => e.cause(),
            LvmError::NulError(ref e) => e.cause(),
        }
    }
}

impl LvmError {
    /// Create a new LvmError with a String message
    pub fn new(err: (Errno, String)) -> LvmError {
        LvmError::Error((err.0, err.1))
    }
}

impl From<IOError> for LvmError {
    fn from(err: IOError) -> LvmError {
        LvmError::IoError(err)
    }
}

impl From<NulError> for LvmError {
    fn from(err: NulError) -> LvmError {
        LvmError::NulError(err)
    }
}

/*
struct dm_list {
    struct dm_list *n, 
    struct dm_list *p,
}

struct lvm_str_list {
    struct dm_list list;
    const char *str;
}
*/

macro_rules! dm_list_iterate {
    ($v:ty, $head:expr) => {
        let mut ptr: $v = (*$head).n;
        while ptr.field != (*$head) {
            (t *)((v) - &((t *) 0)->head)
        }
        /* 
        dm_list_iterate_items(v, head)
        dm_list_iterate_items_gen(v, head, field) \
        for (v = dm_list_struct_base((head)->n, __typeof__(*v), field); 
             &v->field != (head); 
             v = dm_list_struct_base(v->field.n, __typeof__(*v), field))

        #define dm_list_struct_base(v, t, head) \
            ((t *)((const char *)(v) - (const char *)&((t *) 0)->head))
        */
    } 
}

#[derive(Debug)]
pub struct Lvm {
    handle: lvm_t,
}

impl Drop for Lvm {
    fn drop(&mut self) {
        unsafe {
            lvm_quit(self.handle);
        }
    }
}

#[derive(Debug)]
pub struct VolumeGroup<'a> {
    handle: vg_t,
    lvm: &'a Lvm,
}

impl<'a> Drop for VolumeGroup<'a> {
    fn drop(&mut self) {
        unsafe {
            lvm_vg_close(self.handle);
        }
    }
}

#[derive(Debug)]
pub struct PhysicalVolume {
    handle: pv_t,
}

#[derive(Debug)]
pub struct LogicalVolume {
    handle: lv_t,
}

impl Lvm {
    fn get_error(&self) -> LvmResult<(Errno, String)> {
        let error = unsafe { lvm_errno(self.handle) };
        let msg = unsafe {
            CStr::from_ptr(lvm_errmsg(self.handle))
                .to_string_lossy()
                .into_owned()
        };

        Ok((Errno(error), msg))
    }

    /// use system_dir to set an alternative LVM system directory
    pub fn new(system_dir: Option<&str>) -> LvmResult<Self> {
        match system_dir {
            Some(s) => {
                let d = CString::new(s)?;

                unsafe {
                    let handle = lvm_init(d.as_ptr());
                    Ok(Lvm { handle })
                }
            }
            None => {
                let p = ptr::null();
                unsafe {
                    let handle = lvm_init(p);
                    Ok(Lvm { handle })
                }
            }
        }
    }

    pub fn list_volume_groups(&self) -> LvmResult<Vec<String>> {
        let mut names: Vec<String> = vec![];
        unsafe {
            let mut vg_names = lvm_list_vg_names(self.handle);
            dm_list_iterate!(lvm_str_list_t, vg_names);
            loop {
                println!("vg_names: {:p} vg_names.n:{:p} vg_names.p:{:p}", vg_names, (*vg_names).n, (*vg_names).p);
                if (*vg_names).p == vg_names {
                    break;
                }
                let name = CStr::from_ptr((*vg_names).p as *const i8)
                    .to_string_lossy()
                    .into_owned();
                println!("name: {}", name);
                names.push(name);
                //if (*vg_names).n.is_null() {
                    //break;
                //}
                vg_names = (*vg_names).n;
            }
        }

        Ok(names)
    }

    /// Scan all devices on the system for VGs and LVM metadata
    pub fn scan(&self) -> LvmResult<()> {
        unsafe {
            let retcode = lvm_scan(self.handle);
            if retcode < 0 {
                let err = self.get_error()?;
                return Err(LvmError::new((err.0, err.1)));
            }
        }
        Ok(())
    }
}
