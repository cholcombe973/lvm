//!  Taken from the lvm2app.h header file:
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

use errno;
#[macro_use]
extern crate log;

use uuid;

use std::error::Error as err;
use std::ffi::{CStr, CString, NulError};
use std::fmt;
use std::io::Error as IOError;
use std::path::Path;
use std::ptr;
use std::str::FromStr;

use errno::Errno;
use lvm_sys::*;
use uuid::Uuid;

pub type LvmResult<T> = Result<T, LvmError>;

/// Custom error handling
#[derive(Debug)]
pub enum LvmError {
    Error((Errno, String)),
    IoError(IOError),
    NulError(NulError),
    ParseError(uuid::Error),
}

impl fmt::Display for LvmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.description())
    }
}

impl err for LvmError {
    fn description(&self) -> &str {
        match *self {
            LvmError::Error(ref e) => &e.1,
            LvmError::IoError(ref e) => e.description(),
            LvmError::NulError(ref e) => e.description(),
            LvmError::ParseError(ref e) => e.description(),
        }
    }
    fn cause(&self) -> Option<&dyn err> {
        match *self {
            LvmError::Error(_) => None,
            LvmError::IoError(ref e) => e.cause(),
            LvmError::NulError(ref e) => e.cause(),
            LvmError::ParseError(ref e) => e.cause(),
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

impl From<uuid::Error> for LvmError {
    fn from(err: uuid::Error) -> LvmError {
        LvmError::ParseError(err)
    }
}

#[derive(Debug)]
pub struct Lvm {
    handle: lvm_t,
}

impl Drop for Lvm {
    fn drop(&mut self) {
        unsafe {
            if !self.handle.is_null() {
                debug!("dropping lvm");
                lvm_quit(self.handle);
            }
        }
    }
}

#[derive(Debug)]
pub enum OpenMode {
    Read,
    Write,
}

impl ToString for OpenMode {
    fn to_string(&self) -> String {
        match self {
            OpenMode::Read => "r".into(),
            OpenMode::Write => "w".into(),
        }
    }
}

/// Thin provisioning discard policies
#[derive(Debug)]
pub enum LvmThinPolicy {
    Ignore,
    NoPassdown,
    Passdown,
}

impl ToString for LvmThinPolicy {
    fn to_string(&self) -> String {
        match self {
            LvmThinPolicy::Ignore => "LVM_THIN_DISCARDS_IGNORE".into(),
            LvmThinPolicy::NoPassdown => "LVM_THIN_DISCARDS_NO_PASSDOWN".into(),
            LvmThinPolicy::Passdown => "LVM_THIN_DISCARDS_PASSDOWN".into(),
        }
    }
}

#[derive(Debug)]
pub enum Property {
    /// zero indicates use detected size of device
    Size(u64),
    /// Number of metadata copies (0,1,2)
    PvMetaDataCopies(u8),
    /// The approx. size to be to be set aside for metadata
    PvMetaDatasize(u64),
    /// Align the start of the data to a multiple of this number
    DataAlignment(u64),
    /// Shift the start of the data area by this addl. offset
    DataAlignmentOffset(u64),
    /// Set to true to zero out first 2048 bytes of device, false to not
    Zero(bool),
}

impl ToString for Property {
    fn to_string(&self) -> String {
        match self {
            Property::Size(_) => "size".into(),
            Property::PvMetaDataCopies(_) => "pvmetadatacopies".into(),
            Property::PvMetaDatasize(_) => "pvmetadatasize".into(),
            Property::DataAlignment(_) => "data_alignment".into(),
            Property::DataAlignmentOffset(_) => "".into(),
            Property::Zero(_) => "zero".into(),
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
            if !self.handle.is_null() {
                debug!("dropping vg");
                lvm_vg_close(self.handle);
            }
        }
    }
}

#[derive(Debug)]
pub struct LvmPropertyValue {
    pub is_settable: bool,
    pub is_string: bool,
    pub is_integer: bool,
    pub is_signed: bool,
}

#[derive(Debug)]
pub struct PhysicalVolume<'a> {
    handle: pv_t,
    lvm: &'a Lvm,
}

pub struct PhysicalVolumeCreateParameters<'a> {
    handle: pv_create_params_t,
    property_value: Option<lvm_property_value>,
    lvm: &'a Lvm,
}

#[derive(Debug)]
pub struct LogicalVolume<'b, 'a: 'b> {
    handle: lv_t,
    lvm: &'a Lvm,
    vg: &'b VolumeGroup<'b>,
}

impl<'a, 'b> LogicalVolume<'a, 'b> {
    fn check_retcode(&self, retcode: i32) -> LvmResult<()> {
        if retcode < 0 {
            let err = self.lvm.get_error()?;
            return Err(LvmError::new((err.0, err.1)));
        }
        Ok(())
    }

    /// Activate a logical volume
    pub fn activate(&self) -> LvmResult<()> {
        unsafe {
            let retcode = lvm_lv_activate(self.handle);
            self.check_retcode(retcode)?;
            Ok(())
        }
    }

    pub fn add_tag(&self, name: &str) -> LvmResult<()> {
        let name = CString::new(name)?;
        unsafe {
            let retcode = lvm_lv_add_tag(self.handle, name.as_ptr());
            self.check_retcode(retcode)?;
            self.vg.write()?;
            Ok(())
        }
    }

    /// Deactivate a logical volume
    pub fn deactivate(&self) -> LvmResult<()> {
        unsafe {
            let retcode = lvm_lv_deactivate(self.handle);
            self.check_retcode(retcode)?;
            Ok(())
        }
    }

    /// Get the attributes of a logical volume
    pub fn get_attributes(&self) -> String {
        unsafe {
            let ptr = lvm_lv_get_attr(self.handle);
            let attrs_str = CStr::from_ptr(ptr).to_string_lossy();
            attrs_str.into_owned()
        }
    }

    /// Get the current name of a logical volume
    pub fn get_name(&self) -> String {
        unsafe {
            let name = lvm_lv_get_name(self.handle);
            let name_str = CStr::from_ptr(name).to_string_lossy();
            name_str.into_owned()
        }
    }

    /// Get the origin of a snapshot
    pub fn get_origin(&self) -> Option<String> {
        unsafe {
            let ptr = lvm_lv_get_origin(self.handle);
            if ptr.is_null() {
                return None;
            }
            let origin = CStr::from_ptr(ptr).to_string_lossy();
            Some(origin.into_owned())
        }
    }

    /// Get the current size in bytes of a logical volume
    pub fn get_size(&self) -> u64 {
        unsafe { lvm_lv_get_size(self.handle) }
    }

    pub fn get_tags(&self) -> LvmResult<Vec<String>> {
        let mut names: Vec<String> = vec![];
        unsafe {
            let tag_head = lvm_lv_get_tags(self.handle);
            let mut tag = dm_list_first(tag_head);
            loop {
                if tag.is_null() {
                    break;
                }
                let str_list = tag as *mut lvm_str_list;
                let name = CStr::from_ptr((*str_list).str)
                    .to_string_lossy()
                    .into_owned();
                names.push(name);
                tag = dm_list_next(tag_head, tag);
            }
        }

        Ok(names)
    }

    /// Get the current name of a logical volume
    pub fn get_uuid(&self) -> String {
        unsafe {
            let uuid = lvm_lv_get_uuid(self.handle);
            let name = CStr::from_ptr(uuid).to_string_lossy();

            name.into_owned()
        }
    }

    pub fn is_active(&self) -> bool {
        unsafe {
            let active = lvm_lv_is_active(self.handle);
            active == 1
        }
    }

    pub fn is_suspended(&self) -> bool {
        unsafe {
            let suspended = lvm_lv_is_suspended(self.handle);
            suspended == 1
        }
    }

    /// Remove a logical volume from a volume group
    pub fn remove(&self) -> LvmResult<()> {
        unsafe {
            let retcode = lvm_vg_remove_lv(self.handle);
            self.check_retcode(retcode)?;
            Ok(())
        }
    }

    pub fn remove_tag(&self, name: &str) -> LvmResult<()> {
        let name = CString::new(name)?;
        unsafe {
            let retcode = lvm_lv_remove_tag(self.handle, name.as_ptr());
            self.check_retcode(retcode)?;
            self.vg.write()?;
            Ok(())
        }
    }

    pub fn rename(&self, new_name: &str) -> LvmResult<()> {
        let new_name = CString::new(new_name)?;
        unsafe {
            let retcode = lvm_lv_rename(self.handle, new_name.as_ptr());
            self.check_retcode(retcode)?;
        }
        Ok(())
    }

    /// Resize logical volume to new_size bytes
    pub fn resize(&self, new_size: u64) -> LvmResult<()> {
        unsafe {
            let retcode = lvm_lv_resize(self.handle, new_size);
            self.check_retcode(retcode)?;
        }
        Ok(())
    }

    /// Create a snapshot of a logical volume
    /// Max snapshot space to use. If you pass zero the same amount of space
    /// as the origin will be used
    pub fn snapshot(
        &self,
        snap_name: &str,
        max_snap_size: u64,
    ) -> LvmResult<LogicalVolume<'_, '_>> {
        let snap_name = CString::new(snap_name)?;
        unsafe {
            let lv_t = lvm_lv_snapshot(self.handle, snap_name.as_ptr(), max_snap_size);
            if lv_t.is_null() {
                let err = self.lvm.get_error()?;
                return Err(LvmError::new((err.0, err.1)));
            }
            Ok({
                LogicalVolume {
                    handle: lv_t,
                    lvm: self.lvm,
                    vg: self.vg,
                }
            })
        }
    }
}

impl Lvm {
    fn check_retcode(&self, retcode: i32) -> LvmResult<()> {
        if retcode < 0 {
            let err = self.get_error()?;
            return Err(LvmError::new((err.0, err.1)));
        }
        Ok(())
    }

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
                    if handle.is_null() {
                        return Err(LvmError::new((
                            errno::errno(),
                            "Memory allocation problem".into(),
                        )));
                    }
                    Ok(Lvm { handle })
                }
            }
            None => {
                let p = ptr::null();
                unsafe {
                    let handle = lvm_init(p);
                    if handle.is_null() {
                        return Err(LvmError::new((
                            errno::errno(),
                            "Memory allocation problem".into(),
                        )));
                    }
                    Ok(Lvm { handle })
                }
            }
        }
    }

    pub fn get_volume_group_names(&self) -> LvmResult<Vec<String>> {
        let mut names: Vec<String> = vec![];
        unsafe {
            let vg_names = lvm_list_vg_names(self.handle);
            if vg_names.is_null() {
                let err = self.get_error()?;
                return Err(LvmError::new((err.0, err.1)));
            }
            let mut vg = dm_list_first(vg_names);
            loop {
                if vg.is_null() {
                    break;
                }
                let str_list = vg as *mut lvm_str_list;
                let name = CStr::from_ptr((*str_list).str)
                    .to_string_lossy()
                    .into_owned();
                names.push(name);
                vg = dm_list_next(vg_names, vg);
            }
        }

        Ok(names)
    }

    pub fn get_volume_group_uuids(&self) -> LvmResult<Vec<Uuid>> {
        let mut ids: Vec<Uuid> = vec![];
        unsafe {
            let vg_uuids = lvm_list_vg_uuids(self.handle);
            if vg_uuids.is_null() {
                let err = self.get_error()?;
                return Err(LvmError::new((err.0, err.1)));
            }
            let mut vg = dm_list_first(vg_uuids);
            loop {
                if vg.is_null() {
                    break;
                }
                let str_list = vg as *mut lvm_str_list;
                let name = CStr::from_ptr((*str_list).str).to_string_lossy();
                ids.push(Uuid::from_str(&name)?);
                vg = dm_list_next(vg_uuids, vg);
            }
        }

        Ok(ids)
    }

    pub fn pv_create(&self, name: &str, size: u64) -> LvmResult<()> {
        let name = CString::new(name)?;
        unsafe {
            let retcode = lvm_pv_create(self.handle, name.as_ptr(), size);
            self.check_retcode(retcode)?;
        }
        Ok(())
    }

    /// Remove a physical volume.
    /// Note: You cannot remove a PV while iterating through the list of PVs as
    /// locks are held for the PV list
    pub fn pv_remove(&self, name: &str) -> LvmResult<()> {
        let name = CString::new(name)?;
        unsafe {
            let retcode = lvm_pv_remove(self.handle, name.as_ptr());
            self.check_retcode(retcode)?;
        }
        Ok(())
    }

    pub fn pv_create_params(&self, pv_name: &str) -> LvmResult<PhysicalVolumeCreateParameters<'_>> {
        let name = CString::new(pv_name)?;
        unsafe {
            let pv_params = lvm_pv_params_create(self.handle, name.as_ptr());
            if pv_params.is_null() {
                let err = self.get_error()?;
                return Err(LvmError::new((err.0, err.1)));
            }
            Ok(PhysicalVolumeCreateParameters {
                handle: pv_params,
                property_value: None,
                lvm: &self,
            })
        }
    }

    /// Scan all devices on the system for VGs and LVM metadata
    pub fn scan(&self) -> LvmResult<()> {
        unsafe {
            let retcode = lvm_scan(self.handle);
            self.check_retcode(retcode)?;
        }
        Ok(())
    }

    ///Return the volume group name given a device name
    pub fn vg_name_from_device(&self, device: &str) -> LvmResult<Option<String>> {
        let device = CString::new(device)?;
        unsafe {
            let id = lvm_vgname_from_device(self.handle, device.as_ptr());
            if id.is_null() {
                return Ok(None);
            }
            let name = CStr::from_ptr(id).to_string_lossy().into_owned();
            Ok(Some(name))
        }
    }

    /// Return the volume group name given a PV UUID
    pub fn vg_name_from_pvid(&self, pvid: &Uuid) -> LvmResult<Option<String>> {
        let pvid = CString::new(pvid.as_bytes().to_vec())?;
        unsafe {
            let id = lvm_vgname_from_pvid(self.handle, pvid.as_ptr());
            if id.is_null() {
                return Ok(None);
            }
            let name = CStr::from_ptr(id).to_string_lossy().into_owned();
            Ok(Some(name))
        }
    }
    ///  This function checks that the name has no invalid characters,
    /// the length doesn't exceed maximum and that the VG name isn't already in use
    /// and that the name adheres to any other limitations.
    pub fn vg_name_validate(&self, name: &str) -> LvmResult<()> {
        let name = CString::new(name)?;
        unsafe {
            let retcode = lvm_vg_name_validate(self.handle, name.as_ptr());
            self.check_retcode(retcode)?;
        }
        Ok(())
    }

    /// Create a VG with default parameters.
    /// This function creates a Volume Group object in memory.
    /// Once all parameters are set appropriately and all devices are added to the
    /// VG, use lvm_vg_write() to commit the new VG to disk, and lvm_vg_close() to
    /// release the VG handle.
    pub fn vg_create(&self, name: &str) -> LvmResult<VolumeGroup<'_>> {
        let name = CString::new(name)?;
        unsafe {
            let vg_t = lvm_vg_create(self.handle, name.as_ptr());
            if vg_t.is_null() {
                let err = self.get_error()?;
                return Err(LvmError::new((err.0, err.1)));
            }
            Ok(VolumeGroup {
                handle: vg_t,
                lvm: &self,
            })
        }
    }

    pub fn vg_open(&self, name: &str, mode: &OpenMode) -> LvmResult<VolumeGroup<'_>> {
        let name = CString::new(name)?;
        let mode = CString::new(mode.to_string())?;
        unsafe {
            let vg_handle = lvm_vg_open(self.handle, name.as_ptr(), mode.as_ptr(), 0);
            if vg_handle.is_null() {
                let err = self.get_error()?;
                return Err(LvmError::new((err.0, err.1)));
            }
            Ok(VolumeGroup {
                handle: vg_handle,
                lvm: &self,
            })
        }
    }
}

impl<'a> PhysicalVolumeCreateParameters<'a> {
    /// Create a parameter object to use in function lvm_pv_create_adv
    pub fn get_property(&mut self, name: &Property) -> LvmResult<()> {
        let name = CString::new(name.to_string())?;
        unsafe {
            let property_value = lvm_pv_params_get_property(self.handle, name.as_ptr());
            self.property_value = Some(property_value);
        }
        Ok(())
    }

    pub fn set_property(&mut self, name: &Property) -> LvmResult<()> {
        let name = CString::new(name.to_string())?;
        unsafe {
            let retcode = lvm_pv_params_set_property(
                self.handle,
                name.as_ptr(),
                &mut self.property_value.unwrap(),
            );
            if retcode < 0 {
                let err = self.lvm.get_error()?;
                return Err(LvmError::new((err.0, err.1)));
            }
        }
        Ok(())
    }
}

impl<'a> PhysicalVolume<'a> {
    fn check_retcode(&self, retcode: i32) -> LvmResult<()> {
        if retcode < 0 {
            let err = self.lvm.get_error()?;
            return Err(LvmError::new((err.0, err.1)));
        }
        Ok(())
    }

    /// Get the current size in bytes of a device underlying a
    /// physical volume
    pub fn get_dev_size(&self) -> u64 {
        unsafe { lvm_pv_get_dev_size(self.handle) }
    }

    /// Get the current unallocated space in bytes of a physical volume
    pub fn get_free(&self) -> u64 {
        unsafe { lvm_pv_get_free(self.handle) }
    }

    /// Get the current number of metadata areas in the physical volume
    pub fn get_mda_count(&self) -> u64 {
        unsafe { lvm_pv_get_mda_count(self.handle) }
    }

    /// Get the current name of a physical volume
    pub fn get_name(&self) -> String {
        unsafe {
            let name = lvm_pv_get_name(self.handle);
            CStr::from_ptr(name).to_string_lossy().into_owned()
        }
    }

    /// Get the current size in bytes of a physical volume
    pub fn get_size(&self) -> u64 {
        unsafe { lvm_pv_get_size(self.handle) }
    }

    /*
    pub fn get_property(&self, name: &str) -> LvmResult<PhysicalVolumeCreateParameters> {
        let name = CString::new(name)?;
        unsafe {
            let val = lvm_pv_get_property(self.handle, name.as_ptr());
            if val.is_valid() != 0 {
                let err = self.lvm.get_error()?;
                return Err(LvmError::new((err.0, err.1)));
            }
            Ok(PhysicalVolumeCreateParameters {})
        }
    }
    */

    pub fn get_uuid(&self) -> String {
        unsafe {
            let id = lvm_pv_get_uuid(self.handle);
            let tmp = CStr::from_ptr(id).to_string_lossy();
            tmp.into_owned()
        }
    }

    pub fn resize(&self, new_size: u64) -> LvmResult<()> {
        unsafe {
            let retcode = lvm_pv_resize(self.handle, new_size);
            self.check_retcode(retcode)?;
        }
        Ok(())
    }
}

impl<'a> VolumeGroup<'a> {
    /// Add a tag to a VG
    pub fn add_tag(&self, tag: &str) -> LvmResult<()> {
        let tag = CString::new(tag)?;
        unsafe {
            let retcode = lvm_vg_add_tag(self.handle, tag.as_ptr());
            self.check_retcode(retcode)?;
        }
        self.write()?;
        Ok(())
    }

    fn check_retcode(&self, retcode: i32) -> LvmResult<()> {
        if retcode < 0 {
            let err = self.lvm.get_error()?;
            return Err(LvmError::new((err.0, err.1)));
        }
        Ok(())
    }

    /// Close a VG
    pub fn close(&self) -> LvmResult<()> {
        unsafe {
            let retcode = lvm_vg_close(self.handle);
            self.check_retcode(retcode)?;
        }
        Ok(())
    }

    /// Return a list of LV handles for a given VG handle
    pub fn list_lvs(&self) -> LvmResult<Vec<LogicalVolume<'_, '_>>> {
        let mut lvs: Vec<LogicalVolume<'_, '_>> = vec![];
        unsafe {
            let lv_head = lvm_vg_list_lvs(self.handle);
            let mut lv = dm_list_first(lv_head);
            loop {
                if lv.is_null() {
                    break;
                }
                let lv_list = lv as *mut lvm_lv_list;
                lvs.push(LogicalVolume {
                    handle: (*lv_list).lv,
                    lvm: self.lvm,
                    vg: self,
                });
                lv = dm_list_next(lv_head, lv);
            }
        }

        Ok(lvs)
    }

    /// Return a list of PV handles for all
    pub fn list_pvs(&self) -> LvmResult<Vec<PhysicalVolume<'_>>> {
        let mut pvs: Vec<PhysicalVolume<'_>> = vec![];
        unsafe {
            let pv_head = lvm_vg_list_pvs(self.handle);
            let mut pv = dm_list_first(pv_head);
            loop {
                if pv.is_null() {
                    break;
                }
                let pv_list = pv as *mut lvm_pv_list;
                pvs.push(PhysicalVolume {
                    handle: (*pv_list).pv,
                    lvm: self.lvm,
                });
                pv = dm_list_next(pv_head, pv);
            }
        }

        Ok(pvs)
    }

    /// Create a linear logical volume
    pub fn create_lv_linear(&self, name: &str, size: u64) -> LvmResult<LogicalVolume<'_, '_>> {
        let name = CString::new(name)?;
        unsafe {
            let lv_t = lvm_vg_create_lv_linear(self.handle, name.as_ptr(), size);
            if lv_t.is_null() {
                let err = self.lvm.get_error()?;
                return Err(LvmError::new((err.0, err.1)));
            }
            Ok(LogicalVolume {
                handle: lv_t,
                lvm: self.lvm,
                vg: self,
            })
        }
    }

    /// Create a thinpool parameter passing object for the specified VG
    /// \param   chunk_size
    /// data block size of the pool
    /// Default value is DEFAULT_THIN_POOL_CHUNK_SIZE * 2 when 0 passed as chunk_size
    /// DM_THIN_MIN_DATA_BLOCK_SIZE < chunk_size < DM_THIN_MAX_DATA_BLOCK_SIZE
    ///
    /// \param meta_size
    /// Size of thin pool's metadata logical volume. Allowed range is 2MB-16GB.
    /// Default value (ie if 0) pool size / pool chunk size * 64
    ///
    /// Note: Passdown discard policy is the default.
    pub fn create_thin_pool(
        &self,
        pool_name: &str,
        size: u64,
        chunk_size: u32,
        metadata_size: u64,
        discard_policy: &LvmThinPolicy,
    ) -> LvmResult<()> {
        let pool_name = CString::new(pool_name)?;
        let discard = match discard_policy {
            LvmThinPolicy::Ignore => lvm_thin_discards_t_LVM_THIN_DISCARDS_IGNORE,
            LvmThinPolicy::NoPassdown => lvm_thin_discards_t_LVM_THIN_DISCARDS_NO_PASSDOWN,
            LvmThinPolicy::Passdown => lvm_thin_discards_t_LVM_THIN_DISCARDS_PASSDOWN,
        };
        unsafe {
            let create_params = lvm_lv_params_create_thin_pool(
                self.handle,
                pool_name.as_ptr(),
                size,
                chunk_size,
                metadata_size,
                discard,
            );
            if create_params.is_null() {
                let err = self.lvm.get_error()?;
                return Err(LvmError::new((err.0, err.1)));
            }
        }
        Ok(())
    }

    /// Extend a VG by adding a device
    pub fn extend(&self, device: &Path) -> LvmResult<()> {
        let dev = CString::new(device.to_string_lossy().as_bytes())?;
        unsafe {
            let retcode = lvm_vg_extend(self.handle, dev.as_ptr());
            self.check_retcode(retcode)?;
        }
        self.write()?;
        Ok(())
    }

    /// Get the current metadata sequence number of a volume group.
    /// The metadata sequence number is incrented for each metadata change.
    /// Applications may use the sequence number to determine if any LVM objects
    /// have changed from a prior query.
    pub fn get_seq_number(&self) -> u64 {
        unsafe { lvm_vg_get_seqno(self.handle) }
    }

    /// Get the current name of a volume group
    pub fn get_name(&self) -> LvmResult<String> {
        unsafe {
            let uid = lvm_vg_get_name(self.handle);
            let tmp = CStr::from_ptr(uid).to_string_lossy();

            Ok(tmp.into_owned())
        }
    }

    /// Get the current number of total extents of a volume group
    pub fn get_extent_count(&self) -> u64 {
        unsafe { lvm_vg_get_extent_count(self.handle) }
    }

    /// Get the current extent size in bytes of a volume group
    pub fn get_extent_size(&self) -> u64 {
        unsafe { lvm_vg_get_extent_size(self.handle) }
    }

    /// Get the current number of free extents of a volume group
    pub fn get_free_extents(&self) -> u64 {
        unsafe { lvm_vg_get_free_extent_count(self.handle) }
    }

    /// Get the current unallocated space in bytes of a volume group
    pub fn get_free_size(&self) -> u64 {
        unsafe { lvm_vg_get_free_size(self.handle) }
    }

    /// Get the maximum number of logical volumes allowed in a volume group
    pub fn get_max_lv(&self) -> u64 {
        unsafe { lvm_vg_get_max_lv(self.handle) }
    }

    /// Get the maximum number of physical volumes allowed in a volume group
    pub fn get_max_pv(&self) -> u64 {
        unsafe { lvm_vg_get_max_pv(self.handle) }
    }

    /// Get the current number of physical volumes of a volume group
    pub fn get_pv_count(&self) -> u64 {
        unsafe { lvm_vg_get_pv_count(self.handle) }
    }

    /// Get the current size in bytes of a volume group
    pub fn get_size(&self) -> u64 {
        unsafe { lvm_vg_get_size(self.handle) }
    }

    pub fn get_tags(&self) -> LvmResult<Vec<String>> {
        let mut names: Vec<String> = vec![];
        unsafe {
            let tag_head = lvm_vg_get_tags(self.handle);
            let mut tag = dm_list_first(tag_head);
            loop {
                if tag.is_null() {
                    break;
                }
                let str_list = tag as *mut lvm_str_list;
                let name = CStr::from_ptr((*str_list).str)
                    .to_string_lossy()
                    .into_owned();
                names.push(name);
                tag = dm_list_next(tag_head, tag);
            }
        }

        Ok(names)
    }

    /// Get the current uuid of a volume group
    pub fn get_uuid(&self) -> String {
        unsafe {
            let uid = lvm_vg_get_uuid(self.handle);
            let tmp = CStr::from_ptr(uid).to_string_lossy();

            tmp.into_owned()
        }
    }

    /// Get whether or not a volume group is clustered
    pub fn is_clustered(&self) -> bool {
        unsafe {
            let clustered = lvm_vg_is_clustered(self.handle);
            clustered == 1
        }
    }

    /// Get whether or not a volume group is exported
    pub fn is_exported(&self) -> bool {
        unsafe {
            let exported = lvm_vg_is_exported(self.handle);
            exported == 1
        }
    }
    /// Get whether or not a volume group is a partial volume group.
    /// When one or more physical volumes belonging to the volume group
    /// are missing from the system the volume group is a partial volume
    ///  group.
    pub fn is_partial(&self) -> bool {
        unsafe {
            let partial = lvm_vg_is_partial(self.handle);
            partial == 1
        }
    }

    pub fn lv_from_name(&self, name: &str) -> LvmResult<LogicalVolume<'_, '_>> {
        let name = CString::new(name)?;
        unsafe {
            let lv_t = lvm_lv_from_name(self.handle, name.as_ptr());
            if lv_t.is_null() {
                let err = self.lvm.get_error()?;
                return Err(LvmError::new((err.0, err.1)));
            }
            Ok(LogicalVolume {
                handle: lv_t,
                lvm: self.lvm,
                vg: self,
            })
        }
    }

    /// Validate a name to be used for LV creation
    /// Validates that the name does not contain any invalid characters,
    /// max length and that the LV name doesn't already exist for this VG
    pub fn name_validate(&self, name: &str) -> LvmResult<()> {
        let name = CString::new(name)?;
        unsafe {
            let retcode = lvm_lv_name_validate(self.handle, name.as_ptr());
            self.check_retcode(retcode)?;
        }
        Ok(())
    }

    /// Lookup an PV handle in a VG by the PV name.
    pub fn pv_from_name(&self, name: &str) -> LvmResult<PhysicalVolume<'_>> {
        let name = CString::new(name)?;
        unsafe {
            let pv_t = lvm_pv_from_name(self.handle, name.as_ptr());
            if pv_t.is_null() {
                let err = self.lvm.get_error()?;
                return Err(LvmError::new((err.0, err.1)));
            }
            Ok(PhysicalVolume {
                handle: pv_t,
                lvm: self.lvm,
            })
        }
    }

    /// Lookup an PV handle in a VG by the PV uuid
    pub fn pv_from_uuid(&self, id: &Uuid) -> LvmResult<PhysicalVolume<'_>> {
        let id = CString::new(id.as_bytes().to_vec())?;
        unsafe {
            let pv_t = lvm_pv_from_uuid(self.handle, id.as_ptr());
            if pv_t.is_null() {
                let err = self.lvm.get_error()?;
                return Err(LvmError::new((err.0, err.1)));
            }
            Ok(PhysicalVolume {
                handle: pv_t,
                lvm: self.lvm,
            })
        }
    }

    /// Reduce a VG by removing an unused device.
    pub fn reduce(&self, device: &str) -> LvmResult<()> {
        let dev = CString::new(device)?;
        unsafe {
            let retcode = lvm_vg_reduce(self.handle, dev.as_ptr());
            self.check_retcode(retcode)?;
        }
        Ok(())
    }

    /// Remove a VG from the system.
    pub fn remove(&self) -> LvmResult<()> {
        unsafe {
            let retcode = lvm_vg_remove(self.handle);
            self.check_retcode(retcode)?;
        }
        self.write()?;
        Ok(())
    }

    /// Remove a tag to a VG
    pub fn remove_tag(&self, tag: &str) -> LvmResult<()> {
        let tag = CString::new(tag)?;
        unsafe {
            let retcode = lvm_vg_remove_tag(self.handle, tag.as_ptr());
            self.check_retcode(retcode)?;
        }
        self.write()?;
        Ok(())
    }

    pub fn set_extent_size(&self, size: u32) -> LvmResult<()> {
        unsafe {
            let retcode = lvm_vg_set_extent_size(self.handle, size);
            self.check_retcode(retcode)?;
        }
        self.write()?;
        Ok(())
    }

    /// Write a VG to disk
    pub fn write(&self) -> LvmResult<()> {
        unsafe {
            let retcode = lvm_vg_write(self.handle);
            self.check_retcode(retcode)?;
        }
        Ok(())
    }
}
