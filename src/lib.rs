/*!
Rust API for the [Lab Streaming Layer](https://github.com/sccn/labstreaminglayer) (LSL).

The lab streaming layer is a peer-to-peer pub/sub system on the local network that allows for
real-time exchange of multi-channel time series (plus their meta-data) between applications and
machines, with built-in cross-device time synchronization.

The most common use case is in lab spaces to make, e.g., instrument data from different pieces of
hardware (e.g., sensors) accessible in real time to client programs (e.g., experimentation scripts,
recording programs, stream viewers, or live processing software). One of the main features of LSL
is the uniform API that allows clients to read formatted multi-channel data from many device types
(such as EEG, eye tracking, audio, human interface devices, events, etc.) with the same few lines
of code.

The API covers two areas:
- The "push API" (aka publish) allows to create stream outlets and to push data (regular or
  irregular measurement time series, event data, coded audio/video frames, etc.) into them.
- The "pull API" (aka subscribe) allows to create stream inlets and read time-synched experiment
  data from them (for recording, viewing or experiment control).

This crate provides safe bindings to the [liblsl](https://github.com/sccn/liblsl) library via the
low-level/raw `lsl-sys` crate.

# Data layout

Samples are passed as slices with one value per channel. Chunks (multiple samples at once) are
passed as flat, *multiplexed* slices, i.e. `[s0c0, s0c1, ..., s0cN, s1c0, s1c1, ...]`, whose
length must be a multiple of the channel count. This is the native layout of liblsl, so chunks
are handed over without any copying or per-sample overhead.

# Threading

`StreamInfo` is `Send + Sync`. `StreamOutlet`, `StreamInlet` and `ContinuousResolver` are `Send`
(they can be moved to and used from another thread) but not `Sync`: liblsl does not allow
concurrent pushes into the same outlet, or concurrent pulls from the same inlet (unless
`ProcessingOption::Threadsafe` is set). Wrap them in a `Mutex` if they must be shared.

# Errors

Operations that allocate OS resources (e.g., memory, sockets, etc) such as the `new()` functions of
the main objects (`StreamInfo`, `StreamOutlet`, `StreamInlet`, `ContinuousResolver`) may return
`Error::ResourceCreation` variants. Invalid arguments (e.g., a sample whose length does not match
the channel count, or strings with embedded zero bytes) yield `Error::BadArgument`. A more
detailed message for the most recent error, if any, is available via `last_error()`.
*/

use lsl_sys::*;
use std::ffi::{self, c_char, c_ulong};
use std::fmt;
use std::marker::PhantomData;

/// Constant to indicate that a stream has variable sampling rate.
pub const IRREGULAR_RATE: f64 = 0.0;

/**
Constant to indicate that a sample has the next successive time stamp.

This is an optional optimization to transmit less data per sample.
The stamp is then deduced from the preceding one according to the stream's sampling rate
(in the case of an irregular rate, the same time stamp as before will is assumed).
*/
pub const DEDUCED_TIMESTAMP: f64 = -1.0;

/**
A very large time duration (> 1 year) for timeout values.

Note that significantly larger numbers can cause the timeout to be invalid on some
operating systems (e.g., 32-bit UNIX).
*/
pub const FOREVER: f64 = 32000000.0;

/// Error type for all errors that can be returned by this library.
#[derive(PartialEq, Eq, Copy, Clone, Debug)]
pub enum Error {
    /// A bad argument was passed into a library function (e.g., negative number, string containing
    /// embedded zero bytes (which C libraries tend to not accept), or a sample whose length does
    /// not match the stream's channel count).
    BadArgument,
    /// A user-provided timeout has expired.
    Timeout,
    /// The stream that this is reading from has disappeared from the network and is unrecoverable.
    /// This can only happen if the stream had an empty `source_id` or if you turned off recovery.
    StreamLost,
    /// Resource creation failed. This is usually due to OS resource exhaustion (e.g., out of
    /// memory, thread handles, sockets, or the like).
    ResourceCreation,
    /// An internal error happened in the library. This is generally unlikely but can be returned
    /// by a variety of library calls.
    Internal,
    /// An unknown error has happened. There are only very few calls where this can happen since no
    /// detailed error codes are available in those cases, and is very unlikely to occur.
    Unknown,
}

/// Result type alias for results with library-specific errors.
pub type Result<T> = std::result::Result<T, Error>;

/// Data format of a channel (each transmitted sample holds an array of channels).
#[derive(PartialEq, Eq, Copy, Clone, Debug)]
pub enum ChannelFormat {
    /// For up to 24-bit precision measurements in the appropriate physical unit
    /// (e.g., microvolts). Integers from -16777216 to 16777216 are represented accurately.
    Float32 = 1,
    /// For universal numeric data as long as permitted by network & disk budget.
    /// The largest representable integer is 53-bit.
    Double64 = 2,
    /// For variable-length strings or data blobs, such as video frames, complex event
    /// descriptions, etc.
    String = 3,
    /// For high-rate digitized formats that require 32-bit precision. Depends critically on
    /// meta-data to represent meaningful units. Useful for application event codes or other
    /// coded data.
    Int32 = 4,
    /// For very high rate signals (40Khz+) such as consumer-grade audio
    /// (for professional audio, float is recommended).
    Int16 = 5,
    /// For binary signals or other coded data. Not recommended for encoding string data.
    Int8 = 6,
    /// 64-bit integers. Note that support for this type is not yet exposed in all languages.
    Int64 = 7,
    /// Can not be transmitted. This is treated as an error/unknown value when used in conjunction
    /// with any of the API methods.
    Undefined = 0,
}

/// Post-processing options for stream inlets.
#[derive(PartialEq, Eq, Copy, Clone, Debug)]
pub enum ProcessingOption {
    /// No automatic post-processing; return the ground-truth time stamps for manual post-
    /// processing (this is the default behavior of the inlet).
    None = 0,
    /// Perform automatic clock synchronization; equivalent to manually adding the value returned
    /// by the `time_correction()` method to the received time stamps.
    ClockSync = 1,
    /// Remove jitter from time stamps. This will apply a smoothing algorithm to the received time
    /// stamps; the smoothing needs to see a minimum number of samples (30-120 seconds worst-case)
    /// until the remaining jitter is consistently below 1ms.
    Dejitter = 2,
    /// Force the time-stamps to be monotonically ascending (only makes sense if timestamps are
    /// dejittered).
    Monotonize = 4,
    /// Post-processing is thread-safe (same inlet can be read from by multiple threads);
    /// uses somewhat more CPU.
    Threadsafe = 8,
    /// The combination of all possible post-processing options.
    ALL = 1 | 2 | 4 | 8,
}

/// Transport options for `StreamOutlet::new_ex()` and `StreamInlet::new_ex()`.
#[derive(PartialEq, Eq, Copy, Clone, Debug)]
pub enum TransportOption {
    /// The supplied max_buf value is in samples (instead of seconds).
    BufsizeSamples = 1,
    /// The supplied max_buf value is in 1/1000ths of a second (instead of seconds).
    BufsizeThousandths = 2,
}

/**
Protocol version number.
- The major version is protocol_version() / 100;
- The minor version is protocol_version() % 100;

Clients with different minor versions are protocol-compatible with each other while clients with
different major versions will refuse to work together (as of this writing, all versions are
compatible with each other).
*/
pub fn protocol_version() -> i32 {
    unsafe { lsl_protocol_version() }
}

/**
Version number of the liblsl library.
- The major version is library_version() / 100;
- The minor version is library_version() % 100;
*/
pub fn library_version() -> i32 {
    unsafe { lsl_library_version() }
}

/**
Get a string containing library/build information.

The format is considered an implementation detail and may change. This is mostly intended for
debugging potential ABI or version issues.
*/
pub fn library_info() -> String {
    unsafe { make_string(lsl_library_info()) }
}

/**
Obtain a local system time stamp in seconds.

This clock should be used for all time measurements that are intended to be used in relation to
LSL time stamps, since LSL cannot time-synchronize other kinds of clocks for you. *However*, if you
build an application in which you have your own synchronized clocks (e.g., atomic clocks), it can
make sense to use those other clocks.

The resolution of this clock is better than a millisecond. The most common use case is to use this
reading to assign time stamps to samples as they are being acquired. Specifically, if the *age* of
a sample is known at a particular time (e.g., from USB transmission delays), it can be used as an
offset to `local_clock()` to obtain a better (back-dated) estimate of when a sample was actually
captured. See `StreamOutlet::push_sample()` for a use case.
*/
pub fn local_clock() -> f64 {
    unsafe { lsl_local_clock() }
}

/**
A human-readable description of the most recent error raised by liblsl on the calling thread
(empty if there was none). Useful for logging after receiving an `Err` from this crate.
*/
pub fn last_error() -> String {
    unsafe { make_string(lsl_last_error()) }
}

/**
Set the path of the configuration file (`lsl_api.cfg`) to be used by liblsl.

This must be called before any other LSL function (including creating any `StreamInfo`), since
the configuration is loaded lazily on first use; afterwards it has no effect.
*/
pub fn set_config_filename(filename: &str) -> Result<()> {
    let filename = ffi::CString::new(filename)?;
    unsafe { lsl_set_config_filename(filename.as_ptr()) };
    Ok(())
}

/**
Set the liblsl configuration directly from an INI-formatted string (same format as
`lsl_api.cfg`). When set, this content takes precedence over any configuration file.

This must be called before any other LSL function (including creating any `StreamInfo`), since
the configuration is loaded lazily on first use; afterwards it has no effect.
*/
pub fn set_config_content(content: &str) -> Result<()> {
    let content = ffi::CString::new(content)?;
    unsafe { lsl_set_config_content(content.as_ptr()) };
    Ok(())
}

// ==========================
// === Stream Declaration ===
// ==========================

/**
The `StreamInfo` object stores the declaration of a data stream.

It represents the following information:
* stream data format (number of channels, channel format)
* core information (stream name, content type, sampling rate)
* optional meta-data about the stream content (channel labels, measurement units, etc.)

Whenever a program wants to provide a new stream on the lab network it will typically first
create a `StreamInfo` to describe its properties and then construct a `StreamOutlet` with it to
create the stream on the network.

The stream can then be discovered based on any of its meta-data, and recipients who discover the
stream on the network can then query the full stream information.

The content of the `StreamInfo` encompasses all the static information that is known up-front about
a data stream, and therefore, anything you would expect to find in a file header for a streaming
data file should be written into the stream info (in fact, if you use a tool to record one or more
streams into an `XDF` file, the stream info goes into the file header.

Cloning a `StreamInfo` makes a deep copy.
*/
#[derive(Debug)]
pub struct StreamInfo {
    handle: lsl_streaminfo,
}

// SAFETY: the native object is not tied to a thread. All operations through `&self` only read the
// object (the query cache used by `matches_query` is internally synchronized); mutation of the
// description requires `&mut self` (see `desc_mut`).
unsafe impl Send for StreamInfo {}
unsafe impl Sync for StreamInfo {}

impl StreamInfo {
    /** Construct a new `StreamInfo` object.
    Core stream information is specified here. Any remaining meta-data can be added subsequently.

    Arguments:
    * `stream_name`: Name of the stream. Describes the device (or product series) that this stream
      makes available (for use by programs, experimenters or data analysts). Cannot be empty.
    * `stream_type`: Content type of the stream.
      Please see [here](https://github.com/sccn/xdf/wiki/Meta-Data) (or web search for: XDF
      meta-data) for pre-defined content-type names that LSL adheres to, but you can also make
      up your own. The content type is the preferred way to find streams (as opposed to searching
      by name).
    * `channel_count`: Number of channels per sample. This stays constant for the lifetime of the
      stream.
    * `nominal_srate`: The sampling rate (in Hz) as advertised by the data source, if regular
      (otherwise set to `lsl::IRREGULAR_RATE`).
    * `channel_format`: Format/type of each channel. If your channels have different formats,
      consider supplying multiple streams or use the largest type that can hold them all (such as
      `ChannelFormat::Double64`).
    * `source_id`: Unique identifier of the device or source of the data, if available (such as
      the serial number). This is critical for system robustness since it allows recipients to
      recover from failure even after the serving app, device or computer crashes (just by finding
      a stream with the same source id on the network again). Therefore, it is highly recommended
      to always try to provide whatever information can uniquely identify the data source itself.
      If you don't have a unique id, you may use an empty str here.
    */
    pub fn new(
        stream_name: &str,
        stream_type: &str,
        channel_count: u32,
        nominal_srate: f64,
        channel_format: ChannelFormat,
        source_id: &str,
    ) -> Result<StreamInfo> {
        if stream_name.is_empty()
            || nominal_srate.is_nan()
            || nominal_srate < 0.0
            || channel_count >= 0x80000000
        {
            return Err(Error::BadArgument);
        }
        let stream_name = ffi::CString::new(stream_name)?;
        let stream_type = ffi::CString::new(stream_type)?;
        let source_id = ffi::CString::new(source_id)?;
        let handle = unsafe {
            lsl_create_streaminfo(
                stream_name.as_ptr(),
                stream_type.as_ptr(),
                channel_count as i32,
                nominal_srate,
                channel_format.to_native(),
                source_id.as_ptr(),
            )
        };
        StreamInfo::from_raw(handle).ok_or(Error::ResourceCreation)
    }

    // ========================
    // === Core Information ===
    // ========================
    // (these fields are assigned at construction)

    /**
    Name of the stream.
    This is a human-readable name. For streams offered by device modules, it refers to the type of
    device or product series that is generating the data of the stream. If the source is an
    application, the name may be a more generic or specific identifier. Multiple streams with the
    same name can coexist, though potentially at the cost of ambiguity (for the recording app or
    experimenter).
    */
    pub fn stream_name(&self) -> String {
        unsafe { make_string(lsl_get_name(self.handle)) }
    }

    /**
    Content type of the stream.
    The content type is a short string such as "EEG", "Gaze" which describes the content carried
    by the channel (if known). If a stream contains mixed content this value need not be assigned
    but may instead be stored in the description of channel types. To be useful to applications and
    automated processing systems, using the recommended content types is preferred. Content types
    usually follow those pre-defined [here](https://github.com/sccn/xdf/wiki/Meta-Data) (or web
    search for: XDF meta-data).
    */
    pub fn stream_type(&self) -> String {
        unsafe { make_string(lsl_get_type(self.handle)) }
    }

    /**
    Number of channels of the stream.
    A stream has at least one channel; the channel count stays constant for all samples.
    */
    pub fn channel_count(&self) -> i32 {
        unsafe { lsl_get_channel_count(self.handle) }
    }

    /**
    Sampling rate of the stream, according to the source (in Hz).
    If a stream is irregularly sampled, this should be set to `lsl::IRREGULAR_RATE`.

    Note that no data will be lost even if this sampling rate is incorrect or if a device has
    temporary hiccups, since all samples will be recorded anyway (except for those dropped by the
    device itself). However, when the recording is imported into an application, a good importer
    may correct such errors more accurately if the advertised sampling rate was close to the specs
    of the device.
    */
    pub fn nominal_srate(&self) -> f64 {
        unsafe { lsl_get_nominal_srate(self.handle) }
    }

    /**
    Channel format of the stream.
    All channels in a stream have the same format. However, a device might offer multiple
    time-synched streams each with its own format.
    */
    pub fn channel_format(&self) -> ChannelFormat {
        unsafe { ChannelFormat::from_native(lsl_get_channel_format(self.handle)) }
    }

    /** Unique identifier of the stream's source, if available.
    The unique source (or device) identifier is an optional piece of information that, if
    available, allows that endpoints (such as the recording program) can re-acquire a stream
    automatically once it is back online.
    */
    pub fn source_id(&self) -> String {
        unsafe { make_string(lsl_get_source_id(self.handle)) }
    }

    // ======================================
    // === Additional Hosting Information ===
    // ======================================
    // (these fields are implicitly assigned once bound to an outlet/inlet)

    /**
    Protocol version used to deliver the stream. Formatted like `lsl::protocol_version()`.
    */
    pub fn version(&self) -> i32 {
        unsafe { lsl_get_version(self.handle) }
    }

    /**
    Creation time stamp of the stream.
    This is the time stamp when the stream was first created
    (as determined via `lsl::local_clock()` on the providing machine).
    */
    pub fn created_at(&self) -> f64 {
        unsafe { lsl_get_created_at(self.handle) }
    }

    /**
    Unique ID of the stream outlet instance (once assigned).
    This is a unique identifier of the stream outlet, and is guaranteed to be different
    across multiple instantiations of the same outlet (e.g., after a re-start).
    */
    pub fn uid(&self) -> String {
        unsafe { make_string(lsl_get_uid(self.handle)) }
    }

    /**
    Session ID for the given stream.
    The session id is an optional human-assigned identifier of the recording session.
    While it is rarely used, it can be used to prevent concurrent recording activitites
    on the same sub-network (e.g., in multiple experiment areas) from seeing each other's streams
    (assigned via a configuration file by the experimenter, see Network Connectivity in the LSL
    wiki).
    */
    pub fn session_id(&self) -> String {
        unsafe { make_string(lsl_get_session_id(self.handle)) }
    }

    /**
    Hostname of the providing machine.
    */
    pub fn hostname(&self) -> String {
        unsafe { make_string(lsl_get_hostname(self.handle)) }
    }

    // ========================
    // === Data Description ===
    // ========================

    /**
    Read-only access to the extended description of the stream.

    See `desc_mut()` for details on the content of the description.
    */
    pub fn desc(&self) -> XMLElement<'_> {
        XMLElement::new(unsafe { lsl_get_desc(self.handle) })
    }

    /**
    Mutable access to the extended description of the stream.

    It is highly recommended that at least the channel labels are described here.
    See code examples on the LSL wiki. Other information, such as amplifier settings,
    measurement units if deviating from defaults, setup information, subject information, etc.,
    can be specified here, as well. Meta-data recommendations follow the XDF file format project
    [here](https://github.com/sccn/xdf/wiki/Meta-Data) (or web search for: XDF meta-data).

    **Important:** if you use a stream content type for which meta-data recommendations exist,
    please try to lay out your meta-data in agreement with these recommendations for compatibility
    with other applications.
    */
    pub fn desc_mut(&mut self) -> XMLElementMut<'_> {
        XMLElementMut::new(unsafe { lsl_get_desc(self.handle) })
    }

    /**
    Test whether the stream information matches the given query string.
    The query is evaluated using the same rules that govern `lsl::resolve_bypred()`.
    */
    pub fn matches_query(&self, query: &str) -> bool {
        match ffi::CString::new(query) {
            Ok(query) => unsafe { lsl_stream_info_matches_query(self.handle, query.as_ptr()) != 0 },
            Err(_) => false,
        }
    }

    // ===============================
    // === Miscellaneous Functions ===
    // ===============================

    /**
    Retrieve the entire streaminfo in XML format.
    This yields an XML document (in string form) whose top-level element is `<info>`. The info
    element contains one element for each field of the streaminfo class, including:

    * the core elements `<name>`, `<type>`, `<channel_count>`, `<nominal_srate>`,
      `<channel_format>`, `<source_id>`
    * the misc elements `<version>`, `<created_at>`, `<uid>`, `<session_id>`,
      `<v4address>`, `<v4data_port>`, `<v4service_port>`, `<v6address>`, `<v6data_port>`,
      `<v6service_port>`
    * the extended description element `<desc>` with user-defined sub-elements.
    */
    pub fn to_xml(&self) -> Result<String> {
        unsafe {
            let tmpstr = lsl_get_xml(self.handle);
            if tmpstr.is_null() {
                return Err(Error::Internal);
            }
            let result = ffi::CStr::from_ptr(tmpstr).to_string_lossy().into_owned();
            lsl_destroy_string(tmpstr);
            Ok(result)
        }
    }

    /// Number of bytes occupied by a channel (0 for string-typed channels).
    pub fn channel_bytes(&self) -> i32 {
        unsafe { lsl_get_channel_bytes(self.handle) }
    }

    /// Number of bytes occupied by a sample (0 for string-typed channels).
    pub fn sample_bytes(&self) -> i32 {
        unsafe { lsl_get_sample_bytes(self.handle) }
    }

    /// Construct a blank `StreamInfo`.
    pub fn from_blank() -> Result<StreamInfo> {
        StreamInfo::new("untitled", "", 0, 0.0, ChannelFormat::Undefined, "")
    }

    /**
    Create a `StreamInfo` from an XML string, as produced by `to_xml()` on the info of an outlet
    or inlet. Fails with `Error::BadArgument` if the XML is malformed or lacks required fields
    (such as the `<uid>` that is only assigned once an outlet is created).
    */
    pub fn from_xml(xml: &str) -> Result<StreamInfo> {
        let xml = ffi::CString::new(xml)?;
        let handle = unsafe { lsl_streaminfo_from_xml(xml.as_ptr()) };
        let info = StreamInfo::from_raw(handle).ok_or(Error::ResourceCreation)?;
        // liblsl reports parse/validation errors by setting the name to "(invalid: <reason>)"
        if info.stream_name().starts_with("(invalid: ") {
            return Err(Error::BadArgument);
        }
        Ok(info)
    }

    // === internal methods ===

    // Take ownership of a native handle (None if it is NULL).
    fn from_raw(handle: lsl_streaminfo) -> Option<StreamInfo> {
        if handle.is_null() {
            None
        } else {
            Some(StreamInfo { handle })
        }
    }
}

impl Clone for StreamInfo {
    fn clone(&self) -> StreamInfo {
        let handle = unsafe { lsl_copy_streaminfo(self.handle) };
        // this only fails if out of memory, which Rust treats as fatal anyway
        StreamInfo::from_raw(handle).expect("failed to clone native lsl_streaminfo object")
    }
}

impl Drop for StreamInfo {
    fn drop(&mut self) {
        unsafe { lsl_destroy_streaminfo(self.handle) }
    }
}

impl fmt::Display for StreamInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "(name={}, type={}, fmt={}, srate={})",
            self.stream_name(),
            self.stream_type(),
            self.channel_format(),
            self.nominal_srate()
        )
    }
}

// =======================
// ==== Stream Outlet ====
// =======================

/**
A stream outlet.
Outlets are used to make streaming data (and the meta-data) available on the lab network.

The actual sample-pushing functionality is provided via the `Pushable` and `ExPushable` traits,
which are implemented for `f32`, `f64`, `i64`, `i32`, `i16`, `i8`, `u8` (numeric formats) and
`String`, `&str`, `Vec<u8>`, `&[u8]` (string/blob format).

**Examples:** the `send_*.rs` examples (found in the crate's github repository) illustrate the use
of `StreamOutlet`.
*/
#[derive(Debug)]
pub struct StreamOutlet {
    handle: lsl_outlet,
    channel_count: usize,
}

// SAFETY: the native outlet is not tied to the thread that created it. It is *not* `Sync`: liblsl's
// send queue is single-producer, so concurrent pushes into the same outlet are not allowed.
unsafe impl Send for StreamOutlet {}

impl StreamOutlet {
    /**
    Establish a new stream outlet. This makes the stream discoverable.

    Arguments:
    * `info`: The stream information to use for creating this stream. Stays constant over the
      lifetime of the outlet.
    * `chunk_size`: The desired chunk granularity (in samples) for transmission.
      If specified as 0, each push operation yields one chunk. Inlets can override this setting.
    * `max_buffered`: The maximum amount of data to buffer (in seconds if there is a
      nominal sampling rate, otherwise x100 in samples). A good default is 360, which corresponds
      to 6 minutes of data. Note that, for high-bandwidth data you should consider using a lower
      value here to avoid running out of RAM in case data have to be buffered unexpectedly.
    */
    pub fn new(info: &StreamInfo, chunk_size: i32, max_buffered: i32) -> Result<StreamOutlet> {
        StreamOutlet::new_ex(info, chunk_size, max_buffered, &[])
    }

    /**
    Establish a new stream outlet with transport options.

    Same as `new()`, except that `options` change how `max_buffered` is interpreted
    (see `TransportOption`).
    */
    pub fn new_ex(
        info: &StreamInfo,
        chunk_size: i32,
        max_buffered: i32,
        options: &[TransportOption],
    ) -> Result<StreamOutlet> {
        let channel_count =
            usize::try_from(info.channel_count()).map_err(|_| Error::BadArgument)?;
        if chunk_size < 0 || max_buffered < 0 || channel_count == 0 {
            return Err(Error::BadArgument);
        }
        let handle = unsafe {
            lsl_create_outlet_ex(
                info.handle,
                chunk_size,
                max_buffered,
                transport_flags(options),
            )
        };
        if handle.is_null() {
            return Err(Error::ResourceCreation);
        }
        Ok(StreamOutlet {
            handle,
            channel_count,
        })
    }

    /// Number of channels per sample.
    pub fn channel_count(&self) -> usize {
        self.channel_count
    }

    // ===============================
    // === Miscellaneous Functions ===
    // ===============================

    /**
    Check whether consumers are currently registered.

    You can use this to disable sending data if there's no consumer (e.g. to save battery on an
    embedded device) -- however, this is not necessary and most production clients do not use it.
    */
    pub fn have_consumers(&self) -> bool {
        unsafe { lsl_have_consumers(self.handle) != 0 }
    }

    /**
    Wait until some consumer shows up (without wasting resources, e.g., on embedded devices).

    To have no timeout, you can use the value `lsl::FOREVER` here. Returns True if the wait was
    successful, false if the timeout expired.

    Note that it is not necessary to do this, and most production clients do not use this feature.
    */
    pub fn wait_for_consumers(&self, timeout: f64) -> bool {
        unsafe { lsl_wait_for_consumers(self.handle, timeout) != 0 }
    }

    /**
    Retrieve the stream info provided by this outlet.

    This is what was used to create the stream (and also has the Additional Network Information
    fields assigned).

    In extremely rare cases this may fail with an `Err::ResourceCreation` variant (e.g., due to
    out of memory).
    */
    pub fn info(&self) -> Result<StreamInfo> {
        StreamInfo::from_raw(unsafe { lsl_get_info(self.handle) }).ok_or(Error::ResourceCreation)
    }

    // --- internal methods ---

    fn check_sample(&self, len: usize) -> Result<()> {
        if len == self.channel_count {
            Ok(())
        } else {
            Err(Error::BadArgument)
        }
    }

    // Validate a multiplexed chunk and return its length as the native element count type.
    fn check_chunk(&self, len: usize) -> Result<c_ulong> {
        if !len.is_multiple_of(self.channel_count) {
            return Err(Error::BadArgument);
        }
        c_ulong::try_from(len).map_err(|_| Error::BadArgument)
    }

    fn check_stamped_chunk(&self, len: usize, stamps: usize) -> Result<c_ulong> {
        if stamps.checked_mul(self.channel_count) != Some(len) {
            return Err(Error::BadArgument);
        }
        self.check_chunk(len)
    }
}

impl Drop for StreamOutlet {
    fn drop(&mut self) {
        unsafe {
            lsl_destroy_outlet(self.handle);
        }
    }
}

/**
A trait that enables the methods `push_sample()`, `push_chunk()` and `push_chunk_stamped()`.
Implemented by `StreamOutlet` for all supported value types `T`.

See also the `ExPushable` trait for the extended-argument versions of these methods. If you push
data of the wrong size (sample length not matching the declared number of channels, or chunk
length not a multiple of it), these functions return `Error::BadArgument`.
*/
pub trait Pushable<T> {
    /**
    Push a sample (one value per channel) into the outlet.

    The data are time-stamped with the current time (using `local_clock()`), and immediately
    transmitted (unless a `chunk_size` was provided at outlet construction, which overrides in what
    granularity data are transmitted). See also `push_sample_ex()` (provided by `ExPushable` trait)
    for a variant that allows for overriding the timestamp and implicit push-through (flush)
    behavior.
    */
    fn push_sample(&self, data: &[T]) -> Result<()>;

    /**
    Push a chunk of samples, multiplexed into one flat slice (all channels of the first sample,
    then all channels of the second sample, and so on), into the outlet.

    The most recent sample is time-stamped with the current time (using `local_clock()`), and the
    others are back-dated according to the sampling rate. The data are immediately transmitted
    (unless a `chunk_size` was provided at outlet construction).
    */
    fn push_chunk(&self, data: &[T]) -> Result<()>;

    /**
    Push a multiplexed chunk of samples along with a separate time stamp for each sample (e.g.,
    for irregular-rate streams) into the outlet.

    Arguments:
    * `data`: A flat slice of samples (`timestamps.len() * channel_count` values).
    * `timestamps`: The capture time of each sample, in agreement with `local_clock()`.
    */
    fn push_chunk_stamped(&self, data: &[T], timestamps: &[f64]) -> Result<()>;
}

impl<T, U: ExPushable<T>> Pushable<T> for U {
    fn push_sample(&self, data: &[T]) -> Result<()> {
        self.push_sample_ex(data, 0.0, true)
    }

    fn push_chunk(&self, data: &[T]) -> Result<()> {
        self.push_chunk_ex(data, 0.0, true)
    }

    fn push_chunk_stamped(&self, data: &[T], timestamps: &[f64]) -> Result<()> {
        self.push_chunk_stamped_ex(data, timestamps, true)
    }
}

/**
A trait that enables the methods `push_sample_ex()`, `push_chunk_ex()` and
`push_chunk_stamped_ex()`. Implemented by `StreamOutlet` for all supported value types `T`.

See also the `Pushable` trait for the simpler variants of these methods.
*/
pub trait ExPushable<T> {
    /**
    Push a sample (one value per channel) into the outlet.

    Arguments:
    * `data`: A slice of values to push (one for each channel).
    * `timestamp`: Optionally the capture time of the sample, in agreement with `local_clock()`;
      if passed as 0.0, the current time is used.
    * `pushthrough`: Whether to push the sample through to the receivers instead of buffering it
      with subsequent samples. Typically this would be `true`. Note that the `chunk_size`, if
      specified at outlet construction, takes precedence over the pushthrough flag.
    */
    fn push_sample_ex(&self, data: &[T], timestamp: f64, pushthrough: bool) -> Result<()>;

    /**
    Push a multiplexed chunk of samples into the outlet.

    Arguments:
    * `data`: A flat slice of samples; its length must be a multiple of the channel count.
    * `timestamp`: Optionally the capture time of the most recent sample, in agreement with
      `local_clock()`; if specified as 0.0, the current time is used. The time stamps of other
      samples are automatically derived according to the sampling rate of the stream.
    * `pushthrough`: Whether to push the chunk through to the receivers instead of buffering it
      with subsequent samples. Typically this would be `true`. Note that the `chunk_size`, if
      specified at outlet construction, takes precedence over the pushthrough flag.
    */
    fn push_chunk_ex(&self, data: &[T], timestamp: f64, pushthrough: bool) -> Result<()>;

    /**
    Push a multiplexed chunk of samples into the outlet, with a separate time stamp for each sample.

    Arguments:
    * `data`: A flat slice of samples (`timestamps.len() * channel_count` values).
    * `timestamps`: The capture time of each sample, in agreement with `local_clock()`.
    * `pushthrough`: Whether to push the chunk through to the receivers instead of buffering it
      with subsequent samples. Typically this would be `true`. Note that the `chunk_size`, if
      specified at outlet construction, takes precedence over the pushthrough flag.
    */
    fn push_chunk_stamped_ex(
        &self,
        data: &[T],
        timestamps: &[f64],
        pushthrough: bool,
    ) -> Result<()>;
}

// Implements ExPushable for a numeric type whose memory layout matches the native element type.
macro_rules! impl_push_numeric {
    ($t:ty, $native:ty, $sample:ident, $chunk:ident, $chunk_stamped:ident) => {
        impl ExPushable<$t> for StreamOutlet {
            fn push_sample_ex(&self, data: &[$t], timestamp: f64, pushthrough: bool) -> Result<()> {
                self.check_sample(data.len())?;
                let ec = unsafe {
                    $sample(
                        self.handle,
                        data.as_ptr() as *const $native,
                        timestamp,
                        pushthrough as i32,
                    )
                };
                errcode_to_result(ec).map(|_| ())
            }

            fn push_chunk_ex(&self, data: &[$t], timestamp: f64, pushthrough: bool) -> Result<()> {
                let n = self.check_chunk(data.len())?;
                if n == 0 {
                    return Ok(());
                }
                let ec = unsafe {
                    $chunk(
                        self.handle,
                        data.as_ptr() as *const $native,
                        n,
                        timestamp,
                        pushthrough as i32,
                    )
                };
                errcode_to_result(ec).map(|_| ())
            }

            fn push_chunk_stamped_ex(
                &self,
                data: &[$t],
                timestamps: &[f64],
                pushthrough: bool,
            ) -> Result<()> {
                let n = self.check_stamped_chunk(data.len(), timestamps.len())?;
                if n == 0 {
                    return Ok(());
                }
                let ec = unsafe {
                    $chunk_stamped(
                        self.handle,
                        data.as_ptr() as *const $native,
                        n,
                        timestamps.as_ptr(),
                        pushthrough as i32,
                    )
                };
                errcode_to_result(ec).map(|_| ())
            }
        }
    };
}

impl_push_numeric!(
    f32,
    f32,
    lsl_push_sample_ftp,
    lsl_push_chunk_ftp,
    lsl_push_chunk_ftnp
);
impl_push_numeric!(
    f64,
    f64,
    lsl_push_sample_dtp,
    lsl_push_chunk_dtp,
    lsl_push_chunk_dtnp
);
impl_push_numeric!(
    i64,
    i64,
    lsl_push_sample_ltp,
    lsl_push_chunk_ltp,
    lsl_push_chunk_ltnp
);
impl_push_numeric!(
    i32,
    i32,
    lsl_push_sample_itp,
    lsl_push_chunk_itp,
    lsl_push_chunk_itnp
);
impl_push_numeric!(
    i16,
    i16,
    lsl_push_sample_stp,
    lsl_push_chunk_stp,
    lsl_push_chunk_stnp
);
// int8 channels: accept both signednesses (`c_char` is one of them, depending on the platform)
impl_push_numeric!(
    i8,
    c_char,
    lsl_push_sample_ctp,
    lsl_push_chunk_ctp,
    lsl_push_chunk_ctnp
);
impl_push_numeric!(
    u8,
    c_char,
    lsl_push_sample_ctp,
    lsl_push_chunk_ctp,
    lsl_push_chunk_ctnp
);

// Gather pointers and lengths of a slice of byte blobs, as expected by the *_buf* functions.
fn blob_parts<T: AsRef<[u8]>>(data: &[T]) -> Result<(Vec<*const c_char>, Vec<u32>)> {
    let ptrs = data
        .iter()
        .map(|x| x.as_ref().as_ptr() as *const c_char)
        .collect();
    let lens = data
        .iter()
        .map(|x| u32::try_from(x.as_ref().len()).map_err(|_| Error::BadArgument))
        .collect::<Result<_>>()?;
    Ok((ptrs, lens))
}

// Implements ExPushable for a type that can be viewed as a byte blob (string channel format).
macro_rules! impl_push_blob {
    (impl<$lt:lifetime> $t:ty) => {
        impl_push_blob!(@impl [$lt] $t);
    };
    ($t:ty) => {
        impl_push_blob!(@impl [] $t);
    };
    (@impl [$($lt:lifetime)?] $t:ty) => {
        impl<$($lt)?> ExPushable<$t> for StreamOutlet {
            fn push_sample_ex(&self, data: &[$t], timestamp: f64, pushthrough: bool) -> Result<()> {
                self.check_sample(data.len())?;
                let (mut ptrs, lens) = blob_parts(data)?;
                let ec = unsafe {
                    lsl_push_sample_buftp(
                        self.handle,
                        ptrs.as_mut_ptr(),
                        lens.as_ptr(),
                        timestamp,
                        pushthrough as i32,
                    )
                };
                errcode_to_result(ec).map(|_| ())
            }

            fn push_chunk_ex(&self, data: &[$t], timestamp: f64, pushthrough: bool) -> Result<()> {
                let n = self.check_chunk(data.len())?;
                if n == 0 {
                    return Ok(());
                }
                let (mut ptrs, lens) = blob_parts(data)?;
                let ec = unsafe {
                    lsl_push_chunk_buftp(
                        self.handle,
                        ptrs.as_mut_ptr(),
                        lens.as_ptr(),
                        n,
                        timestamp,
                        pushthrough as i32,
                    )
                };
                errcode_to_result(ec).map(|_| ())
            }

            fn push_chunk_stamped_ex(
                &self,
                data: &[$t],
                timestamps: &[f64],
                pushthrough: bool,
            ) -> Result<()> {
                let n = self.check_stamped_chunk(data.len(), timestamps.len())?;
                if n == 0 {
                    return Ok(());
                }
                let (mut ptrs, lens) = blob_parts(data)?;
                let ec = unsafe {
                    lsl_push_chunk_buftnp(
                        self.handle,
                        ptrs.as_mut_ptr(),
                        lens.as_ptr(),
                        n,
                        timestamps.as_ptr(),
                        pushthrough as i32,
                    )
                };
                errcode_to_result(ec).map(|_| ())
            }
        }
    };
}

impl_push_blob!(String);
impl_push_blob!(impl<'a> &'a str);
impl_push_blob!(Vec<u8>);
impl_push_blob!(impl<'a> &'a [u8]);

// ===========================
// ==== Resolve Functions ====
// ===========================

// Collect the results of a native resolve call into StreamInfo objects.
fn collect_resolved(
    resolve: impl FnOnce(*mut lsl_streaminfo, u32) -> i32,
) -> Result<Vec<StreamInfo>> {
    // the fixed-size buffer is safe since the native function uses it as the max number of results
    let mut buffer = [std::ptr::null_mut(); 1024];
    let n = errcode_to_result(resolve(buffer.as_mut_ptr(), buffer.len() as u32))? as usize;
    Ok(buffer[..n.min(buffer.len())]
        .iter()
        .filter_map(|&h| StreamInfo::from_raw(h))
        .collect())
}

/**
Resolve all streams on the network.

This function returns all currently available streams from any outlet on the network.
The network is usually the subnet specified at the local router, but may also include
a multicast group of machines (given that the network supports it), or list of hostnames.
These details may optionally be customized by the experimenter in a configuration file
(see Network Connectivity in the LSL wiki).
This is the default mechanism used by the browsing programs and the recording program.

Arguments:
* `wait_time`: The waiting time for the operation, in seconds, to search for streams. A good value
  is around 1.0 or 2.0 seconds. *Warning*: If this is too short (<0.5s) only a subset (or none) of
  the outlets that are present on the network may be returned.

Returns a `Vec` of `StreamInfo` objects (excluding their desc field), any of which can subsequently
be used to open an inlet. The full info can be retrieved from the inlet if needed.
*/
pub fn resolve_streams(wait_time: f64) -> Result<Vec<StreamInfo>> {
    collect_resolved(|buf, len| unsafe { lsl_resolve_all(buf, len, wait_time) })
}

/**
Resolve all streams with a specific value for a given property.

If the goal is to resolve a specific stream, this method is preferred over resolving all streams
and then selecting the desired one.

Arguments:
* `prop`: The `StreamInfo` property that should have a specific value (e.g., "name", "type",
  "source_id", or "desc/manufaturer").
* `value`: The string value that the property should have (e.g., "EEG" as the type property).
* `minimum`: Return at least this number of streams.
* `timeout`: A timeout for the operation, in seconds. If the timeout expires, less than the desired
  number of streams (possibly none) will be returned. To have no timeout you can use the value
  `lsl::FOREVER` here, otherwise use at least 1.0 to 2.0 seconds to allow for results to come in
  on a busy network. *Warning*: If this is too short (<0.5s) only a subset (or none) of the
  outlets that are present on the network may be returned.

Returns a `Vec` of `StreamInfo` objects (excluding their desc field), any of which can subsequently
be used to open an inlet. The full info can be retrieved from the inlet if needed. In case of a
timeout, the result is *not* an `Error::Timeout` but instead an shorter or empty result vector.
*/
pub fn resolve_byprop(
    prop: &str,
    value: &str,
    minimum: i32,
    timeout: f64,
) -> Result<Vec<StreamInfo>> {
    let prop = ffi::CString::new(prop)?;
    let value = ffi::CString::new(value)?;
    collect_resolved(|buf, len| unsafe {
        lsl_resolve_byprop(buf, len, prop.as_ptr(), value.as_ptr(), minimum, timeout)
    })
}

/**
Resolve all streams that match a given predicate.

Advanced query that allows to impose more conditions on the retrieved streams; the given
string is an [XPath 1.0](http://en.wikipedia.org/w/index.php?title=XPath_1.0) predicate evaluated
against the `<info>` element of the `StreamInfo`'s equivalent XML body (omitting the
surrounding []'s), for each stream that's on the network.

Arguments:
* `pred`: The predicate string, e.g. `name='BioSemi'` or
  `type='EEG' and starts-with(name,'BioSemi') and count(info/desc/channel)=32`
* `minimum`: Return at least this many streams.
* `timeout`: A timeout for the operation, in seconds. If the timeout expires, less than the desired
  number of streams (possibly none) will be returned. To have no timeout you can use the value
  `lsl::FOREVER` here, otherwise use at least 1.0 to 2.0 seconds to allow for results to come in
  on a busy network. *Warning*: If this is too short (<0.5s) only a subset (or none) of the
  outlets that are present on the network may be returned.

Returns a `Vec` of `StreamInfo` objects (excluding their desc field), any of which can subsequently
be used to open an inlet. The full info can be retrieved from the inlet if needed. In case of a
timeout, the result is *not* an `Error:Timeout` but instead an shorter or empty result vector.
*/
pub fn resolve_bypred(pred: &str, minimum: i32, timeout: f64) -> Result<Vec<StreamInfo>> {
    let pred = ffi::CString::new(pred)?;
    collect_resolved(|buf, len| unsafe {
        lsl_resolve_bypred(buf, len, pred.as_ptr(), minimum, timeout)
    })
}

// ======================
// ==== Stream Inlet ====
// ======================

/**
A stream inlet.
Inlets are used to receive streaming data (and meta-data) from the lab network.

The actual sample-pulling functionality is provided via the `Pullable` trait, which is implemented
for `f32`, `f64`, `i64`, `i32`, `i16`, `i8`, `u8` (numeric formats) and `String`, `Vec<u8>`
(string/blob format).

**Examples:** the `receive_*.rs` examples (found in the crate's github repository) illustrate the
use of `StreamInlet`.

### Errors

For operations where a timeout is provided, if the operation does not complete in time,
an `Error::Timeout` will be returned -- except for the `pull_*()` functions, where this is not
considered an error. Also, for most operations, an `Error::StreamLost` is returned if the stream
source has been lost in the meantime (see also `recover` option in the inlet's `new()` constructor).
*/
#[derive(Debug)]
pub struct StreamInlet {
    handle: lsl_inlet,
    channel_count: usize,
}

// SAFETY: the native inlet is not tied to the thread that created it. It is not `Sync` since
// concurrent reads from one inlet are only supported with `ProcessingOption::Threadsafe`.
unsafe impl Send for StreamInlet {}

impl StreamInlet {
    /**
    Construct a new stream inlet from a resolved stream info.

    Arguments:
    * `info`: A resolved stream info object (as coming from one of the resolver functions).
      Note: the `StreamInlet` may also be constructed with a manually-constructed `StreamInfo`, if
      the desired channel format and count is already known up-front, but this is strongly
      discouraged and should only ever be done if there is no time to resolve the stream up-front
      (e.g., due to limitations in the client program).
    * `max_buflen`: The maximum amount of data to buffer (in seconds if there is a nominal sampling
      rate, otherwise x100 in samples). Recording applications want to use a fairly large buffer
      size here (a good default would be 360, which corresponds to 6 minutes of data), while
      real-time applications would only buffer as much as they need to perform their next
      calculation (e.g., 1-10).
    * `max_chunklen`: The maximum size, in samples, at which chunks are transmitted (the default
      corresponds to the chunk sizes used by the sender). If specified as 0, the chunk sizes
      preferred by the sender are used. Recording applications can use a generous size here
      (leaving it to the network how to pack things), while real-time applications may want a
      finer (perhaps 1-sample) granularity.
    * `recover`: Try to silently recover lost streams that are recoverable (those that that
      have a `source_id` set). In all other cases (`recover` is `false` or the stream is not
      recoverable) inlet methods may return `Error::StreamLost` if the stream's source is lost
      (e.g., due to an app or computer crash).
    */
    pub fn new(
        info: &StreamInfo,
        max_buflen: i32,
        max_chunklen: i32,
        recover: bool,
    ) -> Result<StreamInlet> {
        StreamInlet::new_ex(info, max_buflen, max_chunklen, recover, &[])
    }

    /**
    Construct a new stream inlet with transport options.

    Same as `new()`, except that `options` change how `max_buflen` is interpreted
    (see `TransportOption`).
    */
    pub fn new_ex(
        info: &StreamInfo,
        max_buflen: i32,
        max_chunklen: i32,
        recover: bool,
        options: &[TransportOption],
    ) -> Result<StreamInlet> {
        let channel_count =
            usize::try_from(info.channel_count()).map_err(|_| Error::BadArgument)?;
        if max_buflen < 0 || max_chunklen < 0 || channel_count == 0 {
            return Err(Error::BadArgument);
        }
        let handle = unsafe {
            lsl_create_inlet_ex(
                info.handle,
                max_buflen,
                max_chunklen,
                recover as i32,
                transport_flags(options),
            )
        };
        if handle.is_null() {
            return Err(Error::ResourceCreation);
        }
        Ok(StreamInlet {
            handle,
            channel_count,
        })
    }

    /// Number of channels per sample.
    pub fn channel_count(&self) -> usize {
        self.channel_count
    }

    /**
    Retrieve the complete information of the given stream, including the extended description.
    Can be invoked at any time of the stream's lifetime.

    Arguments:
    * `timeout`: Timeout of the operation. You can use the value `lsl::FOREVER` to have no timeout.
    */
    pub fn info(&self, timeout: f64) -> Result<StreamInfo> {
        let mut ec = 0;
        let handle = unsafe { lsl_get_fullinfo(self.handle, timeout, &mut ec) };
        errcode_to_result(ec)?;
        StreamInfo::from_raw(handle).ok_or(Error::Unknown)
    }

    /**
    Subscribe to the data stream.

    All samples pushed in at the other end from this moment onwards will be queued and eventually
    be delivered in response to `pull_sample()` or `pull_chunk()` calls.

    In most applications it is not necessary to call this function since the stream will be opened
    implicitly upon the first call to any of the `pull_*()` operations. However, it can be used in
    order to not lose samples that had been sent over the stream prior to the first `pull_*()` call.

    Arguments:
    * `timeout` Optional timeout of the operation. To have no timeout, you can use `lsl::FOREVER`
      here. A timeout can make sense if you want to catch connection errors (e.g., due to
      misconfigured firewalls or the like).
    */
    pub fn open_stream(&self, timeout: f64) -> Result<()> {
        let mut ec = 0;
        unsafe { lsl_open_stream(self.handle, timeout, &mut ec) };
        errcode_to_result(ec).map(|_| ())
    }

    /**
    Unsubscribe from the current data stream.

    All samples that are still buffered or in flight will be dropped and transmission and buffering
    of data for this inlet will be stopped. If an application stops being interested in data from a
    source (temporarily or not) but keeps the outlet alive, it should call `close_stream()` to not
    waste unnecessary system and network resources. This feature is rarely used in practice since
    it's often simpler to just discard the whole inlet and later recreate it.
    */
    pub fn close_stream(&self) {
        unsafe {
            lsl_close_stream(self.handle);
        }
    }

    /**
    Retrieve an estimated time correction offset for the given stream.

    The first call to this function takes several milliseconds until a reliable first estimate is
    obtained. Subsequent calls are instantaneous (and rely on periodic background updates). On a
    well-behaved network, the precision of these estimates should be below 1 ms (empirically it is
    within +/-0.2 ms).

    To get a measure of whether the network is well-behaved, see also the extended version
    `time_correction_ex()`, which additionally returns the round-trip-time, which is an upper bound
    for the uncertainty.

    Arguments:
    * `timeout`: Timeout to acquire the first time-correction estimate. You can use the value
      `lsl::FOREVER` to have no timeout. Otherwise, 2.0-5.0 seconds would be a reasonable timeout.
      Note that even if the timeout fails, the library will continue to attempt retrieving a
      time-correction estimate in the background, which can be queried in a subsequent call.
    */
    pub fn time_correction(&self, timeout: f64) -> Result<f64> {
        let mut ec = 0;
        let result = unsafe { lsl_time_correction(self.handle, timeout, &mut ec) };
        errcode_to_result(ec)?;
        Ok(result)
    }

    /**
    Retrieve extended time-correction information for the given stream.

    This function is used like `time_correction()`, but instead returns additional information in
    a tuple of 3 values, which are (`time_offset`, `remote_time`, `uncertainty`), where:

    * `time_offset` corresponds to the return value of `time_correction()` (see for explanation).
    * `remote_time` is the remote time when the measurement was made, and
      consequently `remote_time + time_offset` is the local time when that measurement was made
      (this will typically lie as much as a few seconds before the current time point -- not
      because of inaccuracy, but because measurements are made periodically in the background,
      and the function only returns the most recent one of them).
    * `uncertainty` is the round-trip-time (RTT) of the measurement in seconds, which is a hard
      upper bound on the uncertainty of the time offset. Empirically, 0.2 ms a typical RTT for
      wired networks, 2 ms is typical of wireless networks, but it can be much higher on poor
      networks.
    */
    pub fn time_correction_ex(&self, timeout: f64) -> Result<(f64, f64, f64)> {
        let mut ec = 0;
        let (mut remote_time, mut uncertainty) = (0.0, 0.0);
        let result = unsafe {
            lsl_time_correction_ex(
                self.handle,
                &mut remote_time,
                &mut uncertainty,
                timeout,
                &mut ec,
            )
        };
        errcode_to_result(ec)?;
        Ok((result, remote_time, uncertainty))
    }

    /**
    Set post-processing flags to use.

    By default, the inlet performs NO post-processing and returns the ground-truth time
    stamps, which can then be manually synchronized using `time_correction()`, and then
    smoothed/dejittered if desired. This function allows automating these two and possibly
    more operations.

    *Warning*: when you enable this, you will no longer receive or be able to recover the
    original time stamps.

    Arguments:
    * `options`: an array of `ProcessingOption` values that shall be set. You can also pass in
      the value `[ProcessingOption::ALL]` to enable all options or an empty array to clear all
      previously set options.
    */
    pub fn set_postprocessing(&self, options: &[ProcessingOption]) -> Result<()> {
        let flags = options.iter().fold(0u32, |acc, &opt| acc | opt as u32);
        errcode_to_result(unsafe { lsl_set_postprocessing(self.handle, flags) }).map(|_| ())
    }

    /**
    Query whether samples are currently available for immediate pickup.

    Note that it is not a good idea to use `samples_available()` to determine whether
    a `pull_*()` call would block: to be sure, set the pull timeout to 0.0 or an acceptably
    low value. If the underlying implementation supports it, the value will be the number of
    samples available (otherwise it will be 1 or 0).
    */
    pub fn samples_available(&self) -> u32 {
        unsafe { lsl_samples_available(self.handle) }
    }

    /**
    Drop all queued not-yet pulled samples.

    Returns the number of dropped samples.
    */
    pub fn flush(&self) -> u32 {
        unsafe { lsl_inlet_flush(self.handle) }
    }

    /**
    Query whether the clock was potentially reset since the last call to `was_clock_reset()`.

    This is a rarely-used function that is only useful to applications that combine multiple
    `time_correction` values to estimate precise clock drift; it allows to tolerate cases where
    the machine from which the stream is coming was hot-swapped or restarted in between two
    measurements.
    */
    pub fn was_clock_reset(&self) -> bool {
        unsafe { lsl_was_clock_reset(self.handle) != 0 }
    }

    /**
    Override the half-time (forget factor) of the time-stamp smoothing.

    The default is 90 seconds unless a different value is set in the config file. Using a longer
    window will yield lower jitter in the time stamps, but longer windows will have trouble
    tracking changes in the clock rate (usually due to temperature changes); the default is able
    to track changes up to 10 degrees C per minute sufficiently well.
    */
    pub fn smoothing_halftime(&self, value: f32) -> Result<()> {
        errcode_to_result(unsafe { lsl_smoothing_halftime(self.handle, value) }).map(|_| ())
    }

    // --- internal methods ---

    // Number of samples a multiplexed chunk buffer can hold, validating the buffer sizes.
    fn chunk_capacity(&self, data: usize, timestamps: usize) -> Result<c_ulong> {
        if timestamps.checked_mul(self.channel_count) != Some(data) {
            return Err(Error::BadArgument);
        }
        c_ulong::try_from(data).map_err(|_| Error::BadArgument)
    }

    // Pull all currently available samples by repeatedly calling a chunk-pulling function.
    fn pull_all<T: Clone + Default>(
        &self,
        pull_into: impl Fn(&mut [T], &mut [f64]) -> Result<usize>,
    ) -> Result<(Vec<T>, Vec<f64>)> {
        let cc = self.channel_count;
        let mut data = Vec::new();
        let mut stamps = Vec::new();
        loop {
            let want = (self.samples_available() as usize).clamp(1, 4096);
            let have = stamps.len();
            data.resize((have + want) * cc, T::default());
            stamps.resize(have + want, 0.0);
            let got = pull_into(&mut data[have * cc..], &mut stamps[have..])?;
            data.truncate((have + got) * cc);
            stamps.truncate(have + got);
            if got < want {
                return Ok((data, stamps));
            }
        }
    }
}

impl Drop for StreamInlet {
    fn drop(&mut self) {
        unsafe {
            lsl_destroy_inlet(self.handle);
        }
    }
}

/**
A trait that enables the methods `pull_sample()`, `pull_sample_buf()`, `pull_chunk_into()` and
`pull_chunk()`. Implemented by `StreamInlet` for all supported value types `T`.

When using these functions keep in mind that, if you do not pick up values for a while or at a
sufficiently fast rate, you will fall behind in the data stream (up to a maximum of the inlet's
`max_buflen` setting, after which the oldest samples are dropped).

Time stamps are the capture times on the remote side (e.g., remote machine). If you want to remap
them to the local machine's clock, you can enable the clock synchronization option on the inlet
using the `set_postprocessing()` method, or add the return value of the inlet's
`time_correction()` method manually.
*/
pub trait Pullable<T> {
    /**
    Pull the next successive sample from the inlet.

    Arguments:
    * `timeout`: The timeout for this operation, if any. If you use 0.0, the function will be
      non-blocking. You can also use `lsl::FOREVER` to have no timeout.

    Returns a tuple of `(sample, timestamp)`, where `sample` is a `Vec<T>` with one value per
    channel. If no new sample was available, the sample vector will be empty and the timestamp
    will be 0.0, i.e., it will *not* return an `Error::Timeout` since we consider this a normal
    behavior.
    */
    fn pull_sample(&self, timeout: f64) -> Result<(Vec<T>, f64)>;

    /**
    Pull the next successive sample from the inlet into a provided buffer.

    Arguments:
    * `buf`: A buffer into which this function will read the data; it will be resized (if
      necessary) to match the number of channels of the stream.
    * `timeout`: The timeout for this operation, if any. If you use 0.0, the function will be
      non-blocking. You can also use `lsl::FOREVER` to have no timeout.

    Returns the timestamp of the sample. If no new sample was available, the returned timestamp
    will be 0.0 and the buffer contents are unspecified.
    */
    fn pull_sample_buf(&self, buf: &mut Vec<T>, timeout: f64) -> Result<f64>;

    /**
    Pull a chunk of samples into caller-provided, multiplexed buffers. This is the most efficient
    way of reading data.

    Arguments:
    * `data`: Buffer for the samples (all channels of the first sample, then all channels of the
      second sample, and so on); must hold exactly `timestamps.len() * channel_count` values.
    * `timestamps`: Buffer for one time stamp per sample.
    * `timeout`: Deadline (in seconds) for filling the buffers: returns once they are full or the
      timeout has expired, whichever comes first. With 0.0, only samples that are available for
      immediate pickup are returned.

    Returns the number of samples that were written into the beginning of the buffers (which may
    be anywhere between 0 and `timestamps.len()`).
    */
    fn pull_chunk_into(
        &self,
        data: &mut [T],
        timestamps: &mut [f64],
        timeout: f64,
    ) -> Result<usize>;

    /**
    Pull all samples that are currently available, without blocking.

    Returns a tuple of `(data, timestamps)`, where `data` holds the samples in multiplexed order
    (`timestamps.len() * channel_count` values). Both are empty if no data was available.
    */
    fn pull_chunk(&self) -> Result<(Vec<T>, Vec<f64>)>;
}

// Implements Pullable for a numeric type whose memory layout matches the native element type.
macro_rules! impl_pull_numeric {
    ($t:ty, $native:ty, $sample:ident, $chunk:ident) => {
        impl Pullable<$t> for StreamInlet {
            fn pull_sample(&self, timeout: f64) -> Result<(Vec<$t>, f64)> {
                let mut sample = Vec::new();
                let ts = self.pull_sample_buf(&mut sample, timeout)?;
                if ts == 0.0 {
                    sample.clear();
                }
                Ok((sample, ts))
            }

            fn pull_sample_buf(&self, buf: &mut Vec<$t>, timeout: f64) -> Result<f64> {
                buf.resize(self.channel_count, Default::default());
                let mut ec = 0;
                let ts = unsafe {
                    $sample(
                        self.handle,
                        buf.as_mut_ptr() as *mut $native,
                        self.channel_count as i32,
                        timeout,
                        &mut ec,
                    )
                };
                errcode_to_result(ec)?;
                Ok(ts)
            }

            fn pull_chunk_into(
                &self,
                data: &mut [$t],
                timestamps: &mut [f64],
                timeout: f64,
            ) -> Result<usize> {
                let n = self.chunk_capacity(data.len(), timestamps.len())?;
                if n == 0 {
                    return Ok(0);
                }
                let mut ec = 0;
                let written = unsafe {
                    $chunk(
                        self.handle,
                        data.as_mut_ptr() as *mut $native,
                        timestamps.as_mut_ptr(),
                        n,
                        timestamps.len() as c_ulong,
                        timeout,
                        &mut ec,
                    )
                };
                errcode_to_result(ec)?;
                Ok(written as usize / self.channel_count)
            }

            fn pull_chunk(&self) -> Result<(Vec<$t>, Vec<f64>)> {
                self.pull_all(|data: &mut [$t], ts| self.pull_chunk_into(data, ts, 0.0))
            }
        }
    };
}

impl_pull_numeric!(f32, f32, lsl_pull_sample_f, lsl_pull_chunk_f);
impl_pull_numeric!(f64, f64, lsl_pull_sample_d, lsl_pull_chunk_d);
impl_pull_numeric!(i64, i64, lsl_pull_sample_l, lsl_pull_chunk_l);
impl_pull_numeric!(i32, i32, lsl_pull_sample_i, lsl_pull_chunk_i);
impl_pull_numeric!(i16, i16, lsl_pull_sample_s, lsl_pull_chunk_s);
impl_pull_numeric!(i8, c_char, lsl_pull_sample_c, lsl_pull_chunk_c);
impl_pull_numeric!(u8, c_char, lsl_pull_sample_c, lsl_pull_chunk_c);

// Take ownership of native strings: convert the first `count` and free all `ptrs` (liblsl
// allocates every slot of a chunk buffer, even those beyond the returned count).
unsafe fn take_blobs<T>(
    ptrs: &[*mut c_char],
    lens: &[u32],
    count: usize,
    mapper: fn(&[u8]) -> T,
    mut sink: impl FnMut(usize, T),
) {
    for (k, (&p, &len)) in ptrs.iter().zip(lens).enumerate() {
        if p.is_null() {
            continue;
        }
        if k < count {
            sink(
                k,
                mapper(std::slice::from_raw_parts(p as *const u8, len as usize)),
            );
        }
        lsl_destroy_string(p);
    }
}

// Implements Pullable for an owned blob type (string channel format).
macro_rules! impl_pull_blob {
    ($t:ty, $mapper:expr) => {
        impl Pullable<$t> for StreamInlet {
            fn pull_sample(&self, timeout: f64) -> Result<(Vec<$t>, f64)> {
                let mut sample = Vec::new();
                let ts = self.pull_sample_buf(&mut sample, timeout)?;
                if ts == 0.0 {
                    sample.clear();
                }
                Ok((sample, ts))
            }

            fn pull_sample_buf(&self, buf: &mut Vec<$t>, timeout: f64) -> Result<f64> {
                let cc = self.channel_count;
                let mut ptrs = vec![std::ptr::null_mut(); cc];
                let mut lens = vec![0u32; cc];
                let mut ec = 0;
                let ts = unsafe {
                    lsl_pull_sample_buf(
                        self.handle,
                        ptrs.as_mut_ptr(),
                        lens.as_mut_ptr(),
                        cc as i32,
                        timeout,
                        &mut ec,
                    )
                };
                buf.resize(cc, Default::default());
                let count = if ec == 0 && ts != 0.0 { cc } else { 0 };
                unsafe { take_blobs(&ptrs, &lens, count, $mapper, |k, v| buf[k] = v) };
                errcode_to_result(ec)?;
                Ok(ts)
            }

            fn pull_chunk_into(
                &self,
                data: &mut [$t],
                timestamps: &mut [f64],
                timeout: f64,
            ) -> Result<usize> {
                let n = self.chunk_capacity(data.len(), timestamps.len())?;
                if n == 0 {
                    return Ok(0);
                }
                let mut ptrs = vec![std::ptr::null_mut(); data.len()];
                let mut lens = vec![0u32; data.len()];
                let mut ec = 0;
                let written = unsafe {
                    lsl_pull_chunk_buf(
                        self.handle,
                        ptrs.as_mut_ptr(),
                        lens.as_mut_ptr(),
                        timestamps.as_mut_ptr(),
                        n,
                        timestamps.len() as c_ulong,
                        timeout,
                        &mut ec,
                    )
                } as usize;
                let count = if ec == 0 { written.min(data.len()) } else { 0 };
                unsafe { take_blobs(&ptrs, &lens, count, $mapper, |k, v| data[k] = v) };
                errcode_to_result(ec)?;
                Ok(count / self.channel_count)
            }

            fn pull_chunk(&self) -> Result<(Vec<$t>, Vec<f64>)> {
                self.pull_all(|data: &mut [$t], ts| self.pull_chunk_into(data, ts, 0.0))
            }
        }
    };
}

impl_pull_blob!(String, |x| String::from_utf8_lossy(x).into_owned());
impl_pull_blob!(Vec<u8>, |x| x.to_vec());

// =====================
// ==== XML Element ====
// =====================

/**
A read-only cursor into the XML description of a `StreamInfo` (see `StreamInfo::desc()`).

This provides operations for navigating to parent, children and sibling elements. Each element
has a name and can have multiple named children or have text content as value; attributes are
omitted. The API is modeled after a subset of pugixml's node type; see
[here](https://pugixml.org/docs/manual.html#access) for additional documentation.

Navigating to a non-existent node yields an empty element, on which all operations are no-ops
returning empty values; use `is_valid()` to check for that.
*/
#[derive(Clone, Copy, Debug)]
pub struct XMLElement<'a> {
    cursor: lsl_xml_ptr,
    _info: PhantomData<&'a StreamInfo>,
}

// SAFETY: an `XMLElement` is a shared borrow of a `StreamInfo` (which is `Sync`) that only reads.
unsafe impl Send for XMLElement<'_> {}
unsafe impl Sync for XMLElement<'_> {}

impl<'a> XMLElement<'a> {
    fn new(cursor: lsl_xml_ptr) -> XMLElement<'a> {
        XMLElement {
            cursor,
            _info: PhantomData,
        }
    }

    fn nav(&self, f: unsafe extern "C" fn(lsl_xml_ptr) -> lsl_xml_ptr) -> XMLElement<'a> {
        XMLElement::new(unsafe { f(self.cursor) })
    }

    fn nav_named(
        &self,
        f: unsafe extern "C" fn(lsl_xml_ptr, *const c_char) -> lsl_xml_ptr,
        name: &str,
    ) -> XMLElement<'a> {
        // a name with embedded zero bytes cannot match any element
        let cursor = match ffi::CString::new(name) {
            Ok(name) => unsafe { f(self.cursor, name.as_ptr()) },
            Err(_) => std::ptr::null_mut(),
        };
        XMLElement::new(cursor)
    }

    // === Tree Navigation ===

    /// Get the first child of the element.
    pub fn first_child(&self) -> XMLElement<'a> {
        self.nav(lsl_first_child)
    }

    /// Get the last child of the element.
    pub fn last_child(&self) -> XMLElement<'a> {
        self.nav(lsl_last_child)
    }

    /// Get the next sibling in the children list of the parent node.
    pub fn next_sibling(&self) -> XMLElement<'a> {
        self.nav(lsl_next_sibling)
    }

    /// Get the previous sibling in the children list of the parent node.
    pub fn previous_sibling(&self) -> XMLElement<'a> {
        self.nav(lsl_previous_sibling)
    }

    /// Get the parent node.
    pub fn parent(&self) -> XMLElement<'a> {
        self.nav(lsl_parent)
    }

    /// Iterate over the children of this element.
    pub fn children(&self) -> impl Iterator<Item = XMLElement<'a>> {
        let first = self.first_child();
        std::iter::successors(Some(first).filter(|e| e.is_valid()), |e| {
            Some(e.next_sibling()).filter(|e| e.is_valid())
        })
    }

    // === Tree Navigation by Name ===

    /// Get a child with a specified name.
    pub fn child(&self, name: &str) -> XMLElement<'a> {
        self.nav_named(lsl_child, name)
    }

    /// Get the next sibling with the specified name.
    pub fn next_sibling_named(&self, name: &str) -> XMLElement<'a> {
        self.nav_named(lsl_next_sibling_n, name)
    }

    /// Get the previous sibling with the specified name.
    pub fn previous_sibling_named(&self, name: &str) -> XMLElement<'a> {
        self.nav_named(lsl_previous_sibling_n, name)
    }

    // === Content Queries ===

    /// Returns true if the current node is valid, false otherwise.
    pub fn is_valid(&self) -> bool {
        !self.cursor.is_null()
    }

    /// Whether this node is empty.
    pub fn empty(&self) -> bool {
        unsafe { lsl_empty(self.cursor) != 0 }
    }

    /// Whether this is a text body (instead of an XML element). True both for plain char
    /// data and CData.
    pub fn is_text(&self) -> bool {
        unsafe { lsl_is_text(self.cursor) != 0 }
    }

    /// Name of the element.
    pub fn name(&self) -> String {
        unsafe { make_string(lsl_name(self.cursor)) }
    }

    /// Value of the element.
    pub fn value(&self) -> String {
        unsafe { make_string(lsl_value(self.cursor)) }
    }

    /// Get child value (value of the first child that is text).
    pub fn child_value(&self) -> String {
        unsafe { make_string(lsl_child_value(self.cursor)) }
    }

    /// Get child value of a child with a specified name.
    pub fn child_value_named(&self, name: &str) -> String {
        match ffi::CString::new(name) {
            Ok(name) => unsafe { make_string(lsl_child_value_n(self.cursor, name.as_ptr())) },
            Err(_) => String::new(),
        }
    }
}

impl fmt::Display for XMLElement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_valid() {
            write!(
                f,
                "(name={}, value={}, parent name={})",
                self.name(),
                self.value(),
                self.parent().name()
            )
        } else {
            write!(f, "(not valid)")
        }
    }
}

/**
A mutable cursor into the XML description of a `StreamInfo` (see `StreamInfo::desc_mut()`).

To guarantee that no cursor can ever point to a removed (freed) node, only one mutable cursor is
usable at a time:
* Methods that descend into a child (`append_child`, `child`, `first_child`, ...) borrow this
  cursor until the returned child cursor is no longer used.
* Methods that move sideways (`next_sibling`, ...) consume this cursor.
* There is no upward navigation; keep the parent cursor around instead.
* Read-only access (including upward navigation) is available through `as_ref()`.

Operations on non-existent nodes are safe no-ops (the resulting cursors are not `is_valid()`).
Strings with embedded zero bytes yield `Error::BadArgument`.

# Example

```
# fn main() -> lsl::Result<()> {
let mut info = lsl::StreamInfo::new("Dev", "EEG", 2, 100.0, lsl::ChannelFormat::Float32, "")?;
let mut desc = info.desc_mut();
desc.append_child_value("manufacturer", "SiFi Labs")?;
let mut channels = desc.append_child("channels")?;
for label in ["C3", "C4"] {
    channels
        .append_child("channel")?
        .append_child_value("label", label)?
        .append_child_value("unit", "microvolts")?;
}
assert_eq!(info.desc().child("channels").children().count(), 2);
# Ok(())
# }
```

A cursor cannot be used after its node may have been removed:

```compile_fail
# let mut info = lsl::StreamInfo::new("Dev", "EEG", 2, 100.0, lsl::ChannelFormat::Float32, "").unwrap();
let mut desc = info.desc_mut();
let child = desc.append_child("x").unwrap();
desc.remove_child_named("x").unwrap(); // error: `desc` is still borrowed by `child`
println!("{}", child.name());
```

```compile_fail
# let mut info = lsl::StreamInfo::new("Dev", "EEG", 2, 100.0, lsl::ChannelFormat::Float32, "").unwrap();
let read = info.desc().child("x");
info.desc_mut().remove_child_named("x").unwrap(); // error: `info` is borrowed by `read`
println!("{}", read.name());
```
*/
#[derive(Debug)]
pub struct XMLElementMut<'a> {
    cursor: lsl_xml_ptr,
    _info: PhantomData<&'a mut StreamInfo>,
}

// SAFETY: an `XMLElementMut` is an exclusive borrow of a `StreamInfo`, which is `Send`.
unsafe impl Send for XMLElementMut<'_> {}

impl<'a> XMLElementMut<'a> {
    fn new(cursor: lsl_xml_ptr) -> XMLElementMut<'a> {
        XMLElementMut {
            cursor,
            _info: PhantomData,
        }
    }

    fn named(
        &mut self,
        f: unsafe extern "C" fn(lsl_xml_ptr, *const c_char) -> lsl_xml_ptr,
        name: &str,
    ) -> Result<lsl_xml_ptr> {
        let name = ffi::CString::new(name)?;
        Ok(unsafe { f(self.cursor, name.as_ptr()) })
    }

    /// Read-only view of this element (allows navigating anywhere, including upwards).
    pub fn as_ref(&self) -> XMLElement<'_> {
        XMLElement::new(self.cursor)
    }

    /// Returns true if the current node is valid, false otherwise.
    pub fn is_valid(&self) -> bool {
        !self.cursor.is_null()
    }

    /// Name of the element.
    pub fn name(&self) -> String {
        self.as_ref().name()
    }

    /// Value of the element.
    pub fn value(&self) -> String {
        self.as_ref().value()
    }

    // === Navigation (downwards: borrowing) ===

    /// Get the first child of the element.
    pub fn first_child(&mut self) -> XMLElementMut<'_> {
        XMLElementMut::new(unsafe { lsl_first_child(self.cursor) })
    }

    /// Get the last child of the element.
    pub fn last_child(&mut self) -> XMLElementMut<'_> {
        XMLElementMut::new(unsafe { lsl_last_child(self.cursor) })
    }

    /// Get a child with a specified name (an invalid cursor if there is none).
    pub fn child(&mut self, name: &str) -> XMLElementMut<'_> {
        let cursor = self.named(lsl_child, name).unwrap_or(std::ptr::null_mut());
        XMLElementMut::new(cursor)
    }

    // === Navigation (sideways: consuming) ===

    /// Move to the next sibling in the children list of the parent node.
    pub fn next_sibling(self) -> XMLElementMut<'a> {
        XMLElementMut::new(unsafe { lsl_next_sibling(self.cursor) })
    }

    /// Move to the previous sibling in the children list of the parent node.
    pub fn previous_sibling(self) -> XMLElementMut<'a> {
        XMLElementMut::new(unsafe { lsl_previous_sibling(self.cursor) })
    }

    /// Move to the next sibling with the specified name.
    pub fn next_sibling_named(mut self, name: &str) -> XMLElementMut<'a> {
        let cursor = self
            .named(lsl_next_sibling_n, name)
            .unwrap_or(std::ptr::null_mut());
        XMLElementMut::new(cursor)
    }

    /// Move to the previous sibling with the specified name.
    pub fn previous_sibling_named(mut self, name: &str) -> XMLElementMut<'a> {
        let cursor = self
            .named(lsl_previous_sibling_n, name)
            .unwrap_or(std::ptr::null_mut());
        XMLElementMut::new(cursor)
    }

    // === Modification ===

    /**
    Append a child node with a given name, and give it a (nameless) plain-text child with
    the given text value.

    Returns this element (not the child), to allow chaining.
    */
    pub fn append_child_value(&mut self, name: &str, value: &str) -> Result<&mut Self> {
        let name = ffi::CString::new(name)?;
        let value = ffi::CString::new(value)?;
        unsafe { lsl_append_child_value(self.cursor, name.as_ptr(), value.as_ptr()) };
        Ok(self)
    }

    /**
    Prepend a child node with a given name and give it a (nameless) plain-text child with
    the given text value.

    Returns this element (not the child), to allow chaining.
    */
    pub fn prepend_child_value(&mut self, name: &str, value: &str) -> Result<&mut Self> {
        let name = ffi::CString::new(name)?;
        let value = ffi::CString::new(value)?;
        unsafe { lsl_prepend_child_value(self.cursor, name.as_ptr(), value.as_ptr()) };
        Ok(self)
    }

    /// Set the text value of the (nameless) plain-text child of a named child node.
    /// Fails with `Error::BadArgument` if there is no such child.
    pub fn set_child_value(&mut self, name: &str, value: &str) -> Result<()> {
        let name = ffi::CString::new(name)?;
        let value = ffi::CString::new(value)?;
        match unsafe { lsl_set_child_value(self.cursor, name.as_ptr(), value.as_ptr()) } {
            0 => Err(Error::BadArgument),
            _ => Ok(()),
        }
    }

    /// Set the element's name. Fails with `Error::BadArgument` if the node is empty.
    pub fn set_name(&mut self, name: &str) -> Result<()> {
        let name = ffi::CString::new(name)?;
        match unsafe { lsl_set_name(self.cursor, name.as_ptr()) } {
            0 => Err(Error::BadArgument),
            _ => Ok(()),
        }
    }

    /// Set the element's value. Fails with `Error::BadArgument` if the node is empty.
    pub fn set_value(&mut self, value: &str) -> Result<()> {
        let value = ffi::CString::new(value)?;
        match unsafe { lsl_set_value(self.cursor, value.as_ptr()) } {
            0 => Err(Error::BadArgument),
            _ => Ok(()),
        }
    }

    /// Append a child element with the specified name and return a cursor to it.
    pub fn append_child(&mut self, name: &str) -> Result<XMLElementMut<'_>> {
        Ok(XMLElementMut::new(self.named(lsl_append_child, name)?))
    }

    /// Prepend a child element with the specified name and return a cursor to it.
    pub fn prepend_child(&mut self, name: &str) -> Result<XMLElementMut<'_>> {
        Ok(XMLElementMut::new(self.named(lsl_prepend_child, name)?))
    }

    /// Append a copy of the specified element (e.g., from another `StreamInfo`) as a child and
    /// return a cursor to the copy.
    pub fn append_copy(&mut self, e: &XMLElement<'_>) -> XMLElementMut<'_> {
        XMLElementMut::new(unsafe { lsl_append_copy(self.cursor, e.cursor) })
    }

    /// Prepend a copy of the specified element (e.g., from another `StreamInfo`) as a child and
    /// return a cursor to the copy.
    pub fn prepend_copy(&mut self, e: &XMLElement<'_>) -> XMLElementMut<'_> {
        XMLElementMut::new(unsafe { lsl_prepend_copy(self.cursor, e.cursor) })
    }

    /// Remove the first child element with the specified name (if any).
    pub fn remove_child_named(&mut self, name: &str) -> Result<()> {
        let name = ffi::CString::new(name)?;
        unsafe { lsl_remove_child_n(self.cursor, name.as_ptr()) };
        Ok(())
    }

    /// Remove this element (and all its children) from the document.
    pub fn remove(self) {
        unsafe { lsl_remove_child(lsl_parent(self.cursor), self.cursor) }
    }
}

impl fmt::Display for XMLElementMut<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_ref().fmt(f)
    }
}

// =============================
// ==== Continuous Resolver ====
// =============================

/**
A convenience class that resolves streams continuously in the background.

This object can be queried at any time for the set of streams that are currently visible on the
network.

**Examples:** the `resolving_continuously.rs` example (found in the crate's github repository)
illustrates the use of the `ContinuousResolver`.
*/
#[derive(Debug)]
pub struct ContinuousResolver {
    handle: lsl_continuous_resolver,
}

// SAFETY: the native resolver is not tied to the thread that created it.
unsafe impl Send for ContinuousResolver {}

impl ContinuousResolver {
    /**
    Construct a new continuous_resolver that resolves all streams on the network.

    This is analogous to the functionality offered by the free function `resolve_streams()`.

    Arguments:
    * `forget_after` When a stream is no longer visible on the network (e.g., because it was
      shut down), this is the time in seconds after which it is no longer reported by the
      resolver. A good value here is 5.0 to report any stream that had been visible in the last
      5 seconds.
    */
    pub fn new(forget_after: f64) -> Result<ContinuousResolver> {
        if forget_after.is_nan() || forget_after <= 0.0 {
            return Err(Error::BadArgument);
        }
        ContinuousResolver::from_raw(unsafe { lsl_create_continuous_resolver(forget_after) })
    }

    /**
    Construct a new `ContinuousResolver` that resolves all streams with a specific value for a
    given property.

    This is analogous to the functionality provided by the free function `resolve_byprop()`.

    Arguments:
    * `prop`: The `StreamInfo` property that should have a specific value (e.g., "name", "type",
      "source_id", or "desc/manufaturer").
    * `value`: The string value that the property should have (e.g., "EEG" as the type property).
    * `forget_after`: When a stream is no longer visible on the network (e.g., because it was shut
      down), this is the time in seconds after which it is no longer reported by the resolver.
    */
    pub fn new_with_prop(prop: &str, value: &str, forget_after: f64) -> Result<ContinuousResolver> {
        if forget_after.is_nan() || forget_after <= 0.0 {
            return Err(Error::BadArgument);
        }
        let prop = ffi::CString::new(prop)?;
        let value = ffi::CString::new(value)?;
        ContinuousResolver::from_raw(unsafe {
            lsl_create_continuous_resolver_byprop(prop.as_ptr(), value.as_ptr(), forget_after)
        })
    }

    /**
    Construct a new `ContinuousResolver` that resolves all streams matching a given predicate.

    This is analogous to the functionality provided by the free function `resolve_bypred()`.

    Arguments:
    * `pred`: An XPath 1.0 predicate, e.g. `name='BioSemi'` (see `resolve_bypred()`).
    * `forget_after`: When a stream is no longer visible on the network (e.g., because it was shut
      down), this is the time in seconds after which it is no longer reported by the resolver.
    */
    pub fn new_with_pred(pred: &str, forget_after: f64) -> Result<ContinuousResolver> {
        if forget_after.is_nan() || forget_after <= 0.0 {
            return Err(Error::BadArgument);
        }
        let pred = ffi::CString::new(pred)?;
        ContinuousResolver::from_raw(unsafe {
            lsl_create_continuous_resolver_bypred(pred.as_ptr(), forget_after)
        })
    }

    /**
    Obtain the set of currently present streams on the network (i.e. resolve result).

    Returns a vector of matching stream info objects (excluding their meta-data), any of which can
    subsequently be used to open an inlet.
    */
    pub fn results(&self) -> Result<Vec<StreamInfo>> {
        collect_resolved(|buf, len| unsafe { lsl_resolver_results(self.handle, buf, len) })
    }

    fn from_raw(handle: lsl_continuous_resolver) -> Result<ContinuousResolver> {
        if handle.is_null() {
            Err(Error::ResourceCreation)
        } else {
            Ok(ContinuousResolver { handle })
        }
    }
}

impl Drop for ContinuousResolver {
    fn drop(&mut self) {
        unsafe {
            lsl_destroy_continuous_resolver(self.handle);
        }
    }
}

// ========================
// === Internal Helpers ===
// ========================

fn transport_flags(options: &[TransportOption]) -> lsl_transport_options_t {
    options
        .iter()
        .fold(lsl_transport_options_t_transp_default, |acc, &opt| {
            acc | opt as lsl_transport_options_t
        })
}

// helper functions for interop with native data types in the lsl_sys module
impl ChannelFormat {
    /// Convert to corresponding native data type.
    pub fn to_native(&self) -> lsl_channel_format_t {
        match self {
            ChannelFormat::Float32 => lsl_channel_format_t_cft_float32,
            ChannelFormat::Double64 => lsl_channel_format_t_cft_double64,
            ChannelFormat::String => lsl_channel_format_t_cft_string,
            ChannelFormat::Int32 => lsl_channel_format_t_cft_int32,
            ChannelFormat::Int16 => lsl_channel_format_t_cft_int16,
            ChannelFormat::Int8 => lsl_channel_format_t_cft_int8,
            ChannelFormat::Int64 => lsl_channel_format_t_cft_int64,
            ChannelFormat::Undefined => lsl_channel_format_t_cft_undefined,
        }
    }

    /// Convert from the corresponding native data type.
    pub fn from_native(fmt: lsl_channel_format_t) -> ChannelFormat {
        #[allow(non_upper_case_globals)]
        match fmt {
            lsl_channel_format_t_cft_float32 => ChannelFormat::Float32,
            lsl_channel_format_t_cft_double64 => ChannelFormat::Double64,
            lsl_channel_format_t_cft_string => ChannelFormat::String,
            lsl_channel_format_t_cft_int32 => ChannelFormat::Int32,
            lsl_channel_format_t_cft_int16 => ChannelFormat::Int16,
            lsl_channel_format_t_cft_int8 => ChannelFormat::Int8,
            lsl_channel_format_t_cft_int64 => ChannelFormat::Int64,
            // Note that this will convert any unknown values that come ouf of the lib
            // into Undefined
            _ => ChannelFormat::Undefined,
        }
    }
}

impl fmt::Display for ChannelFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ChannelFormat::Float32 => "float32",
            ChannelFormat::Double64 => "double64",
            ChannelFormat::String => "string",
            ChannelFormat::Int32 => "int32",
            ChannelFormat::Int16 => "int16",
            ChannelFormat::Int8 => "int8",
            ChannelFormat::Int64 => "int64",
            ChannelFormat::Undefined => "undefined",
        };
        f.write_str(s)
    }
}

// error type conversion
impl From<ffi::NulError> for Error {
    fn from(_: ffi::NulError) -> Error {
        Error::BadArgument
    }
}

// human-readable error messages
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let msg = match self {
            Error::Timeout => "operation timed out",
            Error::StreamLost => "stream has been lost",
            Error::BadArgument => "incorrectly specified argument",
            Error::ResourceCreation => "resource creation failed",
            Error::Internal => "internal error in native library",
            Error::Unknown => "unknown error",
        };
        f.write_str(msg)
    }
}

impl std::error::Error for Error {}

// Create a String from a const char* returned by liblsl (empty if NULL). Invalid UTF-8 is
// replaced by placeholder characters.
unsafe fn make_string(s: *const c_char) -> String {
    if s.is_null() {
        String::new()
    } else {
        ffi::CStr::from_ptr(s).to_string_lossy().into_owned()
    }
}

// check whether a given value that may be an error code signals an error,
// and convert to the correct Err() type or Ok(value) otherwise
fn errcode_to_result(ec: i32) -> Result<i32> {
    if ec < 0 {
        #[allow(non_upper_case_globals)]
        match ec {
            lsl_error_code_t_lsl_timeout_error => Err(Error::Timeout),
            lsl_error_code_t_lsl_argument_error => Err(Error::BadArgument),
            lsl_error_code_t_lsl_lost_error => Err(Error::StreamLost),
            lsl_error_code_t_lsl_internal_error => Err(Error::Internal),
            _ => Err(Error::Unknown),
        }
    } else {
        Ok(ec)
    }
}
