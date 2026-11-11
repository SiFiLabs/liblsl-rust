# 0.2.0
Fork of the unmaintained upstream crate; not published to crates.io. Breaking API changes:
- `StreamInfo` is now `Send + Sync` and owns its handle directly (`Clone` is a deep copy).
- XML access is split into a read-only `XMLElement<'a>` (`StreamInfo::desc()`) and an exclusive
  `XMLElementMut<'a>` (`StreamInfo::desc_mut()`) that borrow the `StreamInfo`. This makes it
  impossible to use a cursor to a removed node (previously a use-after-free in safe code).
  `XMLElementMut` descends by reborrowing, moves sideways by value, has no `parent()` (use
  `as_ref()` for read-only navigation), and replaces `remove_child(e)` by `remove()`. Methods
  taking strings return `Result` instead of panicking on embedded zero bytes. New:
  `XMLElement::children()`.
- Push/pull APIs take slices of values instead of `&Vec`; the trait type parameter is now the
  value type (e.g. `Pushable<f32>`, not `Pushable<Vec<f32>>`). Chunks are flat, multiplexed
  slices and go through liblsl's native chunk functions (one FFI call per chunk).
- New `Pullable::pull_chunk_into()` for allocation-free reading; `pull_chunk()` returns
  `(Vec<T>, Vec<f64>)` in multiplexed layout.
- Wrong sample/chunk sizes return `Error::BadArgument` instead of panicking.
- `i8` and `u8` are both accepted for `Int8` channels (instead of the platform's `c_char`).
- `StreamInfo::from_xml()` returns `Error::BadArgument` for invalid XML instead of an info named
  `(invalid: ...)`.
- `StreamInlet::smoothing_halftime()` returns a `Result`.
- Removed the `HasNominalRate` helper trait.

Additions and fixes:
- requires lsl-sys 0.2.0 (liblsl v1.17.7)
- `StreamOutlet`, `StreamInlet` and `ContinuousResolver` are now `Send`
- `StreamOutlet::new_ex()` / `StreamInlet::new_ex()` with `TransportOption`s
- `StreamInlet::flush()`, `channel_count()` on outlets and inlets
- `last_error()`, `set_config_filename()` and `set_config_content()`
- `Result` type alias is now public
- `i64` push/pull is now available on Windows
- fix panic in `push_chunk_stamped[_ex]()` when given an empty chunk
- edition 2021, CI workflow, loopback tests

# 0.1.1
- transferred repo from https://github.com/intheon to https://github.com/labstreaminglayer organization and updated links

# 0.1.0
- Initial Release