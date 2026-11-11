use lsl::{
    ChannelFormat, Error, ExPushable, Pullable, Pushable, StreamInfo, StreamInlet, StreamOutlet,
};

#[test]
fn clock_is_working() {
    assert_ne!(lsl::local_clock(), 0.0);
}

#[test]
fn library_version_matches_submodule() {
    // bump this together with the lsl-sys/liblsl submodule
    assert_eq!(lsl::library_version(), 117);
}

#[test]
fn handles_are_send() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    assert_send::<StreamInfo>();
    assert_sync::<StreamInfo>();
    assert_send::<StreamOutlet>();
    assert_send::<StreamInlet>();
    assert_send::<lsl::ContinuousResolver>();
}

#[test]
fn streaminfo_basic() {
    let info =
        StreamInfo::new("MyStream", "EEG", 8, 100.0, ChannelFormat::Float32, "12345").unwrap();
    assert_eq!(info.stream_name(), "MyStream");
    assert_eq!(info.stream_type(), "EEG");
    assert_eq!(info.channel_count(), 8);
    assert_eq!(info.nominal_srate(), 100.0);
    assert_eq!(info.channel_format(), ChannelFormat::Float32);
    assert_eq!(info.source_id(), "12345");
    assert!(info.matches_query("name='MyStream' and type='EEG'"));
    assert!(!info.matches_query("name='MyStream' and type='ECG'"));
    let info2 = info.clone();
    assert_eq!(info2.stream_name(), "MyStream");
}

#[test]
fn streaminfo_bad_arguments() {
    assert_eq!(
        StreamInfo::new("", "EEG", 1, 1.0, ChannelFormat::Float32, "").unwrap_err(),
        Error::BadArgument
    );
    assert_eq!(
        StreamInfo::new("a\0b", "EEG", 1, 1.0, ChannelFormat::Float32, "").unwrap_err(),
        Error::BadArgument
    );
    assert_eq!(
        StreamInfo::new("a", "EEG", 1, -1.0, ChannelFormat::Float32, "").unwrap_err(),
        Error::BadArgument
    );
    assert_eq!(
        StreamInfo::new("a", "EEG", 1, f64::NAN, ChannelFormat::Float32, "").unwrap_err(),
        Error::BadArgument
    );
}

#[test]
fn streaminfo_xml() {
    let mut info =
        StreamInfo::new("MyStream", "EEG", 8, 100.0, ChannelFormat::Float32, "12345").unwrap();
    {
        let mut desc = info.desc_mut();
        let mut channels = desc.append_child("channels").unwrap();
        for label in ["C3", "C4", "Cz"] {
            channels
                .append_child("channel")
                .unwrap()
                .append_child_value("label", label)
                .unwrap()
                .append_child_value("unit", "microvolts")
                .unwrap();
        }
        // embedded zero bytes are rejected instead of panicking
        assert_eq!(
            channels.append_child("bad\0name").err(),
            Some(Error::BadArgument)
        );
        assert_eq!(
            channels.append_child_value("label", "x\0").err(),
            Some(Error::BadArgument)
        );
    }

    let labels: Vec<_> = info
        .desc()
        .child("channels")
        .children()
        .map(|c| c.child_value_named("label"))
        .collect();
    assert_eq!(labels, ["C3", "C4", "Cz"]);
    assert!(!info.desc().child("missing").is_valid());
    assert_eq!(info.desc().child("channels").parent().name(), "desc");

    let xml = info.to_xml().unwrap();
    assert!(xml.contains("<name>MyStream</name>"));
    assert!(xml.contains("<label>C4</label>"));

    // a deep copy is unaffected by later changes to the original
    let copy = info.clone();

    // removal through the mutable cursor
    {
        let mut desc = info.desc_mut();
        let mut channels = desc.child("channels");
        let second = channels.first_child().next_sibling();
        assert_eq!(second.as_ref().child_value_named("label"), "C4");
        second.remove();
        channels.remove_child_named("channel").unwrap(); // removes C3
    }
    let labels: Vec<_> = info
        .desc()
        .child("channels")
        .children()
        .map(|c| c.child_value_named("label"))
        .collect();
    assert_eq!(labels, ["Cz"]);
    assert_eq!(copy.desc().child("channels").children().count(), 3);

    // copying elements between infos
    let mut other = StreamInfo::new("Other", "EEG", 8, 100.0, ChannelFormat::Float32, "").unwrap();
    other.desc_mut().append_copy(&copy.desc().child("channels"));
    assert_eq!(other.desc().child("channels").children().count(), 3);

    // round-trip through XML (requires the uid that an outlet assigns)
    assert_eq!(StreamInfo::from_xml(&xml).unwrap_err(), Error::BadArgument);
    assert_eq!(
        StreamInfo::from_xml("<not xml").unwrap_err(),
        Error::BadArgument
    );
    let outlet = StreamOutlet::new(&copy, 0, 1).unwrap();
    let parsed = StreamInfo::from_xml(&outlet.info().unwrap().to_xml().unwrap()).unwrap();
    assert_eq!(parsed.stream_name(), "MyStream");
    assert_eq!(parsed.desc().child("channels").children().count(), 3);
}

#[test]
fn outlet_rejects_bad_sizes() {
    let info = StreamInfo::new(
        "BadSizes",
        "Test",
        2,
        100.0,
        ChannelFormat::Float32,
        "bad-sizes",
    )
    .unwrap();
    let outlet = StreamOutlet::new(&info, 0, 1).unwrap();
    assert_eq!(outlet.push_sample(&[1.0f32]), Err(Error::BadArgument));
    assert_eq!(
        outlet.push_sample(&[1.0f32, 2.0, 3.0]),
        Err(Error::BadArgument)
    );
    assert_eq!(
        outlet.push_chunk(&[1.0f32, 2.0, 3.0]),
        Err(Error::BadArgument)
    );
    assert_eq!(
        outlet.push_chunk_stamped(&[1.0f32, 2.0], &[1.0, 2.0]),
        Err(Error::BadArgument)
    );
    assert_eq!(outlet.push_sample(&["one"]), Err(Error::BadArgument));
    // empty chunks are a no-op
    outlet.push_chunk(&[] as &[f32]).unwrap();
    outlet.push_chunk_stamped(&[] as &[f32], &[]).unwrap();
    // zero-channel streams cannot have outlets
    let blank = StreamInfo::from_blank().unwrap();
    assert_eq!(
        StreamOutlet::new(&blank, 0, 1).unwrap_err(),
        Error::BadArgument
    );
}

// Create a connected outlet/inlet pair on a stream with a unique source id.
fn connected_pair(name: &str, channels: u32, format: ChannelFormat) -> (StreamOutlet, StreamInlet) {
    let source_id = format!("{}-{}-{}", name, std::process::id(), lsl::local_clock());
    let info = StreamInfo::new(name, "Test", channels, 100.0, format, &source_id).unwrap();
    let outlet = StreamOutlet::new(&info, 0, 10).unwrap();
    let found = lsl::resolve_byprop("source_id", &source_id, 1, 5.0).unwrap();
    assert_eq!(found.len(), 1);
    let inlet = StreamInlet::new(&found[0], 10, 0, false).unwrap();
    inlet.open_stream(5.0).unwrap();
    assert!(outlet.wait_for_consumers(5.0));
    (outlet, inlet)
}

#[test]
fn loopback_samples() {
    let (outlet, inlet) = connected_pair("LoopSamples", 3, ChannelFormat::Float32);

    // push from a different thread to exercise Send
    let pusher = std::thread::spawn(move || {
        for k in 0..10 {
            let v = k as f32;
            outlet
                .push_sample_ex(&[v, v + 0.5, -v], 1000.0 + k as f64, true)
                .unwrap();
        }
        outlet
    });
    let _outlet = pusher.join().unwrap();

    let mut buf: Vec<f32> = Vec::new();
    for k in 0..10 {
        let v = k as f32;
        if k % 2 == 0 {
            let (sample, ts): (Vec<f32>, f64) = inlet.pull_sample(5.0).unwrap();
            assert_eq!(sample, [v, v + 0.5, -v]);
            assert_eq!(ts, 1000.0 + k as f64);
        } else {
            let ts = inlet.pull_sample_buf(&mut buf, 5.0).unwrap();
            assert_eq!(buf, [v, v + 0.5, -v]);
            assert_eq!(ts, 1000.0 + k as f64);
        }
    }
    let (sample, ts): (Vec<f32>, f64) = inlet.pull_sample(0.0).unwrap();
    assert!(sample.is_empty());
    assert_eq!(ts, 0.0);
}

#[test]
fn loopback_chunks() {
    let (outlet, inlet) = connected_pair("LoopChunks", 2, ChannelFormat::Int32);

    // 3 samples with per-sample stamps, then 2 samples stamped from the last one at 100 Hz
    outlet
        .push_chunk_stamped(&[1, 2, 3, 4, 5, 6], &[10.0, 11.0, 12.0])
        .unwrap();
    outlet.push_chunk_ex(&[7i32, 8, 9, 10], 20.0, true).unwrap();

    // pull into fixed buffers until we have everything
    let mut data = vec![0i32; 10];
    let mut stamps = [0.0; 5];
    let mut have = 0;
    while have < 5 {
        let got = inlet
            .pull_chunk_into(&mut data[have * 2..], &mut stamps[have..], 5.0)
            .unwrap();
        assert!(got > 0);
        have += got;
    }
    assert_eq!(data, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    assert_eq!(&stamps[..3], [10.0, 11.0, 12.0]);
    assert!((stamps[3] - 19.99).abs() < 1e-9);
    assert_eq!(stamps[4], 20.0);

    // mismatched buffers are rejected
    assert_eq!(
        inlet.pull_chunk_into(&mut data[..3], &mut stamps[..1], 0.0),
        Err(Error::BadArgument)
    );

    // pull_chunk collects everything available
    outlet.push_chunk(&[11i32, 12, 13, 14]).unwrap();
    let mut collected: (Vec<i32>, Vec<f64>) = (vec![], vec![]);
    let deadline = lsl::local_clock() + 5.0;
    while collected.1.len() < 2 && lsl::local_clock() < deadline {
        let (d, s): (Vec<i32>, Vec<f64>) = inlet.pull_chunk().unwrap();
        collected.0.extend(d);
        collected.1.extend(s);
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert_eq!(collected.0, [11, 12, 13, 14]);
    assert_eq!(collected.1.len(), 2);

    // flush drops pending samples
    outlet.push_chunk(&[0i32; 8]).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(200));
    inlet.flush();
    let (d, s): (Vec<i32>, Vec<f64>) = inlet.pull_chunk().unwrap();
    assert!(d.is_empty() && s.is_empty());
}

#[test]
fn loopback_strings() {
    let (outlet, inlet) = connected_pair("LoopStrings", 2, ChannelFormat::String);

    outlet.push_sample(&["hello", ""]).unwrap();
    outlet
        .push_sample(&[String::from("wörld"), String::from("x")])
        .unwrap();
    outlet
        .push_sample(&[&b"\x00\xffbin"[..], &b"y"[..]])
        .unwrap();
    outlet
        .push_chunk_stamped(&["a", "b", "c", "d"], &[1.0, 2.0])
        .unwrap();

    let (s, _): (Vec<String>, f64) = inlet.pull_sample(5.0).unwrap();
    assert_eq!(s, ["hello", ""]);
    let mut buf: Vec<String> = Vec::new();
    inlet.pull_sample_buf(&mut buf, 5.0).unwrap();
    assert_eq!(buf, ["wörld", "x"]);
    let (b, _): (Vec<Vec<u8>>, f64) = inlet.pull_sample(5.0).unwrap();
    assert_eq!(b, [b"\x00\xffbin".to_vec(), b"y".to_vec()]);

    // buffer larger than what is available: the unused slots must still be freed by the wrapper
    let mut data = vec![String::new(); 8];
    let mut stamps = vec![0.0; 4];
    // (the timeout is a deadline for filling the whole buffer, so keep it short)
    let got = inlet.pull_chunk_into(&mut data, &mut stamps, 0.5).unwrap();
    assert_eq!(got, 2);
    assert_eq!(&data[..4], ["a", "b", "c", "d"]);
    assert_eq!(&stamps[..2], [1.0, 2.0]);
}

#[test]
fn transport_options() {
    let info = StreamInfo::new(
        "Transport",
        "Test",
        1,
        100.0,
        ChannelFormat::Double64,
        "transport",
    )
    .unwrap();
    let outlet =
        StreamOutlet::new_ex(&info, 0, 500, &[lsl::TransportOption::BufsizeSamples]).unwrap();
    assert_eq!(outlet.channel_count(), 1);
    let inlet = StreamInlet::new_ex(
        &info,
        1000,
        0,
        false,
        &[lsl::TransportOption::BufsizeThousandths],
    )
    .unwrap();
    assert_eq!(inlet.channel_count(), 1);
}
