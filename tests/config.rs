// Runs in its own process: the configuration must be set before liblsl is first used.

#[test]
fn config_content_is_applied() {
    lsl::set_config_content("[lab]\nSessionID = cfgtest\n").unwrap();
    let info =
        lsl::StreamInfo::new("Cfg", "Test", 1, 1.0, lsl::ChannelFormat::Float32, "").unwrap();
    // the session id is assigned from the configuration when the outlet is created
    let outlet = lsl::StreamOutlet::new(&info, 0, 1).unwrap();
    assert_eq!(outlet.info().unwrap().session_id(), "cfgtest");
}
