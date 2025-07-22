fn main() {
    let ctx = ibverbs::devices()
        .unwrap()
        .iter()
        .next()
        .expect("no rdma device available")
        .open()
        .unwrap();

    let cq = ctx.create_cq(16, 0).unwrap();
    let pd = ctx.alloc_pd().unwrap();

    let qp_builder = pd
        .create_qp(&cq, &cq, ibverbs::ibv_qp_type::IBV_QPT_RC)
        .unwrap()
        .set_gid_index(1)
        .allow_remote_rw()
        .build()
        .unwrap();

    let endpoint = qp_builder.endpoint().unwrap();
    let mut qp = qp_builder.handshake(endpoint).unwrap();

    let mut mr = pd.allocate(2).unwrap();
    mr.inner()[0] = 69;
    assert_eq!(mr.inner()[1], 0);

    let local = mr.slice(0..=0);
    let remote = mr.remote().slice(1..=1);
    qp.post_write(&[local], remote, 1, None).unwrap();

    let mut completions = [ibverbs::ibv_wc::default(); 1];
    loop {
        let completed = cq.poll(&mut completions).unwrap();
        if completed.is_empty() {
            continue;
        }
        assert_eq!(completed.len(), 1);
        assert!(completed[0].is_valid(), "{:?}", completed[0]);
        break;
    }
    assert_eq!(mr.inner()[1], 69);
}
