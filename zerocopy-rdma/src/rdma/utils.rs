use std::io;

pub fn await_completions<const N: usize>(cq: &mut ibverbs::CompletionQueue) -> io::Result<()> {
    let mut completions = [ibverbs::ibv_wc::default(); N];
    let mut completed = 0;

    while completed < N {
        for completion in cq.poll(&mut completions)? {
            assert!(completion.error().is_none(), "{:?}", completion);
            completed += 1;
        }
    }
    Ok(())
}
