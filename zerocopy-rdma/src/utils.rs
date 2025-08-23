use tokio::{io, task};

pub async fn await_completions<const N: usize>(
    cq: &mut ibverbs::CompletionQueue,
) -> io::Result<()> {
    let mut completions = [ibverbs::ibv_wc::default(); N];
    let mut completed = 0;

    while completed < N {
        for completion in cq.poll(&mut completions)? {
            assert!(
                completion.is_valid(),
                "Work Completion Error: {:?}",
                completion
            );
            completed += 1;
        }
        task::yield_now().await;
    }
    Ok(())
}
