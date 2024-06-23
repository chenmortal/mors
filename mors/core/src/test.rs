#[cfg(test)]
mod tests {

    use tokio_util::{sync::CancellationToken, task::TaskTracker};
    #[tokio::test]
    async fn test_fs() {
        let tracker = TaskTracker::new();

        for i in 0..10 {
            tracker.spawn(some_operation(i));
        }

        // Once we spawned everything, we close the tracker.
        // tracker.close();

        // Wait for everything to finish.
        tracker.wait().await;

        println!("This is printed after all of the tasks.");
    }

    async fn some_operation(i: u64) {
        // sleep(Duration::from_millis(100 * i)).await;
        println!("Task {} shutting down.", i);
    }

    #[tokio::test]
    async fn test_fs2() {
        let token = CancellationToken::new();

        // Step 2: Clone the token for use in another task
        let cloned_token = token.clone();

        let child_clone = cloned_token.clone();

        let a = token.child_token();

        // Task 1 - Wait for token cancellation or a long time
        let task1_handle = tokio::spawn(async move {
            tokio::select! {
                // Step 3: Using cloned token to listen to cancellation requests
                _ = cloned_token.cancelled() => {
                    // The token was cancelled, task can shut down
                }
                _ = tokio::time::sleep(std::time::Duration::from_secs(9999)) => {
                    // Long work has completed
                }
            }
        });

        // Task 2 - Cancel the original token after a small delay
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;

            // Step 4: Cancel the original or cloned token to notify other tasks about shutting down gracefully
            token.cancel();
        });

        // Wait for tasks to complete
        task1_handle.await.unwrap()
    }
}
