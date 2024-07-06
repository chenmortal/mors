#[cfg(test)]
mod tests {
    use std::fs::create_dir;

    use mors_traits::default::DEFAULT_DIR;

    use crate::{MorsBuilder, Result};
    use tokio_util::task::TaskTracker;
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
    async fn test_build() -> Result<()> {
        let builder = MorsBuilder::default();
        create_dir(DEFAULT_DIR)?;
        let core = builder.build().await.unwrap();
        Ok(())
    }
}
