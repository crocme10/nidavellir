use clap::ArgMatches;
use slog::{info, Logger};
use slog::{o, Drain};
use std::thread;

use super::server::run_server;
use ctl2mimir::error;
use ctl2mimir::settings::Settings;
use ctl2mimir::state::State;

#[allow(clippy::needless_lifetimes)]
pub async fn test<'a>(matches: &ArgMatches<'a>, logger: Logger) -> Result<(), error::Error> {
    let settings = Settings::new(matches)?;

    // FIXME There is work that should be done here to terminate the service
    // when we are done with testing.
    if settings.testing {
        info!(logger, "Launching testing service");
        let handle = tokio::runtime::Handle::current();
        thread::spawn(move || {
            handle.spawn(async move {
                let decorator = slog_term::TermDecorator::new().build();
                let drain = slog_term::FullFormat::new(decorator).build().fuse();
                let drain = slog_async::Async::new(drain).build().fuse();
                let logger = slog::Logger::root(drain, o!());
                let state = State::new(&settings, &logger)
                    .await
                    .expect("state creation");
                run_server(settings, state).await
            });
        });
        //th.join().expect("Waiting for test execution");
    }

    test_environments();
    Ok(())
}

pub fn test_environments() {}
