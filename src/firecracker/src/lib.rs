use std::{
    io::Write,
    os::unix::net::UnixStream,
    path::PathBuf,
    sync::{mpsc::channel, Arc, Mutex},
    thread,
};

use api_server::{ApiServer, ServerError};
use api_server_adapter::{ApiServerAdapter};
use event_manager::SubscriberOps;
use logger::{error, info, ProcessTimeReporter};
use seccompiler::BpfThreadMap;
use utils::eventfd::EventFd;
use vmm::{
    resources::VmResources, rpc_interface::PrebootApiController,
    vmm_config::instance_info::InstanceInfo, EventManager, FcExitCode,
};

pub mod api_server_adapter;
pub mod metrics;

// Configure and start a microVM as described by the command-line JSON.
pub fn build_microvm_from_json(
    seccomp_filters: &BpfThreadMap,
    event_manager: &mut EventManager,
    config_json: String,
    instance_info: InstanceInfo,
    boot_timer_enabled: bool,
    mmds_size_limit: usize,
    metadata_json: Option<&str>,
) -> std::result::Result<(VmResources, Arc<Mutex<vmm::Vmm>>), FcExitCode> {
    let mut vm_resources =
        VmResources::from_json(&config_json, &instance_info, mmds_size_limit, metadata_json)
            .map_err(|err| {
                error!("Configuration for VMM from one single json failed: {}", err);
                vmm::FcExitCode::BadConfiguration
            })?;
    vm_resources.boot_timer = boot_timer_enabled;
    let vmm = vmm::builder::build_microvm_for_boot(
        &instance_info,
        &vm_resources,
        event_manager,
        seccomp_filters,
    )
    .map_err(|err| {
        error!(
            "Building VMM configured from cmdline json failed: {:?}",
            err
        );
        vmm::FcExitCode::BadConfiguration
    })?;
    info!("Successfully started microvm that was configured from one single json");

    Ok((vm_resources, vmm))
}

pub fn run_without_api(
    seccomp_filters: &BpfThreadMap,
    config_json: Option<String>,
    instance_info: InstanceInfo,
    bool_timer_enabled: bool,
    mmds_size_limit: usize,
    metadata_json: Option<&str>,
) -> FcExitCode {
    let mut event_manager = EventManager::new().expect("Unable to create EventManager");

    // Create the firecracker metrics object responsible for periodically printing metrics.
    let firecracker_metrics = Arc::new(Mutex::new(metrics::PeriodicMetrics::new()));
    event_manager.add_subscriber(firecracker_metrics.clone());

    // Build the microVm. We can ignore VmResources since it's not used without api.
    let (_, vmm) = match build_microvm_from_json(
        seccomp_filters,
        &mut event_manager,
        // Safe to unwrap since '--no-api' requires this to be set.
        config_json.unwrap(),
        instance_info,
        bool_timer_enabled,
        mmds_size_limit,
        metadata_json,
    ) {
        Ok((res, vmm)) => (res, vmm),
        Err(exit_code) => return exit_code,
    };

    // Start the metrics.
    firecracker_metrics
        .lock()
        .expect("Poisoned lock")
        .start(metrics::WRITE_METRICS_PERIOD_MS);

    // Run the EventManager that drives everything in the microVM.
    loop {
        event_manager
            .run()
            .expect("Failed to start the event manager");

        if let Some(exit_code) = vmm.lock().unwrap().shutdown_exit_code() {
            return exit_code;
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn run_with_api(
    seccomp_filters: &mut BpfThreadMap,
    config_json: Option<String>,
    bind_path: PathBuf,
    instance_info: InstanceInfo,
    process_time_reporter: ProcessTimeReporter,
    boot_timer_enabled: bool,
    api_payload_limit: usize,
    mmds_size_limit: usize,
    metadata_json: Option<&str>,
) -> FcExitCode {
    // FD to notify of API events. This is a blocking eventfd by design.
    // It is used in the config/pre-boot loop which is a simple blocking loop
    // which only consumes API events.
    let api_event_fd = EventFd::new(0).expect("Cannot create API Eventfd.");

    // Channels for both directions between Vmm and Api threads.
    let (to_vmm, from_api) = channel();
    let (to_api, from_vmm) = channel();
    let (socket_ready_sender, socket_ready_receiver) = channel();

    let to_vmm_event_fd = api_event_fd
        .try_clone()
        .expect("Failed to clone API event FD");
    let api_bind_path = bind_path.clone();
    let api_seccomp_filter = seccomp_filters
        .remove("api")
        .expect("Missing seccomp filter for API thread.");

    // Start the separate API thread.
    let api_thread = thread::Builder::new()
        .name("fc_api".to_owned())
        .spawn(move || {
            match ApiServer::new(to_vmm, from_vmm, to_vmm_event_fd).bind_and_run(
                &api_bind_path,
                process_time_reporter,
                &api_seccomp_filter,
                api_payload_limit,
                socket_ready_sender,
            ) {
                Ok(_) => (),
                Err(api_server::Error::ServerCreation(ServerError::IOError(inner)))
                    if inner.kind() == std::io::ErrorKind::AddrInUse =>
                {
                    let sock_path = api_bind_path.display().to_string();
                    error!(
                        "Failed to open the API socket at: {sock_path}. Check that it is not \
                         already used."
                    );
                    std::process::exit(vmm::FcExitCode::GenericError as i32);
                }
                Err(api_server::Error::ServerCreation(err)) => {
                    error!("Failed to bind and run the HTTP server: {err}");
                    std::process::exit(vmm::FcExitCode::GenericError as i32);
                }
            }
        })
        .expect("API thread spawn failed.");

    let mut event_manager = EventManager::new().expect("Unable to create EventManager");
    // Create the firecracker metrics object responsible for periodically printing metrics.
    let firecracker_metrics = Arc::new(Mutex::new(metrics::PeriodicMetrics::new()));
    event_manager.add_subscriber(firecracker_metrics.clone());

    // Configure, build and start the microVM.
    let build_result = match config_json {
        Some(json) => build_microvm_from_json(
            seccomp_filters,
            &mut event_manager,
            json,
            instance_info,
            boot_timer_enabled,
            mmds_size_limit,
            metadata_json,
        ),
        None => PrebootApiController::build_microvm_from_requests(
            seccomp_filters,
            &mut event_manager,
            instance_info,
            || {
                let req = from_api
                    .recv()
                    .expect("The channel's sending half was disconnected. Cannot receive data.");
                // Also consume the API event along with the message. It is safe to unwrap()
                // because this event_fd is blocking.
                api_event_fd
                    .read()
                    .expect("VMM: Failed to read the API event_fd");
                *req
            },
            |response| {
                to_api
                    .send(Box::new(response))
                    .expect("one-shot channel closed")
            },
            boot_timer_enabled,
            mmds_size_limit,
            metadata_json,
        ),
    };

    let exit_code = match build_result {
        Ok((vm_resources, vmm)) => {
            // Start the metrics.
            firecracker_metrics
                .lock()
                .expect("Poisoned lock")
                .start(crate::metrics::WRITE_METRICS_PERIOD_MS);

            ApiServerAdapter::run_microvm(
                api_event_fd,
                from_api,
                to_api,
                vm_resources,
                vmm,
                &mut event_manager,
            )
        }
        Err(exit_code) => exit_code,
    };

    // We want to tell the API thread to shut down for a clean exit. But this is after
    // the Vmm.stop() has been called, so it's a moment of internal finalization (as
    // opposed to be something the client might call to shut the Vm down).  Since it's
    // an internal signal implementing it with an HTTP request is probably not the ideal
    // way to do it...but having another way would involve multiplexing micro-http server
    // with some other communication mechanism, or enhancing micro-http with exit
    // conditions.

    // We also need to make sure the socket path is ready.
    // The recv will return an error if the other end has already exited which means
    // that there is no need for us to send the "shutdown internal".
    let mut sock;
    if socket_ready_receiver.recv() == Ok(true) {
        // "sock" var is declared outside of this "if" scope so that the socket's fd stays
        // alive until all bytes are sent through; otherwise fd will close before being flushed.
        sock = UnixStream::connect(bind_path).unwrap();
        sock.write_all(b"PUT /shutdown-internal HTTP/1.1\r\n\r\n")
            .unwrap();
    }
    // This call to thread::join() should block until the API thread has processed the
    // shutdown-internal and returns from its function.
    api_thread.join().unwrap();
    exit_code
}
