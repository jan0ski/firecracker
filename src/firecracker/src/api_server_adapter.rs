// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::os::unix::io::AsRawFd;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex};

use api_server::{ApiRequest, ApiResponse};
use event_manager::{EventOps, Events, MutEventSubscriber, SubscriberOps};
use logger::{error, warn};
use utils::epoll::EventSet;
use utils::eventfd::EventFd;
use vmm::resources::VmResources;
use vmm::rpc_interface::{RuntimeApiController, VmmAction};
use vmm::{EventManager, FcExitCode, Vmm};

pub struct ApiServerAdapter {
    api_event_fd: EventFd,
    from_api: Receiver<ApiRequest>,
    to_api: Sender<ApiResponse>,
    controller: RuntimeApiController,
}

impl ApiServerAdapter {
    /// Runs the vmm to completion, while any arising control events are deferred
    /// to a `RuntimeApiController`.
    pub fn run_microvm(
        api_event_fd: EventFd,
        from_api: Receiver<ApiRequest>,
        to_api: Sender<ApiResponse>,
        vm_resources: VmResources,
        vmm: Arc<Mutex<Vmm>>,
        event_manager: &mut EventManager,
    ) -> FcExitCode {
        let api_adapter = Arc::new(Mutex::new(Self {
            api_event_fd,
            from_api,
            to_api,
            controller: RuntimeApiController::new(vm_resources, vmm.clone()),
        }));
        event_manager.add_subscriber(api_adapter);
        loop {
            event_manager
                .run()
                .expect("EventManager events driver fatal error");
            if let Some(exit_code) = vmm.lock().unwrap().shutdown_exit_code() {
                return exit_code;
            }
        }
    }

    fn handle_request(&mut self, req_action: VmmAction) {
        let response = self.controller.handle_request(req_action);
        // Send back the result.
        self.to_api
            .send(Box::new(response))
            .map_err(|_| ())
            .expect("one-shot channel closed");
    }
}
impl MutEventSubscriber for ApiServerAdapter {
    /// Handle a read event (EPOLLIN).
    fn process(&mut self, event: Events, _: &mut EventOps) {
        let source = event.fd();
        let event_set = event.event_set();

        if source == self.api_event_fd.as_raw_fd() && event_set == EventSet::IN {
            match self.from_api.try_recv() {
                Ok(api_request) => {
                    let request_is_pause = *api_request == VmmAction::Pause;
                    self.handle_request(*api_request);

                    // If the latest req is a pause request, temporarily switch to a mode where we
                    // do blocking `recv`s on the `from_api` receiver in a loop, until we get
                    // unpaused. The device emulation is implicitly paused since we do not
                    // relinquish control to the event manager because we're not returning from
                    // `process`.
                    if request_is_pause {
                        // This loop only attempts to process API requests, so things like the
                        // metric flush timerfd handling are frozen as well.
                        loop {
                            let req = self.from_api.recv().expect("Error receiving API request.");
                            let req_is_resume = *req == VmmAction::Resume;
                            self.handle_request(*req);
                            if req_is_resume {
                                break;
                            }
                        }
                    }
                }
                Err(TryRecvError::Empty) => {
                    warn!("Got a spurious notification from api thread");
                }
                Err(TryRecvError::Disconnected) => {
                    panic!("The channel's sending half was disconnected. Cannot receive data.");
                }
            };
            let _ = self.api_event_fd.read();
        } else {
            error!("Spurious EventManager event for handler: ApiServerAdapter");
        }
    }

    fn init(&mut self, ops: &mut EventOps) {
        if let Err(err) = ops.add(Events::new(&self.api_event_fd, EventSet::IN)) {
            error!("Failed to register activate event: {}", err);
        }
    }
}
