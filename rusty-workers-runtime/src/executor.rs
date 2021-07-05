use crate::buffer::*;
use crate::engine::*;
use crate::error::*;
use crate::interface::*;
use crate::io::*;
use crate::isolate::{IsolateGeneration, IsolateGenerationBox, MemoryPoolBox, Poison};
use crate::mm::*;
use crate::runtime::{InstanceStatistics, Runtime};
use maplit::btreemap;
use rand::Rng;
use rusty_v8 as v8;
use rusty_workers::types::*;
use std::cell::{Cell, UnsafeCell};
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

const MAX_RESPONSE_BODY_SIZE: usize = 8 * 1024 * 1024;

pub struct Instance {
    state: Option<InstanceState>,
}

#[derive(Copy, Clone, Debug)]
pub enum TimerControl {
    Start,
    Stop,
    ResetTo(Duration),
}

struct InstanceState {
    rt: tokio::runtime::Handle,
    worker_runtime: Arc<Runtime>,
    task_rx: mpsc::Receiver<Task>,

    /// Unpacked files in the worker bundle.
    files: BTreeMap<String, Arc<[u8]>>,

    script: Arc<[u8]>,

    timer_tx: tokio::sync::mpsc::UnboundedSender<TimerControl>,
    conf: Arc<WorkerConfiguration>,
    handle: WorkerHandle,
    io_waiter: Option<IoWaiter>,

    fetch_response_channel: Option<tokio::sync::oneshot::Sender<ExecutionResult<ResponseObject>>>,

    appid: String,
    detach_tx: Option<DetachResponse>,
    detach_rx: Arc<Mutex<Option<DetachRequest>>>,
}

pub struct InstanceHandle {
    isolate_handle: v8::IsolateHandle,
    task_tx: mpsc::Sender<Task>,
    termination_reason: TerminationReasonBox,
    creation_generation: IsolateGeneration,
    current_generation: IsolateGenerationBox,
    detach_rx: Arc<Mutex<Option<DetachRequest>>>,
}

struct DetachRequest {
    ack: tokio::sync::mpsc::Sender<()>,
    done: tokio::sync::oneshot::Receiver<()>,
    timeout_ms: u64,
}

struct DetachResponse {
    ack: tokio::sync::mpsc::Receiver<()>,
    _done: tokio::sync::oneshot::Sender<()>,
    cpu_timeout_ms: u64,
}

pub struct InstanceTimeControl {
    pub budget: Duration,
    pub timer_rx: mpsc::UnboundedReceiver<TimerControl>,
}

enum Task {
    Fetch(
        RequestObject,
        tokio::sync::oneshot::Sender<ExecutionResult<ResponseObject>>,
        IoScopeConsumer,
    ),
}

impl Task {
    fn make_event(&self) -> ServiceEvent {
        match self {
            Task::Fetch(ref req, _, _) => ServiceEvent::Fetch(FetchEvent {
                request: req.clone(),
            }),
        }
    }
}

impl InstanceHandle {
    /// Properly check generation and perform remote termination.
    fn do_remote_termination(&self, reason: Option<TerminationReason>) {
        // Take the lock.
        let current_generation = self.current_generation.0.lock().unwrap();

        // Check while holding the lock.
        if *current_generation == self.creation_generation {
            if let Some(reason) = reason {
                // We are holding two locks now - be careful not to deadlock.
                let mut reason_place = self.termination_reason.0.lock().unwrap();

                // Only apply the first termination reason
                if *reason_place == TerminationReason::Unknown {
                    *reason_place = reason;
                }
            }
            self.isolate_handle.terminate_execution();
        }
    }

    pub async fn terminate_for_time_limit(&self) {
        tokio::task::block_in_place(|| {
            self.do_remote_termination(Some(TerminationReason::TimeLimit));
        });
    }

    pub async fn fetch(self: Arc<Self>, req: RequestObject) -> ExecutionResult<ResponseObject> {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let (_io_scope, io_scope_consumer) = IoScope::new();

        // Send fails if the instance has terminated
        self.task_tx
            .send(Task::Fetch(req, result_tx, io_scope_consumer))
            .await
            .map_err(|_| ExecutionError::NoSuchWorker)?;

        // Channel to notify that the fetch task is cancelled.
        let (_kill_tx, kill_rx): (
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        ) = tokio::sync::oneshot::channel();

        // Prepare for detach.
        //
        // Continue to run the instance if the request is cancelled. However if the instance is dropped,
        // don't do that anymore. Keep resource limit enforced.
        tokio::spawn(async move {
            // Grab the IO scope to keep it alive.
            let _io_scope = _io_scope;

            // Don't hold strong reference to "self".
            let this = Arc::downgrade(&self);
            drop(self);

            // Wait for "normal" execution.
            drop(kill_rx.await);

            // Check whether we are allowed to continue execution.
            if let Some(this) = this.upgrade() {
                let request = this.detach_rx.lock().unwrap().take();
                if let Some(request) = request {
                    if request.ack.try_send(()).is_ok() {
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_millis(request.timeout_ms)) => {
                                debug!("detached task: timeout");
                            }
                            _ = request.done => {
                                debug!("detached task: done");
                            }
                        }
                    }
                }
            }
        });

        // This errors if the instance terminates without sending a response
        match result_rx.await {
            Ok(res) => res,
            Err(_) => {
                // Instance dropped sender without sending a response.
                // Most probably a runtime error.
                Err(ExecutionError::RuntimeThrowsException)
            }
        }
    }
}

impl Drop for InstanceHandle {
    fn drop(&mut self) {
        let term = || {
            // If InstanceHandle is implicitly dropped then we assume it's removed by the cache policy.
            self.do_remote_termination(Some(TerminationReason::CachePolicy));
        };

        // If we are in a Tokio context, notify the runtime that we may block.
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(term);
        } else {
            term();
        }
    }
}

impl Instance {
    pub fn new(
        isolate: &mut v8::Isolate,
        rt: tokio::runtime::Handle,
        worker_runtime: Arc<Runtime>,
        worker_handle: WorkerHandle,
        appid: String,
        bundle: Vec<u8>,
        conf: &WorkerConfiguration,
    ) -> GenericResult<(Self, InstanceHandle, InstanceTimeControl)> {
        // Unpack the bundle.
        let bundle_size = bundle.len();
        let mut archive = tar::Archive::new(std::io::Cursor::new(bundle));
        let mut files: BTreeMap<String, Arc<[u8]>> = BTreeMap::new();
        for entry in archive
            .entries()
            .map_err(|_| GenericError::Other("bad bundle".into()))?
        {
            let mut entry = entry.map_err(|_| GenericError::Other("bad entry in bundle".into()))?;
            let path = entry
                .path()
                .ok()
                .and_then(|x| x.to_str().map(|x| x.to_string()))
                .ok_or_else(|| GenericError::Other("bad path in bundle".into()))?;
            // TODO: Is `size` validated? Now we are validating it again to prevent memory DoS
            if entry.size() > bundle_size as u64 {
                return Err(GenericError::Other("entry size > bundle size".into()));
            }
            let mut data = vec![0; entry.size() as usize].into_boxed_slice();
            entry
                .read_exact(&mut data)
                .map_err(|_| GenericError::Other("cannot read bundle".into()))?;
            files.insert(path, Arc::from(data));
        }
        drop(archive);

        // Lookup the script.
        let script = files
            .get("./index.js")
            .or_else(|| files.get("index.js"))
            .ok_or_else(|| GenericError::Other("cannot find index.js in bundle".into()))?
            .clone();

        let termination_reason =
            TerminationReasonBox(Arc::new(Mutex::new(TerminationReason::Unknown)));

        // Isolate initialization set. Needs cleanup.
        isolate.set_microtasks_policy(v8::MicrotasksPolicy::Auto);
        isolate.set_promise_reject_callback(on_promise_rejection);
        isolate.set_slot(Some(termination_reason.clone()));
        isolate.set_oom_error_handler(oom_protected_callback);

        // Reset memory pool.
        isolate
            .get_slot::<MemoryPoolBox>()
            .unwrap()
            .0
            .reset((conf.executor.max_ab_memory_mb as usize) * 1048576);

        // Allocate a channel of size 1. We don't want to put back pressure here.
        // The (async) sending side would block.
        let (task_tx, task_rx) = mpsc::channel(1);

        // TODO: unbounded ok here?
        let (timer_tx, timer_rx) = mpsc::unbounded_channel();

        let time_control = InstanceTimeControl {
            timer_rx,
            budget: Duration::from_millis(conf.executor.max_time_ms as u64),
        };

        let isolate_handle = isolate.thread_safe_handle();
        let generation = isolate.get_slot::<IsolateGenerationBox>().unwrap();
        let detach_rx = Arc::new(Mutex::new(None));

        let handle = InstanceHandle {
            isolate_handle,
            task_tx,
            termination_reason,
            creation_generation: *generation.0.lock().unwrap(),
            current_generation: generation.clone(),
            detach_rx: detach_rx.clone(),
        };
        let instance = Instance {
            state: Some(InstanceState {
                rt,
                worker_runtime,
                task_rx,
                files,
                script,
                timer_tx,
                conf: Arc::new(conf.clone()),
                handle: worker_handle,
                io_waiter: None,
                fetch_response_channel: None,
                appid,
                detach_tx: None,
                detach_rx,
            }),
        };
        Ok((instance, handle, time_control))
    }

    pub fn cleanup(isolate: &mut v8::Isolate) -> GenericResult<()> {
        // TODO: Better way of wrapping `AsMut`?
        struct Wrapper<'a>(&'a mut v8::Isolate);
        impl<'a> AsMut<v8::Isolate> for Wrapper<'a> {
            fn as_mut(&mut self) -> &mut v8::Isolate {
                self.0
            }
        }

        if let Some(state) = InstanceState::try_get(isolate) {
            // Drop `io_waiter` and any `Global` references it holds.
            state.io_waiter = None;

            // `protected_js` expects `InstanceState` to be present
            // FIXME: If compilation failed there may be some references left on the heap.
            protected_js(&mut Wrapper(isolate), |isolate| {
                isolate.0.low_memory_notification();
            })?;
        }

        // Prepare for isolate reuse. Cleanup state.
        isolate.set_slot(Option::<TerminationReasonBox>::None);
        isolate.set_slot(Option::<InstanceState>::None);
        Ok(())
    }

    fn compile<'s>(
        scope: &mut v8::HandleScope<'s>,
        script: &str,
    ) -> GenericResult<v8::Local<'s, v8::Script>> {
        let script = v8::String::new(scope, script)
            .ok_or_else(|| GenericError::ScriptInitException("script compilation failed".into()))?;
        let script = v8::Script::compile(scope, script, None)
            .ok_or_else(|| GenericError::ScriptInitException("script compilation failed".into()))?;
        Ok(script)
    }

    pub fn run(
        &mut self,
        context_scope: &mut v8::ContextScope<'_, v8::HandleScope<'_>>,
        ready_callback: impl FnOnce(),
    ) -> GenericResult<()> {
        let state = self.state.take().unwrap();
        let worker_runtime = state.worker_runtime.clone();

        let worker_handle = state.handle.clone();

        // Take an execution thread.
        let mut permit = worker_runtime.acquire_execution_token()?;

        // Take a HandleScope and initialize the environment.
        {
            let scope = &mut v8::HandleScope::new(context_scope);
            let try_catch = &mut v8::TryCatch::new(scope);
            let scope: &mut v8::HandleScope<'_> = try_catch.as_mut();
            state.init_global_env(scope)?;

            // TODO: Compiler bombs?
            let script = std::str::from_utf8(&state.script).map_err(|_| {
                GenericError::ScriptInitException("cannot decode script as utf-8 text".into())
            })?;
            let script = Self::compile(scope, script)?;

            // Notify that we are ready so that timing etc. can start
            ready_callback();

            scope.set_slot(Some(state));
            try_catch.check_on_init()?;

            // Now start the timer, since we are starting to run user code.
            InstanceState::get(try_catch).start_timer();

            protected_js(try_catch.as_mut(), |scope| {
                script.run(scope);
            })?;
            try_catch.check_on_init()?;
        }
        info!("worker instance {} ready", worker_handle.id);

        // Wait for tasks.
        loop {
            update_stats(&worker_runtime, &worker_handle, context_scope);

            let scope = &mut v8::HandleScope::new(context_scope);
            let try_catch = &mut v8::TryCatch::new(scope);
            let scope: &mut v8::HandleScope<'_> = try_catch.as_mut();
            let state = InstanceState::get(scope);
            state.stop_timer();

            let timer_timeout = Duration::from_millis(state.conf.executor.max_time_ms as u64);
            state.reset_timer_to(timer_timeout);

            // Cleanup state
            state.io_waiter = None; // drop it
            state.detach_rx.lock().unwrap().take();
            state.detach_tx.take();

            drop(permit);

            // Try to receive a new task. If receive fails it means that the fetch() task is cancelled and we
            // should stop.
            let task = match state.task_rx.blocking_recv() {
                Some(x) => x,
                None => {
                    // channel closed
                    break;
                }
            };
            permit = worker_runtime.acquire_execution_token()?;
            let event = task.make_event();
            let io_scope = state.populate_with_task(task)?;
            state.start_timer();

            // Start I/O processor (per-request).
            //
            // An `IoProcessor` receives the task's `IoScopeConsumer` as its argument, and stops when the
            // corresponding `IoScope` is dropped.
            let (io_waiter, io_processor) =
                IoWaiter::new(state.conf.clone(), state.worker_runtime.clone());
            state.rt.spawn(io_processor.run(io_scope));
            state.io_waiter = Some(io_waiter);

            let global = scope.get_current_context().global(scope);
            let callback_key = make_string(scope, "_dispatchEvent")?;
            let callback = global.get(scope, callback_key.into()).check()?;
            let callback = v8::Local::<'_, v8::Function>::try_from(callback)
                .map_err(|_| GenericError::Other("bad _dispatchEvent".into()))?;
            let recv = v8::undefined(scope);
            let event_js = native_to_js(scope, &event)?;

            protected_js(scope, |scope| {
                callback.call(scope, recv.into(), &[event_js]);
            })?;

            // Drive to completion.
            loop {
                // Waiting for I/O now. Stop the timer.
                InstanceState::get(try_catch).stop_timer();

                match try_catch.check_on_task() {
                    Ok(Some(reason)) => {
                        try_catch.reset();
                        match reason {
                            NonErrorEarlyTermination::SentFetchResponse => {
                                let state = InstanceState::get(try_catch);
                                if let Some(ref mut detach_tx) = state.detach_tx {
                                    // Wait for ack.
                                    // We may block for some time so let's drop the CPU permit.
                                    drop(permit);
                                    let ok = detach_tx.ack.blocking_recv().is_some();
                                    permit = worker_runtime.acquire_execution_token()?;
                                    if ok {
                                        let cpu_timeout_ms = detach_tx.cpu_timeout_ms;
                                        state.reset_timer_to(Duration::from_millis(cpu_timeout_ms));
                                        debug!("detached execution started");
                                    } else {
                                        debug!("failed to get ack from monitor");
                                        break;
                                    }
                                }
                            }
                            NonErrorEarlyTermination::Done => {
                                debug!("execution completion requested");
                                break;
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        if e.terminates_worker() {
                            InstanceState::try_send_fetch_response(try_catch, Err(e.clone()));
                            return Err(GenericError::Execution(e));
                        } else {
                            debug!("non-critical exception: {:?}", e);
                            try_catch.reset();
                            InstanceState::try_send_fetch_response(try_catch, Err(e));
                            break;
                        }
                    }
                }

                let scope = &mut v8::HandleScope::new(try_catch);

                // A nice point to update statistics!
                update_stats(&worker_runtime, &worker_handle, scope);

                // We are not using CPU now so drop CPU permit
                drop(permit);

                // Take the IO waiter (lifetime conflict with `scope`)
                let mut io_waiter = InstanceState::get(scope).io_waiter.take().unwrap();
                let wait_result = io_waiter.wait(scope);
                InstanceState::get(scope).io_waiter = Some(io_waiter);

                permit = worker_runtime.acquire_execution_token()?;
                let (callback, data, buffers) = match wait_result {
                    Some(x) => x,
                    None => {
                        // IO timeout.
                        break;
                    }
                };

                InstanceState::get(scope).start_timer();

                let callback = v8::Local::<'_, v8::Function>::new(scope, callback);

                // Don't deserialize onto V8 heap here to ensure OOM safety
                let data = data.as_bytes();
                acquire_arraybuffer_precheck(scope, data.len())?;
                let json_data = v8::ArrayBuffer::new(scope, data.len());
                let json_data_backing = json_data.get_backing_store();
                json_data_backing
                    .iter()
                    .enumerate()
                    .for_each(|(i, x)| x.set(data[i]));

                let local_buffers: Vec<v8::Local::<'_, v8::Value>> = buffers.into_iter().map(|x| x.unwrap_on_v8_thread())
                    .map(|x| v8::Local::new(scope, x.expect("we are on v8 thread but unwrap_on_v8_thread failed to upgrade reference")).into())
                    .collect();
                let target_buffers = v8::Array::new_with_elements(scope, &local_buffers);

                protected_js(scope, |scope| {
                    callback.call(
                        scope,
                        recv.into(),
                        &[json_data.into(), target_buffers.into()],
                    );
                })?;
            }
        }
        Ok(())
    }
}

impl InstanceState {
    fn get(isolate: &mut v8::Isolate) -> &mut Self {
        Self::try_get(isolate).expect("InstanceState::get: no current state")
    }

    fn try_get(isolate: &mut v8::Isolate) -> Option<&mut Self> {
        isolate
            .get_slot_mut::<Option<Self>>()
            .and_then(|x| x.as_mut())
    }

    fn io_waiter(&mut self) -> JsResult<&mut IoWaiter> {
        self.io_waiter.as_mut().ok_or_else(|| {
            JsError::new(JsErrorKind::Error, Some("io service not available".into()))
        })
    }

    fn start_timer(&self) {
        drop(self.timer_tx.send(TimerControl::Start));
    }

    fn stop_timer(&self) {
        drop(self.timer_tx.send(TimerControl::Stop));
    }

    fn reset_timer_to(&self, timeout: Duration) {
        drop(self.timer_tx.send(TimerControl::ResetTo(timeout)));
    }

    /// Builds the global object.
    fn init_global_env<'s>(&self, scope: &mut v8::HandleScope<'s>) -> GenericResult<()> {
        let global = scope.get_current_context().global(scope);
        let global_props = btreemap! {
            "_rt_callService" => make_function(scope, call_service_callback)?.into(),
            "queueMicrotask" => make_function(scope, queue_microtask_callback)?.into(),
        };

        // Make sure our internal objects aren't overwritten by adding user props first.
        let user_props: Result<Vec<_>, GenericError> = self
            .conf
            .env
            .iter()
            .map(|(k, v)| Ok((k, make_string(scope, v)?.into())))
            .collect();
        add_props_to_object(scope, &global, user_props?)?;

        add_props_to_object(scope, &global, global_props)?;
        Ok(())
    }

    fn populate_with_task(&mut self, task: Task) -> GenericResult<IoScopeConsumer> {
        match task {
            Task::Fetch(_, res, io_scope) => {
                self.fetch_response_channel = Some(res);
                Ok(io_scope)
            }
        }
    }

    fn try_send_fetch_response(
        isolate: &mut v8::Isolate,
        res: ExecutionResult<ResponseObject>,
    ) -> bool {
        if let Some(ch) = InstanceState::get(isolate).fetch_response_channel.take() {
            ch.send(res).is_ok()
        } else {
            false
        }
    }

    fn check_host_entry_memory(isolate: &mut v8::Isolate) -> GenericResult<()> {
        let mut stats = v8::HeapStatistics::default();
        isolate.get_heap_statistics(&mut stats);
        let isolate_config = InstanceState::get(isolate).worker_runtime.isolate_config();

        // Exclude arraybuffer size here since we have separate checks when constructing arraybuffers
        let total_heap_size = stats.total_heap_size();
        if total_heap_size > isolate_config.max_memory_bytes
            || isolate_config.max_memory_bytes - total_heap_size
                < isolate_config.host_entry_threshold_memory_bytes
        {
            return Err(GenericError::Execution(ExecutionError::MemoryLimitExceeded));
        }

        Ok(())
    }
}

fn update_stats(worker_runtime: &Runtime, worker_handle: &WorkerHandle, scope: &mut v8::Isolate) {
    let mut stats = v8::HeapStatistics::default();
    scope.get_heap_statistics(&mut stats);
    worker_runtime.update_stats(
        worker_handle,
        InstanceStatistics {
            used_memory_bytes: stats.total_heap_size() + stats.external_memory(), // heap + arraybuffers
        },
    );
}

extern "C" fn on_promise_rejection(_msg: v8::PromiseRejectMessage<'_>) {
    debug!("unhandled promise rejection");
}

fn protected_js<F: FnOnce(&mut T), T: AsMut<v8::Isolate>>(
    scope: &mut T,
    f: F,
) -> GenericResult<()> {
    if _do_protected_call(|| f(scope)).is_none() {
        scope.as_mut().set_slot::<Poison>(Poison);
        Err(GenericError::Execution(ExecutionError::MemoryLimitExceeded))
    } else {
        InstanceState::check_host_entry_memory(scope.as_mut())?;
        Ok(())
    }
}

thread_local! {
    // TODO: Get the defs from libc.
    static JMP_ENV: UnsafeCell<Option<[usize; 32]>> = UnsafeCell::new(None);
}

extern "C" {
    #[ffi_returns_twice]
    fn setjmp(env: &mut [usize; 32]) -> i32;
    fn longjmp(env: &[usize; 32], data: i32) -> !;
}

extern "C" fn oom_protected_callback(_: *const std::os::raw::c_char, _: bool) {
    unsafe {
        if let Some(ref mut buf) = *JMP_ENV.with(|x| x.get()) {
            longjmp(buf, 1);
        } else {
            panic!("oom_protected_callback: unprotected OOM");
        }
    }
}

fn _do_protected_call<F: FnOnce() -> R, R>(f: F) -> Option<R> {
    unsafe {
        JMP_ENV.with(|x| {
            let inner = &mut *x.get();
            if inner.is_some() {
                panic!("_do_protected_call: re-entering");
            }
            *inner = Some([0; 32]);

            let ret = if setjmp(inner.as_mut().unwrap()) == 0 {
                Some(f())
            } else {
                None
            };

            *x.get() = None;
            ret
        })
    }
}

fn call_service_callback(
    scope: &mut v8::HandleScope,
    args: v8::FunctionCallbackArguments,
    mut retval: v8::ReturnValue,
) {
    wrap_callback(scope, |scope| {
        let call_str = v8::Local::<'_, v8::String>::try_from(args.get(0))?;
        let call_buf_len = call_str.utf8_length(scope);

        // Large buffers are passed independently so we can enforce a small call_buf_len here.
        if call_buf_len > 65536 {
            return Err(JsError::new(
                JsErrorKind::Error,
                Some("call string too large".into()),
            ));
        }

        // Decode string to UTF-8 arraybuffer first.
        // Do not use native heap (`Vec<u8>`) to enforce memory limit.
        acquire_arraybuffer_precheck(scope, call_buf_len)?;
        let call_buf = v8::ArrayBuffer::new(scope, call_buf_len);
        write_utf8_to_arraybuffer(scope, call_str, call_buf, None);

        let call_backing = call_buf.get_backing_store();
        let call_reader = ReadableByteCellSlice::new(&call_backing);

        // Now decode to the ServiceCall type.
        let call: ServiceCall =
            serde_json::from_reader(call_reader).map_err(|_| GenericError::Conversion)?;
        let buffers = v8::Local::<'_, v8::Array>::try_from(args.get(1))?;
        let buffers_count = buffers.length();

        // FIXME: Hardcoded limit here. Should we change this?
        if buffers_count > 256 {
            return Err(JsError::new(
                JsErrorKind::Error,
                Some("too many buffers".into()),
            ));
        }

        // Collect buffers.
        let mut local_buffers: Vec<JsArrayBufferViewRef> = vec![];
        for i in 0..buffers_count {
            let value = buffers
                .get_index(scope, i)
                .ok_or(JsError::new(JsErrorKind::Error, None))?;
            local_buffers.push(JsArrayBufferViewRef::new(scope, value)?);
        }

        match call {
            ServiceCall::Sync(call) => {
                match call {
                    SyncCall::Log(s) => {
                        debug!("log: {}", s);
                        let state = InstanceState::get(scope);
                        state.worker_runtime.write_log(state.appid.clone(), s);
                    }
                    SyncCall::Done => {
                        debug!("done() called");
                        set_termination_reason(scope, TerminationReason::Done);
                        scope.terminate_execution();
                    }
                    SyncCall::SendFetchResponse(mut res) => {
                        let body = local_buffers
                            .get(0)
                            .ok_or_else(|| {
                                JsError::new(
                                    JsErrorKind::Error,
                                    Some("SendFetchResponse: missing buffer".into()),
                                )
                            })?
                            .read_to_vec(MAX_RESPONSE_BODY_SIZE)
                            .ok_or_else(|| {
                                JsError::new(
                                    JsErrorKind::Error,
                                    Some("SendFetchResponse: body too large".into()),
                                )
                            })?;
                        res.body = HttpBody::Binary(body);
                        if InstanceState::try_send_fetch_response(scope, Ok(res)) {
                            // Terminate it.
                            set_termination_reason(scope, TerminationReason::SentFetchResponse);
                            scope.terminate_execution();
                        } else {
                            return Err(JsError::new(
                                JsErrorKind::Error,
                                Some("SendFetchResponse: already sent".into()),
                            ));
                        }
                    }
                    SyncCall::GetRandomValues => {
                        let output: &[Cell<u8>] = local_buffers.get(0).ok_or_else(|| {
                            JsError::new(
                                JsErrorKind::Error,
                                Some("GetRandomValues: missing buffer".into()),
                            )
                        })?;
                        if output.len() > 65536 {
                            return Err(JsError::new(JsErrorKind::Error, Some("SyncCall::GetRandomValues invoked with length greater than 65536".into())));
                        }
                        let mut rng = rand::thread_rng();
                        for byte in output.iter() {
                            byte.set(rng.gen());
                        }
                    }
                    SyncCall::GetFile(name) => {
                        let state = InstanceState::get(scope);
                        if let Some(file) = state.files.get(&name) {
                            let file = file.clone();
                            let buf = slice_to_arraybuffer(scope, &file)?;
                            retval.set(buf.into());
                        } else {
                            retval.set(v8::null(scope).into());
                        }
                    }
                    SyncCall::Crypto(inner) => {
                        if let Some(x) = inner.run(scope, local_buffers)? {
                            retval.set(x);
                        }
                    }
                    SyncCall::Detach(opts) => {
                        let state = InstanceState::get(scope);

                        if state.detach_tx.is_some() {
                            return Err(JsError::new(
                                JsErrorKind::Error,
                                Some("Detach: already requested".into()),
                            ));
                        }

                        if opts.cpu_timeout_ms > state.conf.executor.max_detached_cpu_time_ms as u64
                        {
                            return Err(JsError::new(
                                JsErrorKind::Error,
                                Some("Detach: cpu time exceeds limit".into()),
                            ));
                        }

                        if opts.timeout_ms > state.conf.executor.max_detached_wall_time_ms as u64 {
                            return Err(JsError::new(
                                JsErrorKind::Error,
                                Some("Detach: wall time exceeds limit".into()),
                            ));
                        }

                        let (ack_tx, ack_rx) = tokio::sync::mpsc::channel(1);
                        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
                        let (req, res) = (
                            DetachRequest {
                                ack: ack_tx,
                                done: done_rx,
                                timeout_ms: opts.timeout_ms,
                            },
                            DetachResponse {
                                ack: ack_rx,
                                _done: done_tx,
                                cpu_timeout_ms: opts.cpu_timeout_ms,
                            },
                        );

                        state.detach_tx = Some(res);
                        *state.detach_rx.lock().unwrap() = Some(req);
                    }
                }
            }
            ServiceCall::Async(call) => {
                let callback = v8::Local::<'_, v8::Function>::try_from(args.get(2))?;
                let callback = v8::Global::new(scope, callback);
                let state = InstanceState::get(scope);
                state.io_waiter()?.issue(
                    false,
                    AsyncCall {
                        v: call,
                        buffers: local_buffers,
                    },
                    callback,
                )?;
            }
        }
        Ok(())
    })
}

fn queue_microtask_callback(
    scope: &mut v8::HandleScope,
    args: v8::FunctionCallbackArguments,
    _retval: v8::ReturnValue,
) {
    wrap_callback(scope, |scope| {
        let cb = v8::Local::<'_, v8::Function>::try_from(args.get(0))?;
        scope.enqueue_microtask(cb);
        Ok(())
    })
}

fn write_utf8_to_arraybuffer(
    scope: &mut v8::HandleScope<'_>,
    src: v8::Local<'_, v8::String>,
    buf: v8::Local<'_, v8::ArrayBuffer>,
    num_utf16_read_out: Option<&mut usize>,
) -> usize {
    let backing = buf.get_backing_store();
    let backing: &[Cell<u8>] = &backing;
    let backing = unsafe {
        std::mem::transmute::<*const [Cell<u8>], &mut [u8]>(backing as *const [Cell<u8>])
    };
    src.write_utf8(scope, backing, num_utf16_read_out, Default::default())
}

/// Converts a `Result` from a callback function into a JavaScript exception.
fn wrap_callback<'s, F: FnOnce(&mut v8::HandleScope<'s>) -> JsResult<()>>(
    scope: &mut v8::HandleScope<'s>,
    f: F,
) {
    // Check we have enough heap memory to safely enter host environment.
    match InstanceState::check_host_entry_memory(scope) {
        Ok(()) => {}
        Err(e) => {
            // Release any data allocated on native heap.
            drop(e);
            drop(f);

            // OOM can still happen here and longjmp's are allowed.
            // So don't allocate on native heap.
            let message = v8::String::new(scope, "no enough memory when calling into host")
                .unwrap_or_else(|| v8::String::empty(scope));
            let exception = v8::Exception::range_error(scope, message);
            scope.throw_exception(exception);
            return;
        }
    }

    // Now we know we are safe, let's enter the actual callback.
    // An assertion that we cannot `longjmp` out in case we OOM'd in a callback.
    let jmp_env = unsafe {
        (*JMP_ENV.with(|x| x.get()))
            .take()
            .expect("wrap_callback: not in protected call")
    };

    match f(scope) {
        Ok(()) => {}
        Err(e) => {
            debug!("Throwing JS exception: {:?}", e);
            let exception = e.build(scope);
            scope.throw_exception(exception);
        }
    }
    unsafe {
        (*JMP_ENV.with(|x| x.get())) = Some(jmp_env);
    }
}
