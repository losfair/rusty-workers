use rusty_v8 as v8;
use rusty_workers::types::*;
use std::sync::{Arc, Mutex};

/// Alias for callback function types.
pub trait Callback =
    Fn(&mut v8::HandleScope, v8::FunctionCallbackArguments, v8::ReturnValue) + Copy + Sized;

/// Shared container for `TerminationReason`.
#[derive(Clone)]
pub struct TerminationReasonBox(pub Arc<Mutex<TerminationReason>>);

/// Reason of execution termination.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TerminationReason {
    /// Terminated for unknown reason.
    Unknown,

    /// Time limit exceeded.
    TimeLimit,

    /// Instance terminated by cache policy.
    CachePolicy,

    /// A fetch response is sent. Not really an error.
    SentFetchResponse,

    Done,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum NonErrorEarlyTermination {
    SentFetchResponse,
    Done,
}

/// Utility trait for converting a `Global` into a `Local`.
///
/// Directly calling `Local::new(scope, do_something_with(scope))` has lifetime issues so added
/// this for convenience.
pub trait IntoLocal {
    type Target;

    /// Converts `self` into a `Local`.
    fn into_local<'s>(self, scope: &mut v8::HandleScope<'s>) -> v8::Local<'s, Self::Target>;
}

impl<T> IntoLocal for v8::Global<T> {
    type Target = T;
    fn into_local<'s>(self, scope: &mut v8::HandleScope<'s>) -> v8::Local<'s, Self::Target> {
        v8::Local::new(scope, self)
    }
}

/// Utility trait for checking the reason of a V8 error.
pub trait CheckedResult {
    type Target;
    fn check<'s>(self) -> GenericResult<Self::Target>;
}

impl<T> CheckedResult for Option<T> {
    type Target = T;
    fn check<'s>(self) -> GenericResult<Self::Target> {
        self.ok_or_else(|| GenericError::Other("v8 returns None".into()))
    }
}

pub trait CheckedTryCatch {
    fn exception_description(&mut self) -> Option<String>;
    fn check_on_init(&mut self) -> GenericResult<()>;
    fn check_on_task(&mut self) -> ExecutionResult<Option<NonErrorEarlyTermination>>;
}

impl<'s, 'p: 's, P> CheckedTryCatch for v8::TryCatch<'s, P>
where
    v8::TryCatch<'s, P>: AsMut<v8::HandleScope<'p, ()>> + AsMut<v8::HandleScope<'p, v8::Context>>,
{
    fn exception_description(&mut self) -> Option<String> {
        let exception = self.exception();
        if let Some(exc) = exception {
            let scope: &mut v8::HandleScope<'p> = self.as_mut();
            let desc = exc.to_rust_string_lossy(scope);
            Some(desc)
        } else {
            None
        }
    }

    fn check_on_init(&mut self) -> GenericResult<()> {
        self.check_on_task()
            .map_err(GenericError::Execution)
            .and_then(|x| {
                if let Some(_) = x {
                    Err(GenericError::Other(
                        "non-error early termination is error during init".into(),
                    ))
                } else {
                    Ok(())
                }
            })
    }

    fn check_on_task(&mut self) -> ExecutionResult<Option<NonErrorEarlyTermination>> {
        match self.exception_description() {
            Some(x) => {
                let e = if self.has_terminated() {
                    match get_termination_reason(self.as_mut()) {
                        TerminationReason::Unknown => {
                            // This should not happen.
                            warn!("check_on_task: termination requested without reason");
                            ExecutionError::RuntimeThrowsException
                        }
                        TerminationReason::TimeLimit => ExecutionError::TimeLimitExceeded,
                        TerminationReason::CachePolicy => ExecutionError::RuntimeThrowsException,
                        TerminationReason::SentFetchResponse => {
                            return Ok(Some(NonErrorEarlyTermination::SentFetchResponse))
                        }
                        TerminationReason::Done => return Ok(Some(NonErrorEarlyTermination::Done)),
                    }
                } else {
                    // Otherwise this is a normal exception thrown from JavaScript.
                    ExecutionError::ScriptThrowsException(x)
                };
                Err(e)
            }
            None => Ok(None),
        }
    }
}

pub fn make_function<'s, C: Callback>(
    scope: &mut v8::HandleScope<'s>,
    native: C,
) -> GenericResult<v8::Local<'s, v8::Function>> {
    Ok(v8::Function::new(scope, native).check()?)
}

pub fn make_string<'s, T: AsRef<str>>(
    scope: &mut v8::HandleScope<'s>,
    s: T,
) -> GenericResult<v8::Local<'s, v8::String>> {
    Ok(v8::String::new(scope, s.as_ref()).check()?)
}

pub fn add_props_to_object<'s, K: AsRef<str>>(
    scope: &mut v8::HandleScope<'s>,
    obj: &v8::Local<'s, v8::Object>,
    elements: impl IntoIterator<Item = (K, v8::Local<'s, v8::Value>)>,
) -> GenericResult<()> {
    for (k, v) in elements.into_iter() {
        let k = v8::String::new(scope, k.as_ref()).check()?;
        obj.set(scope, k.into(), v);
    }
    Ok(())
}

pub fn native_to_js<'s, T: serde::Serialize>(
    scope: &mut v8::HandleScope<'s>,
    v: &T,
) -> GenericResult<v8::Local<'s, v8::Value>> {
    let json_text = v8::String::new(
        scope,
        serde_json::to_string(v)
            .map_err(|_| GenericError::Conversion)?
            .as_str(),
    )
    .check()?;
    let js_value = v8::json::parse(scope, json_text.into()).check()?;
    Ok(js_value)
}

fn get_termination_reason(isolate: &mut v8::Isolate) -> TerminationReason {
    *isolate
        .get_slot_mut::<Option<TerminationReasonBox>>()
        .unwrap()
        .as_ref()
        .unwrap()
        .0
        .lock()
        .unwrap()
}

pub fn set_termination_reason(isolate: &mut v8::Isolate, reason: TerminationReason) {
    *isolate
        .get_slot_mut::<Option<TerminationReasonBox>>()
        .unwrap()
        .as_ref()
        .unwrap()
        .0
        .lock()
        .unwrap() = reason;
}
