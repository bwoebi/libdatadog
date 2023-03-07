use std::{
    collections::HashSet,
    ffi::{self, CString},
};

use crash_handler::CrashHandler;
use ddcommon::cstr;
use nix::libc;

use spawn_worker::utils::{raw_env, CListMutPtr, EnvKey, ExecVec};

use crate::{
    ipc::SidecarTransport,
    ipc_agent, java,
    tracing::{trace_events::SegfaultNotification, TraceContext},
};

type StartMainFn = extern "C" fn(
    main: MainFn,
    argc: ffi::c_int,
    argv: *const *const ffi::c_char,
    init: InitFn,
    fini: FiniFn,
    rtld_fini: FiniFn,
    stack_end: *const ffi::c_void,
);
type MainFn = unsafe extern "C" fn(
    ffi::c_int,
    *const *const ffi::c_char,
    *const *const ffi::c_char,
) -> ffi::c_int;
type InitFn = extern "C" fn(ffi::c_int, *const *const ffi::c_char, *const *const ffi::c_char);
type FiniFn = extern "C" fn();

const ENVKEY_TRACING_ENABLED: EnvKey = EnvKey::from("ENABLE_TRACING");
const ENVKEY_DD_TRACE_AGENT_URL: EnvKey = EnvKey::from("DD_TRACE_AGENT_URL");
const ENVKEY_MINI_AGENT_STARTED: EnvKey = EnvKey::from("_DD_MINI_AGENT_STARTED");

pub fn wrap_result<T, F>(f: F) -> Option<T>
where
    F: FnOnce() -> Result<T, anyhow::Error>,
{
    match f() {
        Ok(res) => Some(res),
        Err(err) => {
            eprintln!("dderror: {:?}", err);
            None
        }
    }
}

static mut HANDLER: Option<CrashHandler> = None;

unsafe fn handle_crash(transport: SidecarTransport, trace_ctx: TraceContext) {
    let handler = crash_handler::CrashHandler::attach(crash_handler::make_crash_event(
        move |cc: &crash_handler::CrashContext| {
            let mut transport = transport.clone();
            transport
                .crash_happened(SegfaultNotification {
                    id: trace_ctx.span_id.clone(),
                    trace_id: trace_ctx.trace_id.clone(),
                })
                .ok();

            crash_handler::CrashEventResult::Handled(false)
        },
    ))
    .ok();
    HANDLER = handler
}

#[allow(dead_code)]
unsafe extern "C" fn new_main(
    mut argc:libc::c_int,
    argv: *const *const libc::c_char,
    _envp: *const *const libc::c_char,
) -> libc::c_int {
    let mut env = raw_env::as_clist();
    let mut argv = CListMutPtr::from_raw_parts(argv as *mut *const libc::c_char);

    // TODO: skip sidecar launching in children - maybe? to speed things up - more testing needed
    // TODO: ld preload also was launched in sidecars... how this did not create a worse race condition I have no idea! :)
    let ld_preload = env
        .remove_entry(EnvKey::from("LD_PRELOAD"))
        .map(|f| f.to_owned());
    let path = match env.get_entry(ENVKEY_MINI_AGENT_STARTED) {
        Some(_) => None,
        None => wrap_result(|| Ok(crate::mini_agent::maybe_start()?)),
    };

    let transport = wrap_result(|| Ok(ipc_agent::maybe_start()?));
    // TODO: resolve this in a nicer way
    let tracing_enabled = env.get_entry(ENVKEY_TRACING_ENABLED).is_some();

    let parent_context = TraceContext::extract_from_c_env(&mut env);
    if path.is_some() {
        env.remove_entry(EnvKey::from("DD_TRACE_AGENT_URL"));
    }
    let mut env: ExecVec<10> = env.into_exec_vec();

    wrap_result(|| {
        if tracing_enabled {
            let context = match parent_context {
                Some(p) => p.to_child(),
                None => TraceContext::default(),
            };

            context.store_in_c_env(&mut env)?;
            if let Some(mut transport) = transport {
                handle_crash(transport.clone(), context.clone());
                transport.span_started(context.span_start(&argv))?;
            }
        }
        Ok(())
    });

    if let Some(path) = path {
        wrap_result(|| {
            env.push_cstring(
                ENVKEY_DD_TRACE_AGENT_URL
                    .build_c_env(format!("unix://{}", path.to_string_lossy()))?,
            );
            env.push_cstring(ENVKEY_MINI_AGENT_STARTED.build_c_env("true")?);

            Ok(())
        });
    }

    if let Some(ld_preload) = ld_preload {
        env.push_cstring(ld_preload);
    }

    let old_environ = raw_env::swap(env.as_ptr());

    let rv = match unsafe { ORIGINAL_MAIN } {
        Some(main) => {
            if java::check_java_rewrite(&mut argv) {
                let mut argv = argv.into_exec_vec::<10>();
                java::rewrite_cmd_args(&mut argv);
                argc = argv.len() as i32;
                main(argc, argv.as_ptr(), env.as_ptr())
            } else {
                main(argc, argv.as_ptr(), env.as_ptr())
            }
        }
        None => 0,
    };

    // setting back before exiting as env will be garbage collected and all of its references will become invalid
    raw_env::swap(old_environ);
    rv
}

/// # Safety
///
/// This method is meant to only be called by the default elf entrypoing once the symbol is replaced by LD_PRELOAD
///
/// avoid allocations or calls to C or Rust functions which might require global
/// initializers to run first. This function is called by elf entry point
/// before any initializers
#[no_mangle]
pub unsafe extern "C" fn __libc_start_main(
    main: MainFn,
    argc: ffi::c_int,
    argv: *const *const ffi::c_char,
    init: InitFn,
    fini: FiniFn,
    rtld_fini: FiniFn,
    stack_end: *const ffi::c_void,
) {
    let libc_start_main =
        spawn_worker::utils::dlsym::<StartMainFn>(libc::RTLD_NEXT, cstr!("__libc_start_main"))
            .unwrap();
    ORIGINAL_MAIN = Some(main);
    #[cfg(not(test))]
    libc_start_main(new_main, argc, argv, init, fini, rtld_fini, stack_end);
    #[cfg(test)]
    libc_start_main(
        unsafe { ORIGINAL_MAIN.unwrap() },
        argc,
        argv,
        init,
        fini,
        rtld_fini,
        stack_end,
    );
}

static mut ORIGINAL_MAIN: Option<MainFn> = None;
