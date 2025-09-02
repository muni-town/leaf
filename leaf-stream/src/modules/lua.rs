use std::pin::Pin;

use blake3::Hash;
use lua::*;
use piccolo as lua;

use crate::{InboundFilterResponse, LeafModule};

pub struct LuaModule {
    id: Hash,
    lua: Lua,
    filter_inbound: StashedClosure,
    filter_outbound: StashedClosure,
    process_event: StashedClosure,
}

#[derive(Debug, thiserror::Error)]
pub enum LuaModuleError {
    #[error("Error handling lua script: {0}")]
    LuaError(#[from] piccolo::ExternError),
}

impl LuaModule {
    pub fn new(
        filter_inbound_lua: &[u8],
        filter_outbound_lua: &[u8],
        process_event_lua: &[u8],
    ) -> Result<Self, LuaModuleError> {
        let mut lua = Lua::core();

        let mut hasher = blake3::Hasher::new();
        hasher.update(filter_inbound_lua);
        hasher.update(filter_outbound_lua);
        hasher.update(process_event_lua);
        let id = hasher.finalize();

        let (filter_inbound, filter_outbound, process_event) = lua.try_enter(|mc| {
            // Clean up the ENV to just make a best effort to prevent the scripts from persisting
            // state through the _ENV across runs.
            let globals = mc.globals();
            let metatable = Table::new(&mc);
            metatable.set(
                mc,
                "__newindex",
                Callback::from_fn(&mc, move |_mc, _fuel, _stack| Ok(CallbackReturn::Return)),
            );
            metatable.set(
                mc,
                "__metatable",
                Value::String(lua::String::from_slice(&mc, "locked")),
            );
            globals.set_metatable(&mc, Some(metatable));
            globals.set(mc, "rawset", Value::Nil)?;

            // Compile the lua scripts
            let filter_inbound = Closure::load(mc, None, filter_inbound_lua)?;
            let filter_outbound = Closure::load(mc, None, filter_outbound_lua)?;
            let process_event = Closure::load(mc, None, process_event_lua)?;
            Ok((
                mc.stash(filter_inbound),
                mc.stash(filter_outbound),
                mc.stash(process_event),
            ))
        })?;

        Ok(Self {
            id,
            lua,
            filter_inbound,
            filter_outbound,
            process_event,
        })
    }
}

impl LeafModule for LuaModule {
    fn id(&self) -> blake3::Hash {
        self.id
    }

    fn process_event(
        &mut self,
        payload: &[u8],
        params: &[u8],
        user: &str,
        db: &libsql::Connection,
    ) -> std::pin::Pin<Box<dyn Future<Output = anyhow::Result<crate::ModuleUpdate>>>> {
        // let ex = self.lua.enter(|mc| {
        //     Executor::start(mc, self.pro, args)
        // });
        todo!();
    }

    fn filter_inbound(
        &mut self,
        payload: &[u8],
        params: &[u8],
        user: &str,
        db: &libsql::Connection,
    ) -> std::pin::Pin<Box<dyn Future<Output = anyhow::Result<crate::InboundFilterResponse>>>> {
        let ex = self.lua.enter(|mc| {
            let closure = mc.fetch(&self.filter_inbound);
            mc.stash(Executor::start(mc, closure.into(), ()))
        });
        let result = self.lua.execute::<bool>(&ex);
        Pin::new(Box::new(async move {
            let b = false;
            Ok(if b {
                InboundFilterResponse::Accept
            } else {
                InboundFilterResponse::Reject {
                    reason: "rejected".into(),
                }
            })
        }))
    }

    fn filter_outbound(
        &mut self,
        params: &[u8],
        user: &str,
        db: &libsql::Connection,
    ) -> std::pin::Pin<Box<dyn Future<Output = anyhow::Result<bool>>>> {
        todo!()
    }
}
