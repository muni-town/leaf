use leaf_module_sdk::*;

register_handlers!(init_db, filter_inbound, filter_outbound, process_event);

fn init_db(creator: String, _params: String) {
    query(
        r#"create table if not exists "state" \
        ("id" integer primary key, "creator" text not null )"#,
        Vec::new(),
    );
    query(
        "insert or ignore into state (id, creator) values (1, :creator);",
        vec![(":creator".into(), creator.into())],
    );
}

fn is_creator(user: &str) -> bool {
    let creator = query("select creator from state where id = 1", Vec::new())
        .parse_rows::<Vec<String>>()
        .unwrap()
        .pop()
        .unwrap();
    user == creator
}

fn filter_inbound(input: IncomingEvent) -> Result<Inbound> {
    if is_creator(&input.user) {
        Ok(Inbound::Allow)
    } else {
        Ok(Inbound::Block {
            reason: "Only events from stream creator are allowed.".into(),
        })
    }
}

fn filter_outbound(input: EventRequest) -> Result<Outbound> {
    if is_creator(&input.requesting_user) {
        Ok(Outbound::Allow)
    } else {
        Ok(Outbound::Block)
    }
}

fn process_event(input: IncomingEvent) -> Result<Process> {
    // If we receive a specialy encoded module update event
    if let Ok(update_event) = ModuleUpdateEvent::decode(&mut &input.payload[..]) {
        // Update the module
        Ok(Process {
            new_module: Some(update_event.new_module),
            new_params: None,
        })
    // Otherwise we have nothing to do
    } else {
        Ok(Process {
            new_module: None,
            new_params: None,
        })
    }
}

#[derive(Decode)]
struct ModuleUpdateEvent {
    _marker: NewModuleUpdateEventMarker,
    new_module: [u8; 32],
}

const NEW_MODULE_UPDATE_EVENT_MARKER: &[u8; 28] = b"__leaf_module_update_event__";
struct NewModuleUpdateEventMarker;
impl Decode for NewModuleUpdateEventMarker {
    fn decode<I: parity_scale_codec::Input>(
        input: &mut I,
    ) -> std::result::Result<Self, parity_scale_codec::Error> {
        let mut prefix_buffer = [0u8; 28];
        input.read(&mut prefix_buffer)?;
        if &prefix_buffer == NEW_MODULE_UPDATE_EVENT_MARKER {
            Ok(NewModuleUpdateEventMarker)
        } else {
            Err("Could not parse admin marker".into())
        }
    }
}
