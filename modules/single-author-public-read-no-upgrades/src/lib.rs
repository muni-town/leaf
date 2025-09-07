use leaf_module_sdk::*;

register_handlers!(init_db, filter_inbound, filter_outbound, process_event);

fn init_db(creator: String, _params: String) {
    query(
        r#"
        create table if not exists "state" (
            "id"    integer primary key,
            "creator"  text not null
        )
        "#,
        Vec::new(),
    );
    query(
        "insert or ignore into state (id, creator) values (1, :creator);",
        vec![(":creator".into(), creator.into())],
    );
}

fn filter_inbound(input: IncomingEvent<String, String>) -> Result<Inbound> {
    if serde_json::from_str::<serde_json::Value>(&input.payload).is_err() {
        return Ok(Inbound::Block {
            reason: "Message is not valid JSON".into(),
        });
    }
    let creator = query("select creator from state where id = 1", Vec::new())
        .parse_rows::<Vec<String>>()
        .unwrap()
        .pop()
        .unwrap();
    if input.user != creator {
        return Ok(Inbound::Block {
            reason: format!(
                "Got event from user {}. Only events from {} are allowed.",
                input.user, creator
            ),
        });
    }

    Ok(Inbound::Allow)
}

// Everything is public, but an optional search filter can be applied.
fn filter_outbound(input: EventRequest<String, String, String>) -> Result<Outbound> {
    // Reject any records that don't include the filter, if present.
    if let Some(filter) = input.filter
        && !input.incoming_event.payload.contains(&filter)
    {
        return Ok(Outbound::Block);
    }

    Ok(Outbound::Allow)
}

fn process_event(_input: IncomingEvent<String, String>) -> Result<Process> {
    Ok(Process {
        new_module: None,
        new_params: None,
    })
}
