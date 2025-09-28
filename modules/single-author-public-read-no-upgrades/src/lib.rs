use leaf_module_sdk::*;

register_handlers!(
    init_db,
    filter_inbound,
    filter_outbound,
    fetch,
    process_event
);

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

fn filter_outbound(_input: EventRequest<String, String>) -> Result<Outbound> {
    Ok(Outbound::Allow)
}

fn fetch(input: FetchInput) -> Result<()> {
    query(
        "
        insert into fetch
        select id from events where
            id >= :start
                and
            id < :end
        limit :limit
        ",
        vec![
            (":start".into(), input.start.unwrap_or(0).into()),
            (":end".into(), input.end.unwrap_or(i64::MAX).into()),
            (":limit".into(), input.limit.into()),
        ],
    );
    Ok(())
}

fn process_event(_input: IncomingEvent<String, String>) -> Result<Process> {
    Ok(Process {
        new_module: None,
        new_params: None,
    })
}
