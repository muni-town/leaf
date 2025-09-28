use base64::Engine;
use leaf_module_sdk::*;
use serde::Deserialize;

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
            "admin"  text not null
        )
        "#,
        Vec::new(),
    );
    query(
        r#"
        create table if not exists "members" (
            "id" text primary key
        )
        "#,
        Vec::new(),
    );
    query(
        "insert or ignore into state (id, admin) values (1, :admin)",
        vec![(":admin".into(), creator.into())],
    );
}

#[allow(unused)]
#[derive(Deserialize)]
enum Event {
    AdminEvent(AdminEvent),
    MemberEvent(MemberEvent),
}

#[allow(unused)]
#[derive(Deserialize)]
enum AdminEvent {
    AddMember(String),
    RemoveMember(String),
    UpdateModule {
        new_module: String,
        new_params: String,
    },
}

#[allow(unused)]
#[derive(Deserialize)]
enum MemberEvent {
    Message(String),
}

fn admin() -> String {
    query("select admin from state where id = 1", Vec::new())
        .parse_rows::<Vec<String>>()
        .unwrap()
        .pop()
        .unwrap()
}
fn is_member(user: String) -> bool {
    let records = query(
        "select 1 from members where id = :id",
        vec![(":id".into(), user.into())],
    );
    !records.rows.is_empty()
}

fn filter_inbound(input: IncomingEvent<String, String>) -> Result<Inbound> {
    let e = match serde_json::from_str::<Event>(&input.payload) {
        Ok(e) => e,
        Err(e) => {
            return Ok(Inbound::Block {
                reason: format!("Could not parse event: {e}"),
            });
        }
    };

    let admin = admin();
    match e {
        Event::AdminEvent(_) => {
            if input.user != admin {
                Ok(Inbound::Block {
                    reason: format!("Only {admin} can send admin events"),
                })
            } else {
                Ok(Inbound::Allow)
            }
        }
        Event::MemberEvent(_) => Ok(if input.user == admin || is_member(input.user) {
            Inbound::Allow
        } else {
            Inbound::Block {
                reason: "Only members can write events to this stream".into(),
            }
        }),
    }
}

fn filter_outbound(input: EventRequest<String, String>) -> Result<Outbound> {
    if input.requesting_user == admin() || is_member(input.requesting_user) {
        Ok(Outbound::Allow)
    } else {
        Ok(Outbound::Block)
    }
}

fn fetch(input: FetchInput) -> Result<()> {
    query(
        "
        insert into fetch
        select id from events where
            exists (
                select 1 from members where id = :user
                    union
                select 1 from state where admin = :user
            )
                and
            id >= :start
                and
            id < :end
        limit :limit
        ",
        vec![
            (":user".into(), input.requesting_user.into()),
            (":start".into(), input.start.unwrap_or(0).into()),
            (":end".into(), input.end.unwrap_or(i64::MAX).into()),
            (":limit".into(), input.limit.into()),
        ],
    );
    Ok(())
}

fn process_event(input: IncomingEvent<String, String>) -> Result<Process> {
    let Ok(event) = serde_json::from_str::<Event>(&input.payload) else {
        return Ok(Process {
            new_module: None,
            new_params: None,
        });
    };

    match event {
        Event::AdminEvent(admin_event) => match admin_event {
            AdminEvent::AddMember(id) => {
                query(
                    "insert or ignore into members (id) values (:id)",
                    vec![(":id".into(), id.into())],
                );
            }
            AdminEvent::RemoveMember(id) => {
                query(
                    "delete from members where id = :id",
                    vec![(":id".into(), id.into())],
                );
            }
            AdminEvent::UpdateModule {
                new_module,
                new_params,
            } => {
                let mut module = [0u8; 32];
                hex::decode_to_slice(new_module, &mut module)?;
                let params = base64::prelude::BASE64_STANDARD.decode(new_params)?;
                return Ok(Process {
                    new_module: Some(module),
                    new_params: Some(params),
                });
            }
        },
        Event::MemberEvent(_) => (),
    }

    Ok(Process {
        new_module: None,
        new_params: None,
    })
}
