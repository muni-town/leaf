use leaf_module_sdk::*;

register_handlers!(filter_inbound, filter_outbound, process_event);

fn filter_inbound(input: ModuleInput<String, String>) -> Result<Inbound> {
    if serde_json::from_str::<serde_json::Value>(&input.payload).is_ok() {
        Ok(Inbound::Allow)
    } else {
        Ok(Inbound::Block {
            reason: "Message is not valid JSON".into(),
        })
    }
}

// Everything is public
fn filter_outbound(_input: ModuleInput<String, String>) -> Result<Outbound> {
    Ok(Outbound::Allow)
}

fn process_event(_input: ModuleInput<String, String>) -> Result<Process> {
    Ok(Process {
        new_module: None,
        new_params: None,
    })
}
