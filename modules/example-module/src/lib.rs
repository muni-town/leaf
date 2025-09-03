use leaf_module_sdk::*;

register_handlers!(filter_inbound, filter_outbound, process_event);

fn filter_inbound(input: ModuleInput<String, String>) -> Result<InboundFilterResponse> {
    if serde_json::from_str::<serde_json::Value>(&input.payload).is_ok() {
        Ok(InboundFilterResponse::Allow)
    } else {
        Ok(InboundFilterResponse::Block {
            reason: "Message is not valid JSON".into(),
        })
    }
}

// Everything is public
fn filter_outbound(_input: ModuleInput<String, String>) -> Result<OutboundFilterResponse> {
    Ok(OutboundFilterResponse::Allow)
}

fn process_event(_input: ModuleInput<String, String>) -> Result<ModuleUpdate> {
    Ok(ModuleUpdate {
        new_module: None,
        new_params: None,
    })
}
