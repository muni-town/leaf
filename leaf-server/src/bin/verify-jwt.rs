use atproto_identity::model::VerificationMethod;
use clap::Parser;

#[derive(clap::Parser)]
struct Args {
    handle: String,
    jwt: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let client = reqwest::Client::new();
    let dns_resolver = atproto_identity::resolve::HickoryDnsResolver::create_resolver(&[]);

    let did =
        atproto_identity::resolve::resolve_handle(&client, &dns_resolver, &args.handle).await?;

    let doc = atproto_identity::plc::query(&client, "plc.directory", &did).await?;

    let verification_method = doc
        .verification_method
        .into_iter()
        .next()
        .expect("Missing verification method");
    let VerificationMethod::Multikey {
        public_key_multibase,
        ..
    } = verification_method
    else {
        panic!("Unrecognized verification method");
    };
    let key_data = atproto_identity::key::identify_key(&public_key_multibase)?;

    let claims = atproto_oauth::jwt::verify(&args.jwt, &key_data)?;

    dbg!(claims);

    Ok(())
}
