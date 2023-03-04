mod client;
use client::Record;

use mongodb::{
    options::{ClientOptions, ResolverConfig},
    Client,
};
use std::error::Error;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client_uri = "mongodb+srv://radu:0fFCHRGolqlI1Mw@cluster0.6ogxery.mongodb.net/?retryWrites=true&w=majority";
    let options =
        ClientOptions::parse_with_resolver_config(&client_uri, ResolverConfig::cloudflare())
            .await?;
    let client = Client::with_options(options)?;

    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b';')
        .escape(Some(b'\\'))
        .from_path("./data/etapa1_full.csv")?;

    for result in rdr.deserialize() {
        let record: Record = result?;

        let new_document = bson::to_bson(&record)
            .unwrap()
            .as_document()
            .unwrap()
            .clone();

        let db_client: mongodb::Collection<_> = client.database("test").collection("contracts");

        let insert_result = db_client.insert_one(new_document.clone(), None).await?;
        println!("New document ID: {}", insert_result.inserted_id);
    }

    Ok(())
}
