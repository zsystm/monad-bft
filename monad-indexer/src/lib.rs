use diesel::{Connection, PgConnection};

pub mod event;
pub mod models;
pub mod schema;

pub fn create_db_connection() -> PgConnection {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL not set");
    PgConnection::establish(&database_url)
        .unwrap_or_else(|_| panic!("error connecting to {}", database_url))
}
