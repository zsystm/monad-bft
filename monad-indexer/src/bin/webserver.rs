use std::error::Error;

use actix_web::{get, web, App, HttpServer};
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::{
    pooled_connection::{bb8::Pool, AsyncDieselConnectionManager},
    AsyncPgConnection, RunQueryDsl,
};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[get("/blocks")]
async fn blocks(
    app_state: web::Data<AppState>,
) -> Result<web::Json<Vec<monad_indexer::models::BlockHeader>>, Box<dyn Error>> {
    let mut conn = app_state.pool.get().await?;

    use monad_indexer::schema::block_header::dsl::*;
    let block_headers = block_header
        .order(timestamp.desc())
        .limit(100)
        .load::<monad_indexer::models::BlockHeader>(&mut conn)
        .await?;
    Ok(web::Json(block_headers))
}

struct AppState {
    pool: Pool<AsyncPgConnection>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL not set");

    let config = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    let pool = Pool::builder()
        .build(config)
        .await
        .expect("failed to build pool");

    let app_state = web::Data::new(AppState { pool });

    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .service(web::scope("/api/v1").service(blocks))
    })
    .bind(("127.0.0.1", 8000))?
    .run()
    .await
}
