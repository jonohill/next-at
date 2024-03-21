extern crate derive_builder;

mod at;
mod db;
mod entity;
mod error;
mod geo;
mod gtfs;
mod maintenance;
mod stops;

#[cfg(test)]
mod test_utils;

use std::env;

use actix_web::{get, middleware::Logger, post, web, App, HttpResponse, HttpServer, Responder};
use at::client::AtClient;

use error::{NextAtError, NextAtResult};
use migration::{Migrator, MigratorTrait};
use reqwest::StatusCode;
use sea_orm::DatabaseConnection;
use serde::Deserialize;
use serde_json::json;
use tokio::select;

use crate::{db::util::open_seaorm, gtfs::realtime::monitor_firehose, maintenance::sync_and_index};

#[derive(Clone)]
pub struct ContextData {
    at_client: AtClient,
    db: DatabaseConnection,
}

#[derive(Deserialize)]
struct StopsQuery {
    lat: Option<f64>,
    lon: Option<f64>,
    code: Option<String>
}

#[get("/ok")]
async fn ok() -> NextAtResult<impl Responder> {
    Ok(HttpResponse::Ok().finish())
}

#[get("/stops")]
async fn get_stops(
    query: web::Query<StopsQuery>,
    ctx: web::Data<ContextData>,
) -> NextAtResult<impl Responder> {
    let mut stops = vec![];
    let mut lat = query.lat;
    let mut lon = query.lon;
    
    if let Some(code) = &query.code {
        if let Some(stop) = stops::get_stop_by_code(&ctx, code).await? {
            stops.push(stop.clone());
            // And nearby stops if no other location set
            if let (None, None, Some(stop_lat), Some(stop_lon)) = (lat, lon, stop.lat, stop.lon) {
                lat = Some(stop_lat);
                lon = Some(stop_lon);
            }
        }
    }

    if let (Some(lat), Some(lon)) = (lat, lon) {
        let mut nearby_stops = stops::get_closest_stops(&ctx, lat, lon, 5).await?;
        // without the existing stop if set
        if let Some(code) = &query.code {
            nearby_stops.retain(|s| s.code != *code);
        }

        stops.extend(nearby_stops);
    }

    let response = web::Json(json!({
        "stops": stops,
    }));
    Ok(response)
}

#[get("/stops/{stop_id}/routes")]
async fn get_stop_routes(
    params: web::Path<(String,)>,
    ctx: web::Data<ContextData>,
) -> NextAtResult<impl Responder> {
    let (stop_id,) = params.into_inner();

    let routes = stops::get_stop_routes(&ctx, &stop_id).await?;
    let response = web::Json(json!({
        "routes": routes,
    }));
    Ok(response)
}

#[get("/stops/{stop_id}/arrivals")]
async fn get_stop_arrivals(
    params: web::Path<(String,)>,
    ctx: web::Data<ContextData>,
) -> NextAtResult<impl Responder> {
    let (stop_id,) = params.into_inner();

    let arrivals = stops::get_stop_arrivals(&ctx, &stop_id).await?;
    let response = web::Json(json!({
        "stop_arrivals": arrivals,
    }));
    Ok(response)
}

#[post("/management/gtfs/sync")]
async fn sync_gtfs(ctx: web::Data<ContextData>) -> NextAtResult<impl Responder> {
    let new_records = gtfs::sync::Sync::sync(&ctx.db).await?;
    let response = web::Json(json!({
        "newRecords": new_records,
    }));
    Ok(response)
}

#[post("/management/gtfs/index-stoptimes")]
async fn index_stop_times() -> NextAtResult<impl Responder> {
    gtfs::index::build_stop_time_index().await?;
    let response = HttpResponse::Ok().status(StatusCode::NO_CONTENT).finish();
    Ok(response)
}

#[post("/management/gtfs/index-stops")]
async fn index_stops() -> NextAtResult<impl Responder> {
    gtfs::index::build_stop_index().await?;
    let response = HttpResponse::Ok().status(StatusCode::NO_CONTENT).finish();
    Ok(response)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info");
    }
    env_logger::try_init().ok();

    log::debug!("Debug logging enabled");

    dotenvy::from_filename(".env").ok();

    let at_client = AtClient::new().map_err(NextAtError::At).unwrap();
    let db = open_seaorm().await;

    log::info!("Migrating database");
    Migrator::up(&db, None)
        .await
        .expect("Failed to migrate database");

    sync_and_index(&db).await?;

    let ctx = ContextData { at_client, db };

    let firehose_ctx = ctx.clone();
    let firehose = monitor_firehose(&firehose_ctx);

    let maintenance = maintenance::keep_maintained();

    let listen_address = env::var("LISTEN_ADDRESS").unwrap_or("127.0.0.1:8080".to_string());

    log::info!("Starting server at {}", listen_address);

    let server = HttpServer::new(move || {
        let logger = Logger::default();

        let mut cors = actix_cors::Cors::default()
            .allowed_methods(vec!["GET"])
            .allowed_headers(vec!["accept"]);

        if let Ok(allowed_origin) = env::var("ALLOW_ORIGIN") {
            if allowed_origin == "*" {
                cors = cors.allow_any_origin();
            } else {
                cors = cors.allowed_origin(&allowed_origin);
            }
        }

        App::new()
            .wrap(logger)
            .wrap(cors)
            .app_data(web::Data::new(ctx.clone()))
            .service(ok)
            .service(get_stops)
            .service(get_stop_routes)
            .service(get_stop_arrivals)
            .service(sync_gtfs)
            .service(index_stop_times)
            .service(index_stops)
    })
    .bind(listen_address)?
    .run();

    select! {
        res = server => {
            log::info!("Server stopped");
            res?;
            Ok::<_, std::io::Error>(())
        },
        res = firehose => {
            log::info!("Firehose monitor stopped");
            res?;
            Ok(())
        }
        res = maintenance => {
            log::info!("Maintenance loop stopped");
            res?;
            Ok::<_, std::io::Error>(())
        }
    }?;

    Ok(())
}
