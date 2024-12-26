use super::*;
use actix_web::{http::StatusCode, test};
use tempfile::tempdir;

#[actix_web::test]
async fn test_put_handler() {
  let temp_dir = tempdir().expect("Failed to create temp dir for put test");
  let mut opts = Options::default();
  opts.dir_path = temp_dir.path().to_path_buf();
  let engine = Arc::new(Engine::open(opts).unwrap());

  let mut app = test::init_service(
    App::new()
      .app_data(web::Data::new(engine.clone()))
      .service(Scope::new("/flash-kv").service(put_handler)),
  )
  .await;

  let req = test::TestRequest::with_uri("/flash-kv/put")
    .method(actix_web::http::Method::POST)
    .set_json(&json!({"key": "test", "value": "test value"}))
    .to_request();

  let resp = test::call_service(&mut app, req).await;
  assert_eq!(resp.status(), StatusCode::OK);
}

#[actix_web::test]
async fn test_get_handler() {
  let temp_dir = tempdir().expect("Failed to create temp dir for get test");
  let mut opts = Options::default();
  opts.dir_path = temp_dir.path().to_path_buf();
  let engine = Arc::new(Engine::open(opts).unwrap());

  engine
    .put((b"test" as &[u8]).into(), (b"test value" as &[u8]).into())
    .unwrap();

  let mut app = test::init_service(
    App::new()
      .app_data(web::Data::new(engine.clone()))
      .service(Scope::new("/flash-kv").service(get_handler)),
  )
  .await;

  let req = test::TestRequest::with_uri("/flash-kv/get/test").to_request();
  let resp = test::call_service(&mut app, req).await;
  assert_eq!(resp.status(), StatusCode::OK);
}

#[actix_web::test]
async fn test_listkeys_handler() {
  let temp_dir = tempdir().expect("Failed to create temp dir for listkeys test");
  let mut opts = Options::default();
  opts.dir_path = temp_dir.path().to_path_buf();
  let engine = Arc::new(Engine::open(opts).unwrap());

  engine
    .put((b"key1" as &[u8]).into(), (b"val1" as &[u8]).into())
    .unwrap();
  engine
    .put((b"key2" as &[u8]).into(), (b"val2" as &[u8]).into())
    .unwrap();

  let mut app = test::init_service(
    App::new()
      .app_data(web::Data::new(engine.clone()))
      .service(Scope::new("/flash-kv").service(listkeys_handler)),
  )
  .await;

  let req = test::TestRequest::with_uri("/flash-kv/listkeys").to_request();
  let resp = test::call_service(&mut app, req).await;
  assert_eq!(resp.status(), StatusCode::OK);
}

#[actix_web::test]
async fn test_stat_handler() {
  let temp_dir = tempdir().expect("Failed to create temp dir for stat test");
  let mut opts = Options::default();
  opts.dir_path = temp_dir.path().to_path_buf();
  let engine = Arc::new(Engine::open(opts).unwrap());

  let mut app = test::init_service(
    App::new()
      .app_data(web::Data::new(engine.clone()))
      .service(Scope::new("/flash-kv").service(stat_handler)),
  )
  .await;

  let req = test::TestRequest::with_uri("/flash-kv/stat").to_request();
  let resp = test::call_service(&mut app, req).await;
  assert_eq!(resp.status(), StatusCode::OK);
}
