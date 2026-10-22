use super::*;

#[tokio::test]
async fn a_reservation_can_be_consumed_only_once() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let reservations = ReservedListeners::bind_all(&BTreeMap::from([(address, "management")]))
        .await
        .unwrap();
    let stop = Shutdown::default();
    let http = reservations.http_adapter(stop.clone());
    let first = http.serve(address, Router::new());
    let error = http.serve(address, Router::new()).await.unwrap_err();
    assert!(error.to_string().contains(&address.to_string()));
    stop.request();
    first.await.unwrap();
    drop(reservations);
    TcpListener::bind(address).await.unwrap();
}

#[tokio::test]
async fn a_bind_error_identifies_the_owner_and_releases_earlier_reservations() {
    let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (free, occupied) = if first.local_addr().unwrap() < second.local_addr().unwrap() {
        (first, second)
    } else {
        (second, first)
    };
    let free_address = free.local_addr().unwrap();
    let occupied_address = occupied.local_addr().unwrap();
    drop(free);
    let declarations = BTreeMap::from([(free_address, "management"), (occupied_address, "status")]);
    let error = match ReservedListeners::bind_all(&declarations).await {
        Ok(_) => panic!("occupied listener must refuse startup"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("status"));
    assert!(error.to_string().contains(&occupied_address.to_string()));
    assert!(std::error::Error::source(&error).is_some());
    TcpListener::bind(free_address).await.unwrap();
}
