use arrow::{array::RecordBatch, datatypes::Schema};
use arrow_flight::{IpcMessage, sql::client::FlightSqlServiceClient};
use futures::TryStreamExt;
use sqllogictest::{AsyncDB, DBOutput};
use tonic::transport::{Channel, Endpoint};

use crate::{
    column::{ArrowColumnType, convert_batches, convert_schema_to_types},
    error::FlightSqlLogicTestError,
};

pub struct FlightSqlDB {
    engine_name: String,
    client: FlightSqlServiceClient<Channel>,
}

impl FlightSqlDB {
    pub async fn new(
        engine_name: impl Into<String>,
        mut client: FlightSqlServiceClient<Channel>,
        username: impl AsRef<str>,
        password: impl AsRef<str>,
    ) -> Result<Self, FlightSqlLogicTestError> {
        client
            .handshake(username.as_ref(), password.as_ref())
            .await?;

        Ok(Self {
            engine_name: engine_name.into(),
            client,
        })
    }

    pub async fn new_from_endpoint(
        engine_name: impl Into<String>,
        endpoint: impl Into<String>,
        username: impl AsRef<str>,
        password: impl AsRef<str>,
    ) -> Result<Self, FlightSqlLogicTestError> {
        let endpoint = Endpoint::from_shared(endpoint.into())?;
        let channel = endpoint.connect().await?;
        let client = FlightSqlServiceClient::new(channel);
        Self::new(engine_name, client, username, password).await
    }

    pub async fn execute(
        &mut self,
        query: impl Into<String>,
    ) -> Result<(Schema, Vec<RecordBatch>), FlightSqlLogicTestError> {
        let flight_info = self.client.execute(query.into(), None).await?;

        let schema: Schema = IpcMessage(flight_info.schema).try_into()?;

        let mut batches = Vec::new();
        for endpoint in flight_info.endpoint {
            let ticket = endpoint
                .ticket
                .as_ref()
                .expect("ticket is required")
                .clone();
            let stream = self.client.do_get(ticket).await?;
            let result: Vec<RecordBatch> = stream.try_collect().await?;
            batches.extend(result);
        }

        Ok((schema, batches))
    }
}

#[async_trait::async_trait]
impl AsyncDB for FlightSqlDB {
    type Error = FlightSqlLogicTestError;
    type ColumnType = ArrowColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let (schema, batches) = self.execute(sql).await?;
        let types = convert_schema_to_types(&schema.fields);
        let rows = convert_batches(&schema, batches)?;

        if rows.is_empty() && types.is_empty() {
            Ok(DBOutput::StatementComplete(0))
        } else {
            Ok(DBOutput::Rows { types, rows })
        }
    }

    /// Shutdown the connection gracefully.
    async fn shutdown(&mut self) {}

    /// Engine name of current database.
    fn engine_name(&self) -> &str {
        &self.engine_name
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;

    use arrow_flight::{
        HandshakeRequest, HandshakeResponse, flight_service_server::FlightServiceServer,
        sql::server::FlightSqlService,
    };
    use futures::{Stream, stream};
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Request, Response, Status, Streaming, metadata::MetadataValue, transport::Server};

    use super::FlightSqlDB;

    struct TestAuthService;

    #[tonic::async_trait]
    impl FlightSqlService for TestAuthService {
        type FlightService = Self;

        async fn do_handshake(
            &self,
            request: Request<Streaming<HandshakeRequest>>,
        ) -> Result<
            Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
            Status,
        > {
            let authorization = request
                .metadata()
                .get("authorization")
                .ok_or_else(|| Status::unauthenticated("missing authorization header"))?
                .to_str()
                .map_err(|_| Status::invalid_argument("authorization header is not utf-8"))?;

            if authorization != "Basic YWRtaW46cGFzc3dvcmQ=" {
                return Err(Status::unauthenticated("invalid credentials"));
            }

            let response = HandshakeResponse {
                protocol_version: 0,
                payload: Default::default(),
            };
            let mut response = Response::new(Box::pin(stream::iter(vec![Ok(response)]))
                as Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>);
            response.metadata_mut().insert(
                "authorization",
                MetadataValue::try_from("Bearer test-token")
                    .expect("bearer token should be valid metadata"),
            );
            Ok(response)
        }

        async fn register_sql_info(&self, _id: i32, _result: &arrow_flight::sql::SqlInfo) {}
    }

    async fn spawn_auth_server() -> String {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test server should bind");
        let addr = listener.local_addr().expect("test server should have addr");

        tokio::spawn(async move {
            Server::builder()
                .add_service(FlightServiceServer::new(TestAuthService))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("test server should serve");
        });

        format!("http://{addr}")
    }

    #[tokio::test]
    async fn new_from_endpoint_runs_handshake() {
        let endpoint = spawn_auth_server().await;

        let db = FlightSqlDB::new_from_endpoint("demo-db", endpoint, "admin", "password")
            .await
            .expect("db should be created after handshake");

        assert_eq!(db.client.token().map(String::as_str), Some("test-token"));
        assert_eq!(db.engine_name, "demo-db");
    }

    #[tokio::test]
    async fn new_from_endpoint_returns_auth_error_for_invalid_credentials() {
        let endpoint = spawn_auth_server().await;

        let result =
            FlightSqlDB::new_from_endpoint("demo-db", endpoint, "admin", "wrong-password").await;

        assert!(
            result.is_err(),
            "db creation should fail when handshake fails"
        );
        let error = result.err().expect("error should be present");
        assert!(error.to_string().contains("invalid credentials"));
    }
}
