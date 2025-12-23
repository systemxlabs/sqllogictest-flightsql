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
    pub fn new(engine_name: impl Into<String>, client: FlightSqlServiceClient<Channel>) -> Self {
        Self {
            engine_name: engine_name.into(),
            client,
        }
    }

    pub async fn new_from_endpoint(
        engine_name: impl Into<String>,
        endpoint: impl Into<String>,
    ) -> Result<Self, FlightSqlLogicTestError> {
        let endpoint = Endpoint::from_shared(endpoint.into())?;
        let channel = endpoint.connect().await?;
        let client = FlightSqlServiceClient::new(channel);
        Ok(Self::new(engine_name, client))
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
