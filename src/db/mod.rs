use async_trait::async_trait;

pub mod model;
pub mod pg;

#[async_trait]
pub trait Db {
    type Conn;
    async fn conn(&self) -> Result<Self::Conn, sqlx::Error>;
}
