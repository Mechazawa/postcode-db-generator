use futures::executor::block_on;
use sea_orm::{DatabaseConnection, EntityTrait, Iterable};
use sea_orm::sea_query::OnConflict;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use crate::entities;
use crate::entities::node::ActiveModel as NodeModel;

pub struct BatchInsert {
    batch: Vec<NodeModel>,
    pub batch_size: usize,
    handles: Vec<JoinHandle<()>>,
    dispatchers: Vec<Sender<Vec<NodeModel>>>,
    last_dispatcher: usize,
}

impl Drop for BatchInsert where {
    fn drop(&mut self) {
        self.dispatchers.clear();

        for handle in self.handles.drain(..) {
            block_on(async {tokio::join!(handle).0.unwrap()});
        }
    }
}

impl BatchInsert
{
    pub fn new(db: DatabaseConnection, batch_size: usize, pool_size: usize) -> BatchInsert {
        let mut dispatchers = vec![];
        let mut handles = vec![];

        // The threads mostly wait on the server so spawning more then the cpu count is fine
        while handles.len() < pool_size {
            let (tx, rx) = mpsc::channel::<Vec<NodeModel>>(512);

            dispatchers.push(tx);
            handles.push(Self::dispatch(db.clone(), rx));
        }

        BatchInsert {
            batch_size,
            batch: Vec::with_capacity(batch_size),
            handles,
            dispatchers,
            last_dispatcher: 0,
        }
    }

    fn dispatch(db: DatabaseConnection, mut rx: Receiver<Vec<NodeModel>>) -> JoinHandle<()> {
        tokio::spawn(async move {
            while let Some(batch) = rx.recv().await {
                entities::node::Entity::insert_many(batch.into_iter())
                    .on_conflict(OnConflict::column(entities::node::Column::Id).update_columns(entities::node::Column::iter()).to_owned())
                    .exec(&db)
                    .await
                    .unwrap();
            }
        })
    }
    pub async fn insert(&mut self, value: NodeModel) {
        self.batch.push(value);

        if self.batch.len() >= self.batch_size {
            self.flush().await;
        }
    }

    pub async fn flush(&mut self) -> usize {
        let count = self.batch.len();
        let batch = self.batch.drain(..).collect();

        self.last_dispatcher = (self.last_dispatcher + 1) % self.dispatchers.len();

        self.dispatchers[self.last_dispatcher].send(batch).await.unwrap();

        self.batch = Vec::with_capacity(self.batch_size);

        count
    }
}