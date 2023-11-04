use sea_orm::{ActiveModelTrait, DatabaseConnection, EntityTrait, IntoActiveModel};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

pub struct BatchInsert<T>
    where
        T: ActiveModelTrait,
        <<T as ActiveModelTrait>::Entity as EntityTrait>::Model: IntoActiveModel<T> {
    batch: Vec<T>,
    pub batch_size: usize,
    handles: Vec<JoinHandle<()>>,
    dispatchers: Vec<Sender<Vec<T>>>,
    last_dispatcher: usize,
}

impl<T: ActiveModelTrait + Send + Sync + 'static> BatchInsert<T>
    where
        <<T as ActiveModelTrait>::Entity as EntityTrait>::Model: IntoActiveModel<T>
{
    pub fn new(db: DatabaseConnection, batch_size: usize) -> BatchInsert<T> {
        let mut dispatchers = vec![];
        let mut handles = vec![];

        // The threads mostly wait on the server so spawning more then the cpu count is fine
        while handles.len() < 50 {
            let (tx, mut rx) = mpsc::channel::<Vec<T>>(512);
            let my_db = db.clone();

            dispatchers.push(tx);
            handles.push(tokio::spawn(async move {
                while let Some(batch) = rx.recv().await {
                    T::Entity::insert_many(batch.into_iter())
                        .exec(&my_db)
                        .await
                        .unwrap();
                }
            }))
        }

        BatchInsert {
            batch_size,
            batch: Vec::with_capacity(batch_size),
            handles,
            dispatchers,
            last_dispatcher: 0,
        }
    }
    pub async fn insert(&mut self, value: T) {
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

    pub async fn join(&mut self) {
        for handle in self.handles.drain(..) {
            tokio::join!(handle).0.unwrap();
        }
    }
}

