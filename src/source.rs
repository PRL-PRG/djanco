use parasite::{DatastoreView, StoreKind, Savepoint};
use anyhow::*;
use crate::Store;

pub trait Source {

}

pub struct DataSource {
    store: DatastoreView,
    substores: Vec<StoreKind>,
    savepoint: Savepoint,
}

impl DataSource {
    pub fn new<S>(dataset_path: S, savepoint: i64, substores: Vec<Store>) -> Result<Self> where S: Into<String> {
        let dataset_path = dataset_path.into();
        let store = DatastoreView::new(dataset_path.as_str());
        let savepoint = store.get_nearest_savepoint(savepoint)
            .with_context(|| {
                format!("Cannot find nearest savepoint to {} in store at path {}.",
                        savepoint, dataset_path)
            })?;
        let substores = substores.into_iter().map(|s| s.into()).collect();
        Ok(DataSource { store, savepoint, substores })
    }
}

impl Source for DataSource {
    // let substores: Vec<SubstoreView> = if substores.is_empty() { // Default: get all available substores
    //     store.substores().collect()
    // } else {
    //     substores.into_iter()
    //         .map(|substore| store.get_substore(substore.kind()))
    //         .collect()
    // };
}

