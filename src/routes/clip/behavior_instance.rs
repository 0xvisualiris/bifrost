use serde_json::Value;
use uuid::Uuid;

use hue::api::{BehaviorInstance, BehaviorInstanceUpdate, RType, Resource, ResourceLink};

use crate::routes::clip::{ApiV2Result, V2Reply};
use crate::server::appstate::AppState;

pub async fn post_behavior_instance(state: &AppState, req: Value) -> ApiV2Result {
    let mut instance: BehaviorInstance = serde_json::from_value(req)?;

    instance.last_error = None;
    instance.status = Some("running".to_string());

    let link = RType::BehaviorInstance.link_to(Uuid::new_v4());

    state
        .res
        .lock()
        .await
        .add(&link, Resource::BehaviorInstance(instance))?;

    V2Reply::ok(link)
}

pub async fn put_behavior_instance(
    state: &AppState,
    rlink: ResourceLink,
    put: Value,
) -> ApiV2Result {
    let upd: BehaviorInstanceUpdate = serde_json::from_value(put)?;

    state
        .res
        .lock()
        .await
        .update::<BehaviorInstance>(&rlink.rid, |inst| {
            *inst += upd;
        })?;

    V2Reply::ok(rlink)
}
