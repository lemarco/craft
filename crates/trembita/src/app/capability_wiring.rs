//! Apply [`CapManifest`](crate::capability::CapManifest) to [`TrembitaAppBuilder`](super::builder::TrembitaAppBuilder).

use std::time::Duration;

use crate::TopicOpts;
use crate::capability::CapGroupApply;
use crate::capability::CapGroupScale;
use crate::capability::Route;
use crate::capability::group::{CapGroup, CapHostActor};
use crate::capability::runtime::{CapRuntime, OpBinding};

use super::builder::TrembitaAppBuilder;

fn op_declares_queued(routes: &[Route]) -> bool {
    routes
        .iter()
        .any(|r| matches!(r, Route::Queued | Route::QueuedWait | Route::Scheduled))
}

fn op_declares_event(routes: &[Route]) -> bool {
    routes.iter().any(|r| matches!(r, Route::Event))
}

pub(crate) fn wire_manifest(
    mut builder: TrembitaAppBuilder,
    groups: Vec<Box<dyn CapGroupApply>>,
) -> (TrembitaAppBuilder, CapRuntime) {
    let mut runtime = CapRuntime::empty();
    for group in groups {
        builder = CapGroupApply::apply(group, builder, &mut runtime);
    }
    (builder, runtime)
}

impl<S: Send + Default + 'static> CapGroupApply for CapGroup<S> {
    fn apply(
        self: Box<Self>,
        mut builder: TrembitaAppBuilder,
        runtime: &mut CapRuntime,
    ) -> TrembitaAppBuilder {
        let group = *self;
        let name = group.name();
        let scale = group.resolved_scale();
        builder.scale_plan.record_capability_group(name, scale);
        let queue_stream = group.queued_stream();
        let event_ingress = group.event_ingress_spec();
        let config = group.host_config(builder.cap_runtime.app_slot());

        for spec in group.ops() {
            if op_declares_queued(&spec.routes) && queue_stream.is_none() {
                builder.config_errors.push(format!(
                    "CapGroup {name:?}: op {:?} uses a queued route but group has no .queue_stream(...)",
                    spec.name
                ));
            }
            if op_declares_event(&spec.routes) && event_ingress.is_none() {
                builder.config_errors.push(format!(
                    "CapGroup {name:?}: op {:?} uses Route::Event but group has no .event_ingress(...)",
                    spec.name
                ));
            }
            runtime.insert(OpBinding {
                group: name,
                op: spec.name,
                routes: spec.routes.clone(),
                queue_stream,
                event_topic: event_ingress.map(|(topic, _)| topic),
                key: spec.key.clone(),
            });
        }

        builder.registration.actors = true;
        CapHostActor::<S>::register_local_spawn_config(&config);
        builder.inner = match scale {
            CapGroupScale::Fixed(instances) => builder
                .inner
                .manage::<CapHostActor<S>>(name, instances, config),
            CapGroupScale::PerNode => builder.inner.manage_auto::<CapHostActor<S>>(name, config),
        };

        if let Some(stream) = queue_stream {
            builder.queue_streams.insert(stream.to_string());
            builder.inner = builder.inner.job_queue(stream, Duration::from_secs(300));
            let group_name = name.to_string();
            builder.pending_consumers.push(Box::new(move |app, stop| {
                crate::capability::queue::spawn_bridge(app, group_name, stream, stop)
            }));
            builder.consumer_streams.push(stream.to_string());
        }

        if let Some((topic, subscription)) = event_ingress {
            builder = builder.topics([TopicOpts::topic(topic).subscriptions([subscription])]);
            let group_name = name.to_string();
            builder.pending_consumers.push(Box::new(move |app, stop| {
                crate::capability::event::spawn_bridge(app, group_name, topic, subscription, stop)
            }));
        }

        builder
    }
}
