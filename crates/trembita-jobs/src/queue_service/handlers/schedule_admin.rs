use std::sync::Arc;

use trembita_net::{
    send_queue_list_schedules, send_queue_remove_schedule, send_queue_upsert_schedule,
};
use trembita_proto::{
    ProductWireError, QueueListSchedulesReply, QueueListSchedulesRequest, QueueRemoveScheduleReply,
    QueueRemoveScheduleRequest, QueueUpsertScheduleReply, QueueUpsertScheduleRequest,
};

use super::super::wire::stream_key;
use crate::wire_to_recurring_job;

use super::super::QueueService;

impl QueueService {
    pub(in crate::queue_service) async fn handle_list_schedules(
        &self,
        request: QueueListSchedulesRequest,
    ) -> QueueListSchedulesReply {
        let stream = stream_key(&request.stream);
        if self.sharded_stream(stream).is_some() {
            return QueueListSchedulesReply {
                schedules: Vec::new(),
                error: Some(ProductWireError::backend(
                    "recurring schedules are not supported on sharded streams",
                )),
            };
        }
        if self.state.is_leader() {
            match self.local_stream(stream) {
                Err(e) => QueueListSchedulesReply {
                    schedules: Vec::new(),
                    error: Some(e),
                },
                Ok(queue) => match queue.list_schedules().await {
                    Ok(schedules) => QueueListSchedulesReply {
                        schedules,
                        error: None,
                    },
                    Err(e) => QueueListSchedulesReply {
                        schedules: Vec::new(),
                        error: Some(ProductWireError::backend(e)),
                    },
                },
            }
        } else {
            let transport = Arc::clone(&self.transport);
            let request = request.clone();
            match self
                .forward_leader(move |leader| {
                    Box::pin(async move {
                        send_queue_list_schedules(transport.as_ref(), leader, &request).await
                    })
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => QueueListSchedulesReply {
                    schedules: Vec::new(),
                    error: Some(e),
                },
            }
        }
    }

    pub(in crate::queue_service) async fn handle_upsert_schedule(
        &self,
        request: QueueUpsertScheduleRequest,
    ) -> QueueUpsertScheduleReply {
        let stream = stream_key(&request.stream);
        if self.sharded_stream(stream).is_some() {
            return QueueUpsertScheduleReply {
                error: Some(ProductWireError::backend(
                    "recurring schedules are not supported on sharded streams",
                )),
            };
        }
        if self.state.is_leader() {
            match self.local_stream(stream) {
                Err(e) => QueueUpsertScheduleReply { error: Some(e) },
                Ok(queue) => {
                    let job = wire_to_recurring_job(&request.schedule);
                    match queue.upsert_schedule_replicated(&job).await {
                        Ok(ops) => {
                            if let Err(e) = self.replicate_ops(stream, &ops).await {
                                return QueueUpsertScheduleReply { error: Some(e) };
                            }
                            QueueUpsertScheduleReply { error: None }
                        }
                        Err(e) => QueueUpsertScheduleReply {
                            error: Some(ProductWireError::backend(e)),
                        },
                    }
                }
            }
        } else {
            let transport = Arc::clone(&self.transport);
            let request = request.clone();
            match self
                .forward_leader(move |leader| {
                    Box::pin(async move {
                        send_queue_upsert_schedule(transport.as_ref(), leader, &request).await
                    })
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => QueueUpsertScheduleReply { error: Some(e) },
            }
        }
    }

    pub(in crate::queue_service) async fn handle_remove_schedule(
        &self,
        request: QueueRemoveScheduleRequest,
    ) -> QueueRemoveScheduleReply {
        let stream = stream_key(&request.stream);
        if self.sharded_stream(stream).is_some() {
            return QueueRemoveScheduleReply {
                error: Some(ProductWireError::backend(
                    "recurring schedules are not supported on sharded streams",
                )),
            };
        }
        if self.state.is_leader() {
            match self.local_stream(stream) {
                Err(e) => QueueRemoveScheduleReply { error: Some(e) },
                Ok(queue) => match queue.remove_schedule_replicated(&request.name).await {
                    Ok(ops) => {
                        if let Err(e) = self.replicate_ops(stream, &ops).await {
                            return QueueRemoveScheduleReply { error: Some(e) };
                        }
                        QueueRemoveScheduleReply { error: None }
                    }
                    Err(e) => QueueRemoveScheduleReply {
                        error: Some(ProductWireError::backend(e)),
                    },
                },
            }
        } else {
            let transport = Arc::clone(&self.transport);
            let request = request.clone();
            match self
                .forward_leader(move |leader| {
                    Box::pin(async move {
                        send_queue_remove_schedule(transport.as_ref(), leader, &request).await
                    })
                })
                .await
            {
                Ok(reply) => reply,
                Err(e) => QueueRemoveScheduleReply { error: Some(e) },
            }
        }
    }
}
