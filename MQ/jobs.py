# from activities import RUN
        
from datetime import timedelta
from temporalio import workflow

from temporalio.common import RetryPolicy

@workflow.defn
class MQflow:
    @workflow.run
    async def run(self, payload: dict) -> dict:
        return await workflow.execute_activity(
            "RUN",
            payload,
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                backoff_coefficient=2.0,
                maximum_interval=timedelta(seconds=10),
                maximum_attempts=1,
                non_retryable_error_types=["ValueError"],
            ),
        )
