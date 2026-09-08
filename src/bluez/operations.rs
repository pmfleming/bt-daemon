//! Ordered device-operation effects, independently testable without D-Bus.
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use super::{BluezAdapter, BluezBackend, BluezDevice};
use crate::backend::{DeviceOperation, OperationProgress, Params};
use crate::management::DevicePolicy;

pub(super) struct Plan {
    operation: DeviceOperation,
    power_on: Option<bool>,
    route_audio: bool,
}

impl Plan {
    pub(super) fn new(
        operation: DeviceOperation,
        params: &Value,
        policy: &DevicePolicy,
    ) -> Result<Self> {
        let connects = matches!(operation, DeviceOperation::Pair | DeviceOperation::Connect);
        Ok(Self {
            operation,
            power_on: if connects {
                Some(
                    params
                        .optional_bool("power_on")?
                        .unwrap_or(policy.power_on_connect),
                )
            } else {
                None
            },
            route_audio: connects
                && (policy.audio_route_on_connect == "switch"
                    || policy.preferred_audio_profile_key.is_some()),
        })
    }
}

#[async_trait]
trait Effects: Sync {
    async fn ensure_powered(&self, power_on: bool) -> Result<()>;
    async fn revoke_credentials(&self) -> Result<()>;
    async fn run(&self, operation: DeviceOperation) -> Result<()>;
    async fn route_audio(&self) -> Result<()>;
}

// Failure or cancellation stops the sequence. Earlier hardware effects cannot
// be rolled back reliably; never report success or forget local state afterward.
async fn execute(effects: &impl Effects, plan: Plan) -> Result<()> {
    if let Some(power_on) = plan.power_on {
        effects.ensure_powered(power_on).await?;
    }
    if plan.operation == DeviceOperation::Remove {
        effects.revoke_credentials().await?;
    }
    effects.run(plan.operation).await?;
    if plan.route_audio {
        effects.route_audio().await?;
    }
    Ok(())
}

pub(super) struct DeviceEffects<'a> {
    pub backend: &'a BluezBackend,
    pub key: &'a str,
    pub adapter: &'a BluezAdapter,
    pub device: &'a BluezDevice,
    pub params: &'a Value,
    pub policy: &'a DevicePolicy,
    pub progress: &'a OperationProgress,
}

impl DeviceEffects<'_> {
    pub(super) async fn execute(&self, plan: Plan) -> Result<()> {
        execute(self, plan).await
    }
}

#[async_trait]
impl Effects for DeviceEffects<'_> {
    async fn ensure_powered(&self, power_on: bool) -> Result<()> {
        self.backend
            .ensure_operation_adapter_powered(self.adapter, power_on, self.progress)
            .await
    }

    async fn revoke_credentials(&self) -> Result<()> {
        // Revoke before BlueZ removal: persistence failure must leave Forget retryable.
        if let Some(provider) = &self.backend.fast_pair {
            let provider = std::sync::Arc::clone(provider);
            let key = self.key.to_owned();
            tokio::task::spawn_blocking(move || provider.forget_account_key(&key)).await??;
        }
        Ok(())
    }

    async fn run(&self, operation: DeviceOperation) -> Result<()> {
        super::run_device_operation(
            self.adapter,
            self.device,
            operation,
            self.params,
            self.backend.fast_pair.as_deref(),
            self.policy,
            self.progress,
        )
        .await
    }

    async fn route_audio(&self) -> Result<()> {
        super::apply_audio_policy(self.key, self.device.address(), self.policy, self.progress).await
    }
}

#[cfg(test)]
mod tests {
    use super::{Effects, Plan, execute};
    use crate::{
        backend::{BackendError, BackendErrorKind, DeviceOperation},
        management::ManagementStore,
    };
    use anyhow::Result;
    use async_trait::async_trait;
    use futures::FutureExt;
    use serde_json::json;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Fake {
        calls: Mutex<Vec<&'static str>>,
        fail: Option<&'static str>,
        block: Option<&'static str>,
    }
    impl Fake {
        async fn call(&self, name: &'static str) -> Result<()> {
            self.calls.lock().unwrap().push(name);
            if self.block == Some(name) {
                std::future::pending::<()>().await;
            }
            if self.fail == Some(name) {
                return Err(BackendError::new(
                    BackendErrorKind::DeviceUnavailable,
                    "peer disappeared",
                )
                .into());
            }
            Ok(())
        }
    }
    #[async_trait]
    impl Effects for Fake {
        async fn ensure_powered(&self, allowed: bool) -> Result<()> {
            self.call(if allowed { "power" } else { "check-power" })
                .await
        }
        async fn revoke_credentials(&self) -> Result<()> {
            self.call("revoke").await
        }
        async fn run(&self, _: DeviceOperation) -> Result<()> {
            self.call("operation").await
        }
        async fn route_audio(&self) -> Result<()> {
            self.call("audio").await
        }
    }
    fn plan(operation: DeviceOperation) -> Plan {
        let store = ManagementStore::in_memory();
        let policy = store
            .update_device_policy("peer", &json!({"audio_route_on_connect":"switch"}))
            .unwrap();
        Plan::new(operation, &json!({}), &policy).unwrap()
    }

    #[tokio::test]
    async fn ordered_effects_follow_operation_policy() {
        for (op, expected) in [
            (DeviceOperation::Pair, vec!["power", "operation", "audio"]),
            (
                DeviceOperation::Connect,
                vec!["power", "operation", "audio"],
            ),
            (DeviceOperation::Remove, vec!["revoke", "operation"]),
            (DeviceOperation::Disconnect, vec!["operation"]),
            (DeviceOperation::SetTrusted, vec!["operation"]),
        ] {
            let fake = Fake::default();
            execute(&fake, plan(op)).await.unwrap();
            assert_eq!(*fake.calls.lock().unwrap(), expected);
        }
    }

    #[tokio::test]
    async fn failure_preserves_classification_and_never_runs_later_effects() {
        for op in [DeviceOperation::Connect, DeviceOperation::Remove] {
            let successful = Fake::default();
            execute(&successful, plan(op)).await.unwrap();
            let calls = successful.calls.into_inner().unwrap();
            for (i, stage) in calls.iter().enumerate() {
                let fake = Fake {
                    fail: Some(stage),
                    ..Fake::default()
                };
                let error = execute(&fake, plan(op)).await.unwrap_err();
                assert_eq!(
                    error.downcast_ref::<BackendError>().unwrap().kind,
                    BackendErrorKind::DeviceUnavailable
                );
                assert_eq!(*fake.calls.lock().unwrap(), calls[..=i]);
            }
        }
    }

    #[tokio::test]
    async fn cancellation_drops_pending_effect_without_advancing() {
        for (op, stage, expected) in [
            (
                DeviceOperation::Connect,
                "operation",
                vec!["power", "operation"],
            ),
            (DeviceOperation::Remove, "revoke", vec!["revoke"]),
        ] {
            let fake = Fake {
                block: Some(stage),
                ..Fake::default()
            };
            assert!(execute(&fake, plan(op)).now_or_never().is_none());
            assert_eq!(*fake.calls.lock().unwrap(), expected);
        }
    }

    #[tokio::test]
    async fn explicit_power_override_and_audio_defaults_are_respected() {
        let policy = ManagementStore::in_memory().device_policy("peer");
        let fake = Fake::default();
        execute(
            &fake,
            Plan::new(
                DeviceOperation::Connect,
                &json!({"power_on":false}),
                &policy,
            )
            .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(*fake.calls.lock().unwrap(), ["check-power", "operation"]);
        assert!(Plan::new(DeviceOperation::Pair, &json!({"power_on":"yes"}), &policy).is_err());
    }
}
