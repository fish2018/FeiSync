#![allow(unexpected_cfgs)]

use chrono::{DateTime, Duration, Utc};
#[cfg(target_os = "macos")]
use dispatch::Queue;
#[cfg(target_os = "macos")]
use tauri::ActivationPolicy;
use parking_lot::RwLock;
use reqwest::{multipart, Client, StatusCode, Url};
use rfd::FileDialog;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs,
    io::SeekFrom,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tauri::{AppHandle, Emitter, Manager, State, WindowEvent};
#[cfg(desktop)]
use tauri::menu::{MenuBuilder, MenuItemBuilder};
#[cfg(desktop)]
use tauri::tray::{MouseButton, TrayIconEvent};
use thiserror::Error;
use tokio::{
    fs as async_fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Notify,
};
use uuid::Uuid;
#[cfg(target_os = "macos")]
use {
    objc::{
        class,
        msg_send,
        rc::autoreleasepool,
        runtime::{Object, NO, YES},
        sel, sel_impl,
    },
    std::{ffi::CStr, os::raw::c_char},
};

const LARK_BASE: &str = "https://open.larksuite.com";
const FEISHU_BASE: &str = "https://open.feishu.cn";
const TENANT_STORE_FILE: &str = "feisync.tenants.json";
const RESOURCE_INDEX_FILE: &str = "feisync.resource-index.json";
const SECURITY_FILE: &str = "feisync.security.json";
const TRANSFER_STATE_FILE: &str = "feisync.transfers.json";

#[cfg(desktop)]
const TRAY_MENU_SHOW: &str = "tray.show";
#[cfg(desktop)]
const TRAY_MENU_HIDE: &str = "tray.hide";
#[cfg(desktop)]
const TRAY_MENU_QUIT: &str = "tray.quit";

#[derive(Debug, Error)]
enum AppError {
    #[error("{0}")]
    Message(String),
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}

type AppResult<T> = Result<T, AppError>;

fn log_transfer(action: &str, message: &str) {
    eprintln!(
        "{} transfer.{} {}",
        chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
        action,
        message
    );
}

fn sanitize_body(body: &str) -> String {
    body.replace('\n', " ").replace('\r', " ")
}

fn log_api_error(label: &str, status: StatusCode, body: &str) {
    log_transfer(
        "official_error",
        &format!(
            "{} status={} body={}",
            label,
            status.as_u16(),
            sanitize_body(body)
        ),
    );
}

fn api_error(label: &str, status: StatusCode, body: &str) -> AppError {
    log_api_error(label, status, body);
    AppError::Message(format!("{} ({}) {}", label, status, body))
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum TenantPlatform {
    Lark,
    Feishu,
}

impl Default for TenantPlatform {
    fn default() -> Self {
        TenantPlatform::Lark
    }
}

impl TenantPlatform {
    fn base_url(&self) -> &'static str {
        match self {
            TenantPlatform::Lark => LARK_BASE,
            TenantPlatform::Feishu => FEISHU_BASE,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct TenantConfig {
    id: String,
    name: String,
    app_id: String,
    app_secret: String,
    quota_gb: f64,
    used_gb: f64,
    active: bool,
    #[serde(default)]
    tenant_access_token: Option<String>,
    #[serde(default)]
    expire_at: Option<DateTime<Utc>>,
    #[serde(default)]
    platform: TenantPlatform,
    #[serde(default)]
    order: i32,
}

impl TenantConfig {
    fn needs_refresh(&self) -> bool {
        match (&self.tenant_access_token, &self.expire_at) {
            (Some(_), Some(expire)) => expire.timestamp() - Utc::now().timestamp() < 30 * 60,
            _ => true,
        }
    }

    fn to_public(&self) -> TenantPublic {
        TenantPublic {
            id: self.id.clone(),
            name: self.name.clone(),
            app_id: self.app_id.clone(),
            quota_gb: self.quota_gb,
            used_gb: self.used_gb,
            active: self.active,
            platform: self.platform.clone(),
            order: self.order,
        }
    }

    fn to_detail(&self) -> TenantDetail {
        TenantDetail {
            id: self.id.clone(),
            name: self.name.clone(),
            app_id: self.app_id.clone(),
            app_secret: Some(self.app_secret.clone()),
            quota_gb: self.quota_gb,
            used_gb: self.used_gb,
            active: self.active,
            platform: self.platform.clone(),
            order: self.order,
        }
    }

    fn api_base(&self) -> &str {
        self.platform.base_url()
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct TenantPublic {
    id: String,
    name: String,
    app_id: String,
    quota_gb: f64,
    used_gb: f64,
    active: bool,
    platform: TenantPlatform,
    order: i32,
}

#[derive(Clone, Serialize, Deserialize)]
struct TenantDetail {
    id: String,
    name: String,
    app_id: String,
    app_secret: Option<String>,
    quota_gb: f64,
    used_gb: f64,
    active: bool,
    platform: TenantPlatform,
    order: i32,
}

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
struct GroupConfig {
    id: String,
    name: String,
    #[serde(default)]
    remark: Option<String>,
    #[serde(default)]
    tenant_ids: Vec<String>,
}

#[derive(Serialize, Deserialize, Default)]
struct TenantStoreFile {
    tenants: Vec<TenantConfig>,
    #[serde(default)]
    groups: Vec<GroupConfig>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct GroupPublic {
    id: String,
    name: String,
    #[serde(default)]
    remark: Option<String>,
    tenant_ids: Vec<String>,
    api_key: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct GroupKeyRecord {
    group_id: String,
    hash: String,
    plain: String,
}

#[derive(Copy, Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TransferDirection {
    Upload,
    Download,
}

#[derive(Copy, Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TransferKind {
    FileUpload,
    FolderUpload,
    FileDownload,
    FolderDownload,
}

#[derive(Copy, Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TransferStatus {
    Pending,
    Running,
    Paused,
    Success,
    Failed,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(tag = "mode", rename_all = "snake_case")]
enum TransferResumeData {
    UploadFile {
        upload_id: String,
        block_size: u64,
        next_seq: u64,
        parent_token: String,
        file_path: String,
        file_name: String,
        size: u64,
    },
    DownloadFile {
        temp_path: String,
        target_path: String,
        downloaded: u64,
        token: String,
        file_name: String,
    },
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct TransferTaskRecord {
    id: String,
    direction: TransferDirection,
    kind: TransferKind,
    name: String,
    #[serde(default)]
    tenant_id: Option<String>,
    #[serde(default)]
    parent_token: Option<String>,
    #[serde(default)]
    resource_token: Option<String>,
    #[serde(default)]
    local_path: Option<String>,
    #[serde(default)]
    remote_path: Option<String>,
    #[serde(default)]
    size: u64,
    #[serde(default)]
    transferred: u64,
    status: TransferStatus,
    #[serde(default)]
    message: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    #[serde(default)]
    resume: Option<TransferResumeData>,
}

#[derive(Serialize, Deserialize, Default)]
struct TransferStateFile {
    tasks: Vec<TransferTaskRecord>,
}

#[derive(Debug)]
struct TransferControl {
    paused: AtomicBool,
    cancelled: AtomicBool,
    notify: Notify,
}

impl TransferControl {
    fn new() -> Self {
        TransferControl {
            paused: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

struct TransferTaskArgs {
    id: Option<String>,
    direction: TransferDirection,
    kind: TransferKind,
    name: String,
    tenant_id: Option<String>,
    parent_token: Option<String>,
    resource_token: Option<String>,
    local_path: Option<String>,
    remote_path: Option<String>,
    size: u64,
    transferred: u64,
    status: TransferStatus,
    resume: Option<TransferResumeData>,
    message: Option<String>,
}

#[derive(Clone)]
enum AccessScope {
    Admin,
    Group(String),
}

struct AppState {
    client: Client,
    store_path: PathBuf,
    resource_path: PathBuf,
    security_path: PathBuf,
    transfer_state_path: PathBuf,
    tenants: RwLock<HashMap<String, TenantConfig>>,
    groups: RwLock<HashMap<String, GroupConfig>>,
    group_keys: RwLock<HashMap<String, GroupKeyRecord>>,
    resource_index: RwLock<HashMap<String, String>>,
    api_key_hash: RwLock<Option<String>>,
    api_key_plain: RwLock<Option<String>>,
    transfers: RwLock<HashMap<String, TransferTaskRecord>>,
    transfer_controls: RwLock<HashMap<String, Arc<TransferControl>>>,
    active_tasks: RwLock<HashSet<String>>,
}

impl AppState {
    fn new() -> Self {
        let cwd = std::env::current_dir().unwrap();
        let store_path = cwd.join(TENANT_STORE_FILE);
        let resource_path = cwd.join(RESOURCE_INDEX_FILE);
        let security_path = cwd.join(SECURITY_FILE);
        let file_payload = if store_path.exists() {
            let content =
                fs::read_to_string(&store_path).expect("无法读取 feisync.tenants.json，请检查权限");
            serde_json::from_str::<TenantStoreFile>(&content).or_else(|_| {
                serde_json::from_str::<Vec<TenantConfig>>(&content).map(|tenants| TenantStoreFile {
                    tenants,
                    groups: Vec::new(),
                })
            }).expect("feisync.tenants.json 格式错误，请删除后重新启动")
        } else {
            let payload = TenantStoreFile::default();
            fs::write(
                &store_path,
                serde_json::to_string_pretty(&payload).unwrap(),
            )
            .expect("无法写入 feisync.tenants.json");
            payload
        };
        let mut tenant_list = file_payload.tenants;
        let group_list = file_payload.groups;
        tenant_list.sort_by_key(|t| t.order);
        for (idx, tenant) in tenant_list.iter_mut().enumerate() {
            if tenant.order == 0 {
                tenant.order = (idx + 1) as i32;
            }
        }
        let mut tenant_ids = HashSet::new();
        let tenants_map: HashMap<String, TenantConfig> = tenant_list
            .into_iter()
            .map(|t| {
                tenant_ids.insert(t.id.clone());
                (t.id.clone(), t)
            })
            .collect();
        let groups_map: HashMap<String, GroupConfig> = group_list
            .into_iter()
            .map(|mut g| {
                g.tenant_ids.retain(|id| tenant_ids.contains(id));
                (g.id.clone(), g)
            })
            .collect();
        let resource_index = if resource_path.exists() {
            fs::read_to_string(&resource_path)
                .ok()
                .and_then(|content| serde_json::from_str::<HashMap<String, String>>(&content).ok())
                .unwrap_or_default()
        } else {
            HashMap::new()
        };
        let (api_key_hash, api_key_plain, group_keys_vec) = if security_path.exists() {
            fs::read_to_string(&security_path)
                .ok()
                .map(|content| {
                    if content.trim_start().starts_with('{') {
                        serde_json::from_str::<SecurityFile>(&content).unwrap_or_default()
                    } else {
                        SecurityFile {
                            hash: if content.trim().is_empty() {
                                None
                            } else {
                                Some(content.trim().to_string())
                            },
                            plain: None,
                            group_keys: Vec::new(),
                        }
                    }
                })
                .map(|data| (data.hash, data.plain, data.group_keys))
                .unwrap_or((None, None, Vec::new()))
        } else {
            (None, None, Vec::new())
        };
        let group_keys_map: HashMap<String, GroupKeyRecord> = group_keys_vec
            .into_iter()
            .map(|record| (record.group_id.clone(), record))
            .collect();

        let transfer_state_path = cwd.join(TRANSFER_STATE_FILE);
        let transfer_file = if transfer_state_path.exists() {
            fs::read_to_string(&transfer_state_path)
                .ok()
                .and_then(|content| serde_json::from_str::<TransferStateFile>(&content).ok())
                .unwrap_or_default()
        } else {
            TransferStateFile::default()
        };
        let mut transfer_tasks = transfer_file.tasks;
        for task in transfer_tasks.iter_mut() {
            if matches!(task.status, TransferStatus::Running | TransferStatus::Pending) {
                task.status = TransferStatus::Failed;
                task.message = Some("上次运行异常终止，已停止。".into());
                task.updated_at = Utc::now();
            }
        }
        let transfers_map: HashMap<String, TransferTaskRecord> = transfer_tasks
            .into_iter()
            .map(|task| (task.id.clone(), task))
            .collect();
        AppState {
            client: Client::new(),
            store_path,
            resource_path,
            security_path,
            transfer_state_path,
            tenants: RwLock::new(tenants_map),
            groups: RwLock::new(groups_map),
            group_keys: RwLock::new(group_keys_map),
            resource_index: RwLock::new(resource_index),
            api_key_hash: RwLock::new(api_key_hash),
            api_key_plain: RwLock::new(api_key_plain),
            transfers: RwLock::new(transfers_map),
            transfer_controls: RwLock::new(HashMap::new()),
            active_tasks: RwLock::new(HashSet::new()),
        }
    }

    async fn update_tenant_meta(&self, payload: UpdateTenantPayload) -> AppResult<TenantPublic> {
        let mut need_refresh = false;
        {
            let mut map = self.tenants.write();
            let tenant = map
                .get_mut(&payload.tenant_id)
                .ok_or_else(|| AppError::Message("企业实例不存在".into()))?;
            if let Some(name) = payload.name.clone() {
                tenant.name = name;
            }
            if let Some(quota) = payload.quota_gb {
                tenant.quota_gb = quota;
            }
            if let Some(active) = payload.active {
                tenant.active = active;
            }
            if let Some(app_id) = payload.app_id.clone() {
                tenant.app_id = app_id;
                need_refresh = true;
            }
            if let Some(secret) = payload.app_secret.clone() {
                tenant.app_secret = secret;
                need_refresh = true;
            }
            if let Some(platform) = payload.platform.clone() {
                tenant.platform = platform;
                need_refresh = true;
            }
            if let Some(order) = payload.order {
                tenant.order = order;
            }
        }
        if need_refresh {
            self.refresh_token_by_id(&payload.tenant_id).await?;
        } else {
            self.save()?;
        }
        let map = self.tenants.read();
        map.get(&payload.tenant_id)
            .cloned()
            .ok_or_else(|| AppError::Message("企业实例不存在".into()))
            .map(|t| t.to_public())
    }

    fn remove_tenant(&self, tenant_id: &str) -> AppResult<()> {
        {
            let mut map = self.tenants.write();
            map.remove(tenant_id)
                .ok_or_else(|| AppError::Message("企业实例不存在".into()))?;
        }
        {
            let mut groups = self.groups.write();
            for group in groups.values_mut() {
                group.tenant_ids.retain(|id| id != tenant_id);
            }
        }
        self.save()?;
        {
            let mut resources = self.resource_index.write();
            resources.retain(|_, owner| owner != tenant_id);
        }
        self.save_resources()?;
        Ok(())
    }

    fn get_tenant_detail(&self, tenant_id: &str) -> AppResult<TenantDetail> {
        let map = self.tenants.read();
        map.get(tenant_id)
            .cloned()
            .ok_or_else(|| AppError::Message("企业实例不存在".into()))
            .map(|t| t.to_detail())
    }

    fn save(&self) -> AppResult<()> {
        eprintln!(
            "{} save begin",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
        );
        let tenants = self.tenants.read();
        let groups = self.groups.read();
        let payload = TenantStoreFile {
            tenants: tenants.values().cloned().collect(),
            groups: groups.values().cloned().collect(),
        };
        let data = serde_json::to_string_pretty(&payload)?;
        fs::write(&self.store_path, data)?;
        eprintln!(
            "{} save finished tenants={} groups={}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            payload.tenants.len(),
            payload.groups.len()
        );
        Ok(())
    }

    fn save_resources(&self) -> AppResult<()> {
        let map = self.resource_index.read();
        let data = serde_json::to_string_pretty(&*map)?;
        fs::write(&self.resource_path, data)?;
        Ok(())
    }

    fn hash_key(value: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(value.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    fn persist_security(&self) -> AppResult<()> {
        let data = SecurityFile {
            hash: self.api_key_hash.read().clone(),
            plain: self.api_key_plain.read().clone(),
            group_keys: self.group_keys.read().values().cloned().collect(),
        };
        let serialized = serde_json::to_string_pretty(&data)?;
        fs::write(&self.security_path, serialized)?;
        Ok(())
    }

    fn persist_transfers(&self) -> AppResult<()> {
        let guard = self.transfers.read();
        let payload = TransferStateFile {
            tasks: guard.values().cloned().collect(),
        };
        let json = serde_json::to_string_pretty(&payload)?;
        fs::write(&self.transfer_state_path, json)?;
        Ok(())
    }

    fn ensure_transfer_control(&self, id: &str) -> Arc<TransferControl> {
        let mut guard = self.transfer_controls.write();
        guard
            .entry(id.to_string())
            .or_insert_with(|| Arc::new(TransferControl::new()))
            .clone()
    }

    fn remove_transfer_control(&self, id: &str) {
        let mut guard = self.transfer_controls.write();
        guard.remove(id);
    }

    async fn wait_for_transfer_control(control: Option<&Arc<TransferControl>>) -> AppResult<()> {
        if let Some(ctrl) = control {
            loop {
                if ctrl.is_cancelled() {
                    return Err(AppError::Message("任务已取消".into()));
                }
                if !ctrl.is_paused() {
                    break;
                }
                ctrl.notify.notified().await;
            }
        }
        Ok(())
    }

    fn assert_not_cancelled(control: Option<&Arc<TransferControl>>) -> AppResult<()> {
        if let Some(ctrl) = control {
            if ctrl.is_cancelled() {
                return Err(AppError::Message("任务已取消".into()));
            }
        }
        Ok(())
    }

    fn emit_transfer_event(&self, app: Option<&AppHandle>, task: &TransferTaskRecord) {
        if let Some(handle) = app {
            let _ = handle.emit("transfer://event", task.clone());
        }
    }

    fn is_task_active(&self, id: &str) -> bool {
        self.active_tasks.read().contains(id)
    }

    fn register_active_control(&self, id: &str) -> Arc<TransferControl> {
        let control = self.ensure_transfer_control(id);
        {
            let mut guard = self.active_tasks.write();
            guard.insert(id.to_string());
        }
        control
    }

    fn unregister_active_task(&self, id: &str) {
        let mut guard = self.active_tasks.write();
        guard.remove(id);
    }

    fn list_transfer_snapshots(&self) -> Vec<TransferTaskRecord> {
        let mut list: Vec<_> = self.transfers.read().values().cloned().collect();
        list.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        list
    }

    fn get_transfer_task(&self, id: &str) -> AppResult<TransferTaskRecord> {
        self.transfers
            .read()
            .get(id)
            .cloned()
            .ok_or_else(|| AppError::Message("传输任务不存在".into()))
    }

    fn create_transfer_task(
        &self,
        args: TransferTaskArgs,
        app: Option<&AppHandle>,
    ) -> AppResult<TransferTaskRecord> {
        let now = Utc::now();
        let record = TransferTaskRecord {
            id: args.id.unwrap_or_else(|| Uuid::new_v4().to_string()),
            direction: args.direction,
            kind: args.kind,
            name: args.name,
            tenant_id: args.tenant_id,
            parent_token: args.parent_token,
            resource_token: args.resource_token,
            local_path: args.local_path,
            remote_path: args.remote_path,
            size: args.size,
            transferred: args.transferred,
            status: args.status,
            message: args.message,
            created_at: now,
            updated_at: now,
            resume: args.resume,
        };
        {
            let mut guard = self.transfers.write();
            guard.insert(record.id.clone(), record.clone());
        }
        self.ensure_transfer_control(&record.id);
        self.persist_transfers()?;
        self.emit_transfer_event(app, &record);
        Ok(record)
    }

    fn update_transfer_task<F>(
        &self,
        id: &str,
        mutator: F,
        app: Option<&AppHandle>,
    ) -> AppResult<TransferTaskRecord>
    where
        F: FnOnce(&mut TransferTaskRecord),
    {
        let mut guard = self.transfers.write();
        let task = guard
            .get_mut(id)
            .ok_or_else(|| AppError::Message("传输任务不存在".into()))?;
        mutator(task);
        task.updated_at = Utc::now();
        let snapshot = task.clone();
        drop(guard);
        self.persist_transfers()?;
        self.emit_transfer_event(app, &snapshot);
        Ok(snapshot)
    }

    fn record_transfer_progress(
        &self,
        id: &str,
        transferred: u64,
        resume: Option<TransferResumeData>,
        app: Option<&AppHandle>,
    ) -> AppResult<()> {
        let mut resume_data = resume;
        self.update_transfer_task(
            id,
            |task| {
                task.transferred = transferred.min(task.size);
                if let Some(data) = resume_data.take() {
                    task.resume = Some(data);
                }
            },
            app,
        )?;
        Ok(())
    }

    fn finalize_transfer(
        &self,
        id: &str,
        status: TransferStatus,
        message: Option<String>,
        app: Option<&AppHandle>,
    ) -> AppResult<()> {
        self.update_transfer_task(
            id,
            |task| {
                task.status = status;
                task.message = message.clone();
                if matches!(status, TransferStatus::Success) {
                    task.transferred = task.size;
                    task.resume = None;
                }
            },
            app,
        )?;
        self.unregister_active_task(id);
        self.remove_transfer_control(id);
        Ok(())
    }

    fn remove_transfer_tasks_by<F>(&self, predicate: F) -> AppResult<usize>
    where
        F: Fn(&TransferTaskRecord) -> bool,
    {
        let mut guard = self.transfers.write();
        let before = guard.len();
        let mut removed_ids = Vec::new();
        guard.retain(|id, task| {
            if predicate(task) {
                removed_ids.push(id.clone());
                false
            } else {
                true
            }
        });
        let removed = before.saturating_sub(guard.len());
        drop(guard);
        self.persist_transfers()?;
        if removed > 0 {
            let mut control_guard = self.transfer_controls.write();
            for id in removed_ids {
                self.unregister_active_task(&id);
                control_guard.remove(&id);
            }
        }
        Ok(removed)
    }

    fn delete_transfer_entry(&self, id: &str) -> AppResult<()> {
        let mut map = self.transfers.write();
        let record = map
            .remove(id)
            .ok_or_else(|| AppError::Message("传输任务不存在".into()))?;
        if matches!(record.status, TransferStatus::Running | TransferStatus::Pending) {
            map.insert(id.to_string(), record);
            return Err(AppError::Message("任务执行中，无法删除".into()));
        }
        drop(map);
        self.persist_transfers()?;
        self.remove_transfer_control(id);
        Ok(())
    }

    fn set_api_key(&self, key: String) -> AppResult<()> {
        let hash = Self::hash_key(&key);
        {
            let mut guard = self.api_key_hash.write();
            *guard = Some(hash);
        }
        {
            let mut guard = self.api_key_plain.write();
            *guard = Some(key);
        }
        self.persist_security()
    }

    fn set_group_key(&self, group_id: &str, key: String) -> AppResult<GroupKeyRecord> {
        let record = GroupKeyRecord {
            group_id: group_id.to_string(),
            hash: Self::hash_key(&key),
            plain: key,
        };
        {
            let mut map = self.group_keys.write();
            map.insert(group_id.to_string(), record.clone());
        }
        self.persist_security()?;
        Ok(record)
    }

    fn remove_group_key(&self, group_id: &str) -> AppResult<()> {
        {
            let mut map = self.group_keys.write();
            map.remove(group_id);
        }
        self.persist_security()
    }

    fn generate_local_key() -> String {
        Uuid::new_v4().to_string().replace('-', "")
    }

    fn ensure_group_key_record(&self, group_id: &str) -> AppResult<GroupKeyRecord> {
        if let Some(record) = {
            let map = self.group_keys.read();
            map.get(group_id).cloned()
        } {
            eprintln!(
                "{} ensure_group_key_record hit id={}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                group_id
            );
            Ok(record)
        } else {
            eprintln!(
                "{} ensure_group_key_record miss id={}, generating",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                group_id
            );
            self.set_group_key(group_id, Self::generate_local_key())
        }
    }

    fn make_group_public(&self, group: &GroupConfig) -> AppResult<GroupPublic> {
        eprintln!(
            "{} make_group_public start id={}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            group.id
        );
        let record = self.ensure_group_key_record(&group.id)?;
        let result = GroupPublic {
            id: group.id.clone(),
            name: group.name.clone(),
            remark: group.remark.clone(),
            tenant_ids: group.tenant_ids.clone(),
            api_key: record.plain.clone(),
        };
        eprintln!(
            "{} make_group_public done id={}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            group.id
        );
        Ok(result)
    }

    fn sanitize_group_tenants(&self, ids: &[String]) -> Vec<String> {
        let tenants = self.tenants.read();
        let mut unique = HashSet::new();
        ids.iter()
            .filter_map(|id| {
                if tenants.contains_key(id.as_str()) && unique.insert(id.clone()) {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    fn create_group(&self, payload: GroupPayload) -> AppResult<GroupPublic> {
        let group = GroupConfig {
            id: Uuid::new_v4().to_string(),
            name: payload.name,
            remark: payload.remark,
            tenant_ids: self.sanitize_group_tenants(&payload.tenant_ids),
        };
        eprintln!(
            "{} create_group start name={} tenants={}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            group.name,
            group.tenant_ids.len()
        );
        {
            let mut groups = self.groups.write();
            groups.insert(group.id.clone(), group.clone());
        }
        eprintln!(
            "{} create_group inserted id={}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            group.id
        );
        eprintln!(
            "{} create_group before save",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
        );
        self.save()?;
        eprintln!(
            "{} create_group saved id={}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            group.id
        );
        let public = self.make_group_public(&group)?;
        eprintln!(
            "{} create_group finished id={}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            public.id
        );
        Ok(public)
    }

    fn update_group_meta(&self, payload: UpdateGroupPayload) -> AppResult<GroupPublic> {
        eprintln!(
            "{} update_group start id={}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            payload.group_id
        );
        let snapshot = {
            let mut groups = self.groups.write();
            let group = groups
                .get_mut(&payload.group_id)
                .ok_or_else(|| AppError::Message("分组不存在".into()))?;
            if let Some(name) = payload.name {
                group.name = name;
            }
            if let Some(remark) = payload.remark {
                group.remark = Some(remark);
            }
            if let Some(ids) = payload.tenant_ids {
                group.tenant_ids = self.sanitize_group_tenants(&ids);
            }
            group.clone()
        };
        eprintln!(
            "{} update_group before save id={}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            payload.group_id
        );
        self.save()?;
        eprintln!(
            "{} update_group saved id={}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            payload.group_id
        );
        let public = self.make_group_public(&snapshot)?;
        eprintln!(
            "{} update_group finished id={}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            public.id
        );
        Ok(public)
    }

    fn remove_group(&self, group_id: &str) -> AppResult<()> {
        eprintln!(
            "{} remove_group start id={}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            group_id
        );
        {
            let mut groups = self.groups.write();
            if groups.remove(group_id).is_none() {
                return Err(AppError::Message("分组不存在".into()));
            }
        }
        eprintln!(
            "{} remove_group before save id={}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            group_id
        );
        self.save()?;
        let _ = self.remove_group_key(group_id);
        eprintln!(
            "{} remove_group finished id={}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            group_id
        );
        Ok(())
    }

    fn regenerate_group_key(&self, group_id: &str) -> AppResult<GroupPublic> {
        eprintln!(
            "{} regenerate_group_key start id={}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            group_id
        );
        if !self.groups.read().contains_key(group_id) {
            return Err(AppError::Message("分组不存在".into()));
        }
        let new_record = self.set_group_key(group_id, Self::generate_local_key())?;
        // ensure record stored
        {
            let mut map = self.group_keys.write();
            map.insert(group_id.to_string(), new_record);
        }
        let groups = self.groups.read();
        let group = groups
            .get(group_id)
            .ok_or_else(|| AppError::Message("分组不存在".into()))?;
        eprintln!(
            "{} regenerate_group_key building public id={}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            group_id
        );
        let public = self.make_group_public(group)?;
        eprintln!(
            "{} regenerate_group_key finished id={}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            group_id
        );
        Ok(public)
    }

    fn list_groups_snapshot(&self) -> AppResult<Vec<GroupPublic>> {
        eprintln!(
            "{} list_groups_snapshot start",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
        );
        let groups = self.groups.read();
        let mut list = Vec::new();
        for group in groups.values() {
            eprintln!(
                "{} list_groups_snapshot building id={}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                group.id
            );
            list.push(self.make_group_public(group)?);
        }
        list.sort_by(|a, b| a.name.cmp(&b.name));
        eprintln!(
            "{} list_groups_snapshot finished count={}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            list.len()
        );
        Ok(list)
    }

    fn ensure_admin(scope: &AccessScope) -> AppResult<()> {
        match scope {
            AccessScope::Admin => Ok(()),
            _ => Err(AppError::Message("需要管理员权限".into())),
        }
    }

    fn tenants_for_scope(&self, scope: &AccessScope) -> AppResult<Vec<TenantConfig>> {
        let tenants = self.tenants.read();
        let list = match scope {
            AccessScope::Admin => tenants.values().cloned().collect(),
            AccessScope::Group(group_id) => {
                let groups = self.groups.read();
                let group = groups
                    .get(group_id)
                    .ok_or_else(|| AppError::Message("分组不存在".into()))?;
                group
                    .tenant_ids
                    .iter()
                    .filter_map(|id| tenants.get(id))
                    .cloned()
                    .collect()
            }
        };
        Ok(list)
    }

    fn select_active_tenant_for_scope(&self, scope: &AccessScope) -> AppResult<String> {
        match scope {
            AccessScope::Admin => self.select_active_tenant(),
            AccessScope::Group(group_id) => {
                let groups = self.groups.read();
                let group = groups
                    .get(group_id)
                    .ok_or_else(|| AppError::Message("分组不存在".into()))?;
                let tenants = self.tenants.read();
                group
                    .tenant_ids
                    .iter()
                    .filter_map(|id| tenants.get(id))
                    .filter(|t| t.active)
                    .min_by(|a, b| {
                        let ratio = |tenant: &TenantConfig| {
                            if tenant.quota_gb.abs() < f64::EPSILON {
                                f64::MAX
                            } else {
                                tenant.used_gb / tenant.quota_gb
                            }
                        };
                        ratio(a)
                            .partial_cmp(&ratio(b))
                            .unwrap_or(std::cmp::Ordering::Equal)
                            .then_with(|| a.order.cmp(&b.order))
                    })
                    .map(|t| t.id.clone())
                    .ok_or_else(|| AppError::Message("当前分组无可用企业实例".into()))
            }
        }
    }

    fn scope_for_key(&self, value: &str) -> AppResult<AccessScope> {
        if let Some(expected) = self.api_key_hash.read().as_ref() {
            if *expected == Self::hash_key(value) {
                return Ok(AccessScope::Admin);
            }
        } else {
            return Ok(AccessScope::Admin);
        }
        let hash = Self::hash_key(value);
        let map = self.group_keys.read();
        for record in map.values() {
            if record.hash == hash {
                return Ok(AccessScope::Group(record.group_id.clone()));
            }
        }
        Err(AppError::Message("API Key 无效".into()))
    }

    fn verify_api_key(&self, provided: Option<String>) -> AppResult<AccessScope> {
        if let Some(value) = provided.or_else(|| self.api_key_plain.read().clone()) {
            return self.scope_for_key(&value);
        }
        if self.api_key_hash.read().is_none() {
            Ok(AccessScope::Admin)
        } else {
            Err(AppError::Message("缺少 API Key".into()))
        }
    }

    fn assert_scope_for_tenant(&self, scope: &AccessScope, tenant_id: &str) -> AppResult<()> {
        match scope {
            AccessScope::Admin => Ok(()),
            AccessScope::Group(group_id) => {
                let groups = self.groups.read();
                let group = groups
                    .get(group_id)
                    .ok_or_else(|| AppError::Message("分组不存在".into()))?;
                if group.tenant_ids.iter().any(|id| id == tenant_id) {
                    Ok(())
                } else {
                    Err(AppError::Message("无权访问目标企业实例".into()))
                }
            }
        }
    }

    fn assert_scope_for_token(&self, scope: &AccessScope, token: &str) -> AppResult<String> {
        let tenant_id = self
            .resolve_tenant_for_token(token)
            .map_err(|e| AppError::Message(e.to_string()))?;
        self.assert_scope_for_tenant(scope, &tenant_id)?;
        Ok(tenant_id)
    }

    fn register_resource<S: Into<String>>(&self, tenant_id: &str, token: S) -> AppResult<()> {
        let mut map = self.resource_index.write();
        map.insert(token.into(), tenant_id.to_string());
        drop(map);
        self.save_resources()
    }

    fn register_resources<I, S>(&self, tenant_id: &str, tokens: I) -> AppResult<()>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut map = self.resource_index.write();
        for token in tokens {
            map.insert(token.into(), tenant_id.to_string());
        }
        drop(map);
        self.save_resources()
    }

    fn remove_resource(&self, token: &str) -> AppResult<()> {
        let mut map = self.resource_index.write();
        map.remove(token);
        drop(map);
        self.save_resources()
    }

    fn resolve_tenant_for_token(&self, token: &str) -> AppResult<String> {
        let map = self.resource_index.read();
        map.get(token)
            .cloned()
            .ok_or_else(|| AppError::Message("未找到资源对应的企业实例，请先通过 FeiSync 列表获取该资源。".into()))
    }

    async fn enrich_entries_with_meta(&self, tenant: &TenantConfig, entries: &mut [FileEntry]) -> AppResult<()> {
        let token = tenant
            .tenant_access_token
            .as_ref()
            .ok_or_else(|| AppError::Message("token 不存在".into()))?
            .to_string();
        let client = &self.client;
        let mut index = 0;
        let chunk_size = 200usize;
        while index < entries.len() {
            let end = (index + chunk_size).min(entries.len());
            let docs: Vec<_> = entries[index..end]
                .iter()
                .filter(|entry| !entry.entry_type.is_empty())
                .map(|entry| {
                    serde_json::json!({
                        "doc_token": entry.token,
                        "doc_type": entry.entry_type
                    })
                })
                .collect();
            index = end;
            if docs.is_empty() {
                continue;
            }
            let body = serde_json::json!({ "request_docs": docs });
            let resp = client
                .post(format!(
                    "{}/open-apis/drive/v1/metas/batch_query",
                    tenant.api_base()
                ))
                .bearer_auth(&token)
                .json(&body)
                .send()
                .await?
                .error_for_status()?;
            let value = resp.json::<MetaBatchResponse>().await?;
            if value.code != 0 {
                continue;
            }
            if let Some(data) = value.data {
                for meta in data.metas {
                    if let Some(entry) = entries.iter_mut().find(|item| item.token == meta.doc_token) {
                        if let Some(ts) = meta.latest_modify_time.or(meta.create_time) {
                            entry.update_time = Some(ts);
                        }
                        if entry.size.is_none() {
                            entry.size = meta.file_size.or(meta.size);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn select_active_tenant(&self) -> AppResult<String> {
        let tenants = self.tenants.read();
        tenants
            .values()
            .filter(|t| t.active)
            .min_by(|a, b| {
                let a_ratio = if a.quota_gb.abs() < f64::EPSILON {
                    f64::MAX
                } else {
                    a.used_gb / a.quota_gb
                };
                let b_ratio = if b.quota_gb.abs() < f64::EPSILON {
                    f64::MAX
                } else {
                    b.used_gb / b.quota_gb
                };
                match a_ratio.partial_cmp(&b_ratio).unwrap_or(std::cmp::Ordering::Equal) {
                    std::cmp::Ordering::Equal => a.order.cmp(&b.order),
                    other => other,
                }
            })
            .map(|t| t.id.clone())
            .ok_or_else(|| AppError::Message("暂无可用企业实例，请先添加。".into()))
    }

    async fn add_tenant(&self, payload: TenantPayload) -> AppResult<TenantPublic> {
        let TenantPayload {
            name,
            app_id,
            app_secret,
            quota_gb,
            platform,
        } = payload;
        let next_order = {
            let map = self.tenants.read();
            map.len() as i32 + 1
        };
        let mut tenant = TenantConfig {
            id: Uuid::new_v4().to_string(),
            name,
            app_id,
            app_secret,
            quota_gb,
            used_gb: 0.0,
            active: true,
            tenant_access_token: None,
            expire_at: None,
            platform: platform.unwrap_or_default(),
            order: next_order,
        };
        let token = self.fetch_tenant_token(&tenant).await?;
        tenant.tenant_access_token = Some(token.tenant_access_token.clone());
        tenant.expire_at = Some(Utc::now() + Duration::seconds(token.expire as i64));

        let mut map = self.tenants.write();
        map.insert(tenant.id.clone(), tenant.clone());
        drop(map);
        self.save()?;
        Ok(tenant.to_public())
    }

    async fn refresh_token_by_id(&self, tenant_id: &str) -> AppResult<TenantPublic> {
        let tenant = {
            let map = self.tenants.read();
            map.get(tenant_id)
                .cloned()
                .ok_or_else(|| AppError::Message("企业实例不存在".into()))?
        };
        let token = self.fetch_tenant_token(&tenant).await?;
        let mut map = self.tenants.write();
        if let Some(entry) = map.get_mut(tenant_id) {
            entry.tenant_access_token = Some(token.tenant_access_token);
            entry.expire_at = Some(Utc::now() + Duration::seconds(token.expire as i64));
        }
        drop(map);
        self.save()?;
        let updated = {
            let map = self.tenants.read();
            map.get(tenant_id).cloned().unwrap().to_public()
        };
        Ok(updated)
    }

    async fn ensure_token(&self, tenant_id: &str) -> AppResult<TenantConfig> {
        let needs_refresh = {
            let map = self.tenants.read();
            map.get(tenant_id)
                .cloned()
                .ok_or_else(|| AppError::Message("企业实例不存在".into()))?
        };
        if needs_refresh.needs_refresh() {
            self.refresh_token_by_id(tenant_id).await?;
        }
        let map = self.tenants.read();
        Ok(map
            .get(tenant_id)
            .cloned()
            .ok_or_else(|| AppError::Message("企业实例不存在".into()))?)
    }

    async fn fetch_tenant_token(&self, tenant: &TenantConfig) -> AppResult<TenantTokenResponse> {
        let url = format!(
            "{}/open-apis/auth/v3/tenant_access_token/internal",
            tenant.api_base()
        );
        let resp = self
            .client
            .post(url)
            .json(&serde_json::json!({
                "app_id": tenant.app_id,
                "app_secret": tenant.app_secret
            }))
            .send()
            .await?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            return Err(api_error("tenant_access_token", status, &text));
        }
        let data: TenantTokenResponse = serde_json::from_str(&text)?;
        if data.code != 0 {
            log_transfer(
                "tenant_access_token.code",
                &format!(
                    "tenant={} code={} msg={}",
                    tenant.id,
                    data.code,
                    data.msg.clone().unwrap_or_default()
                ),
            );
            return Err(AppError::Message(data.msg.unwrap_or_else(|| "获取 token 失败".into())));
        }
        Ok(data)
    }

    async fn drive_get<T: for<'de> Deserialize<'de>>(
        &self,
        tenant: &TenantConfig,
        path: &str,
        query: Option<Vec<(String, String)>>,
    ) -> AppResult<T> {
        let url = build_url(tenant.api_base(), path, query)?;
        let resp = self
            .client
            .get(url)
            .bearer_auth(tenant.tenant_access_token.as_ref().ok_or_else(|| {
                AppError::Message("token 不存在".into())
            })?)
            .send()
            .await?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            return Err(api_error(path, status, &text));
        }
        Ok(serde_json::from_str::<T>(&text)?)
    }

    async fn forward_request(
        &self,
        tenant: &TenantConfig,
        method: &str,
        path: &str,
        query: Option<Vec<(String, String)>>,
        body: Option<Value>,
    ) -> AppResult<Value> {
        let url = build_url(tenant.api_base(), path, query)?;
        let token = tenant
            .tenant_access_token
            .as_ref()
            .ok_or_else(|| AppError::Message("token 不存在".into()))?;
        let builder = match method.to_uppercase().as_str() {
            "GET" => self.client.get(url),
            "POST" => self.client.post(url),
            "PUT" => self.client.put(url),
            "PATCH" => self.client.patch(url),
            "DELETE" => self.client.delete(url),
            _ => return Err(AppError::Message("不支持的 HTTP 方法".into())),
        };
        let builder = if let Some(body) = body {
            builder.json(&body)
        } else {
            builder
        };
        let resp = builder.bearer_auth(token).send().await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(api_error(path, status, &text));
        }
        Ok(match serde_json::from_str::<Value>(&text) {
            Ok(v) => v,
            Err(_) => Value::String(text),
        })
    }

    async fn upload_file_chunked(
        &self,
        tenant: &TenantConfig,
        path: &PathBuf,
        parent_token: &str,
        file_name: &str,
        file_size: u64,
        task_id: Option<&str>,
        app: Option<&AppHandle>,
        resume: Option<TransferResumeData>,
        control: Option<Arc<TransferControl>>,
    ) -> AppResult<String> {
        let token = tenant
            .tenant_access_token
            .as_ref()
            .ok_or_else(|| AppError::Message("token 不存在".into()))?
            .to_string();
        let prepare_url = build_url(tenant.api_base(), "/open-apis/drive/v1/files/upload_prepare", None)?;
        let upload_part_url = build_url(tenant.api_base(), "/open-apis/drive/v1/files/upload_part", None)?;
        let finish_url = build_url(tenant.api_base(), "/open-apis/drive/v1/files/upload_finish", None)?;
        let mut reader = async_fs::File::open(path).await?;
        let (upload_id, chunk_size, mut seq, mut transferred) =
            if let Some(TransferResumeData::UploadFile {
                upload_id: saved_id,
                block_size,
                next_seq,
                size,
                ..
            }) = resume.clone()
            {
                let start = (block_size * next_seq).min(size);
                reader.seek(std::io::SeekFrom::Start(start)).await?;
                (
                    saved_id,
                    usize::try_from(block_size).unwrap_or(4 * 1024 * 1024).max(1),
                    next_seq,
                    start,
                )
            } else {
                let prepare_resp = self
                    .client
                    .post(prepare_url)
                    .bearer_auth(&token)
                    .json(&serde_json::json!({
                        "file_name": file_name,
                        "parent_type": "explorer",
                        "parent_node": parent_token,
                        "size": file_size
                    }))
                    .send()
                    .await?;
                let prepare_status = prepare_resp.status();
                let prepare_text = prepare_resp.text().await.unwrap_or_default();
                if !prepare_status.is_success() {
                    return Err(api_error("upload_prepare", prepare_status, &prepare_text));
                }
                let prepare_resp =
                    serde_json::from_str::<DriveApiResponse<UploadPrepareResult>>(&prepare_text)?
                        .into_data()?;
                (
                    prepare_resp.upload_id.clone(),
                    usize::try_from(prepare_resp.block_size).unwrap_or(4 * 1024 * 1024).max(1),
                    0,
                    0,
                )
            };
        Self::wait_for_transfer_control(control.as_ref()).await?;
        if let Some(id) = task_id {
            let resume_payload = TransferResumeData::UploadFile {
                upload_id: upload_id.clone(),
                block_size: chunk_size as u64,
                next_seq: seq,
                parent_token: parent_token.to_string(),
                file_path: path.to_string_lossy().to_string(),
                file_name: file_name.to_string(),
                size: file_size,
            };
            self.record_transfer_progress(id, transferred, Some(resume_payload), app)?;
        }
        while transferred < file_size {
            Self::wait_for_transfer_control(control.as_ref()).await?;
            let remaining = file_size - transferred;
            let read_len = remaining.min(chunk_size as u64) as usize;
            let mut chunk = vec![0u8; read_len];
            reader.read_exact(&mut chunk).await?;
            let checksum = adler32_checksum(&chunk);
            let form = multipart::Form::new()
                .text("upload_id", upload_id.clone())
                .text("seq", seq.to_string())
                .text("size", read_len.to_string())
                .text("checksum", checksum.to_string())
                .part(
                    "file",
                    multipart::Part::bytes(chunk).file_name(format!("{}-{}", file_name, seq)),
                );
            let resp = self
                .client
                .post(upload_part_url.clone())
                .bearer_auth(&token)
                .multipart(form)
                .send()
                .await?;
            let status = resp.status();
            if !status.is_success() {
                let text = resp.text().await.unwrap_or_default();
                return Err(api_error("upload_part", status, &text));
            }
            seq += 1;
            transferred += read_len as u64;
            if let Some(id) = task_id {
                let resume_payload = TransferResumeData::UploadFile {
                    upload_id: upload_id.clone(),
                    block_size: chunk_size as u64,
                    next_seq: seq,
                    parent_token: parent_token.to_string(),
                    file_path: path.to_string_lossy().to_string(),
                    file_name: file_name.to_string(),
                    size: file_size,
                };
                self.record_transfer_progress(id, transferred, Some(resume_payload), app)?;
            }
        }
        if transferred == 0 {
            return Err(AppError::Message("文件内容为空".into()));
        }
        let finish_body = serde_json::json!({
            "upload_id": upload_id,
            "block_num": seq as i64
        });
        let finish_resp = self
            .client
            .post(finish_url)
            .bearer_auth(&token)
            .json(&finish_body)
            .send()
            .await?;
        let finish_status = finish_resp.status();
        let finish_text = finish_resp.text().await.unwrap_or_default();
        if !finish_status.is_success() {
            return Err(api_error("upload_finish", finish_status, &finish_text));
        }
        let finish_resp =
            serde_json::from_str::<DriveApiResponse<UploadFileResult>>(&finish_text)?.into_data()?;
        Ok(finish_resp.file_token)
    }

    async fn upload_local_file_path(
        &self,
        tenant_id: &str,
        tenant: &TenantConfig,
        parent_token: &str,
        path: &Path,
        file_name: &str,
        existing_task: Option<TransferTaskRecord>,
        app: Option<&AppHandle>,
    ) -> AppResult<String> {
        let metadata = async_fs::metadata(path).await?;
        if !metadata.is_file() {
            return Err(AppError::Message(format!("{} 不是文件", path.display())));
        }
        let sanitized = normalize_node_name(file_name)?;
        let task_record = if let Some(record) = existing_task {
            self.update_transfer_task(
                &record.id,
                |task| {
                    task.status = TransferStatus::Running;
                    task.message = None;
                },
                app,
            )?;
            record
        } else {
            self
                .create_transfer_task(
                    TransferTaskArgs {
                        id: None,
                        direction: TransferDirection::Upload,
                        kind: TransferKind::FileUpload,
                        name: sanitized.clone(),
                        tenant_id: Some(tenant_id.to_string()),
                        parent_token: Some(parent_token.to_string()),
                        resource_token: None,
                        local_path: Some(path.to_string_lossy().to_string()),
                        remote_path: None,
                        size: metadata.len(),
                        transferred: 0,
                        status: TransferStatus::Running,
                        resume: None,
                        message: None,
                    },
                    app,
                )?
        };
        let task_id = task_record.id.clone();
        let resume_state = match task_record.resume.clone() {
            Some(data @ TransferResumeData::UploadFile { .. }) => Some(data),
            _ => None,
        };
        let control = Some(self.register_active_control(&task_id));
        let result = if metadata.len() <= 20 * 1024 * 1024 {
            Self::wait_for_transfer_control(control.as_ref()).await?;
            let file_bytes = async_fs::read(path).await?;
            let token_value = tenant
                .tenant_access_token
                .clone()
                .ok_or_else(|| AppError::Message("缺少 tenant token".into()))?;
            let url = build_url(tenant.api_base(), "/open-apis/drive/v1/files/upload_all", None)?;
            let form = multipart::Form::new()
                .text("file_name", sanitized.clone())
                .text("parent_type", "explorer".to_string())
                .text("parent_node", parent_token.to_string())
                .text("size", metadata.len().to_string())
                .part("file", multipart::Part::bytes(file_bytes).file_name(sanitized.clone()));
        let resp = self
            .client
            .post(url)
            .bearer_auth(token_value)
            .multipart(form)
            .send()
            .await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(api_error("upload_all", status, &text));
        }
        let resp = serde_json::from_str::<DriveApiResponse<UploadFileResult>>(&text)?.into_data()?;
            Self::assert_not_cancelled(control.as_ref())?;
            self
                .record_transfer_progress(&task_id, metadata.len(), None, app)?;
            Ok(resp.file_token)
        } else {
            self
                .upload_file_chunked(
                    tenant,
                    &PathBuf::from(path),
                    parent_token,
                    &sanitized,
                    metadata.len(),
                    Some(task_id.as_str()),
                    app,
                    resume_state,
                    control.clone(),
                )
                .await
        };
        match result {
            Ok(file_token) => {
                self.register_resource(tenant_id, file_token.clone())?;
                self.finalize_transfer(&task_id, TransferStatus::Success, None, app)?;
                Ok(file_token)
            }
            Err(err) => {
                let message = err.to_string();
                let _ =
                    self.finalize_transfer(&task_id, TransferStatus::Failed, Some(message.clone()), app);
                Err(err)
            }
        }
    }

    async fn create_drive_folder_entry(
        &self,
        tenant: &TenantConfig,
        tenant_id: &str,
        parent_token: &str,
        raw_name: &str,
    ) -> AppResult<String> {
        let folder_name = normalize_node_name(raw_name)?;
        let resp = self
            .forward_request(
                tenant,
                "POST",
                "/open-apis/drive/v1/files/create_folder",
                None,
                Some(serde_json::json!({
                    "name": folder_name,
                    "folder_token": parent_token
                })),
            )
            .await?;
        let result = serde_json::from_value::<DriveApiResponse<CreateFolderResult>>(resp)?.into_data()?;
        self.register_resource(tenant_id, result.token.clone())?;
        Ok(result.token)
    }

    async fn upload_directory_recursive(
        &self,
        tenant_id: &str,
        tenant: &TenantConfig,
        parent_token: &str,
        dir_path: &Path,
        app: Option<&AppHandle>,
    ) -> AppResult<()> {
        let mut queue = VecDeque::new();
        queue.push_back((dir_path.to_path_buf(), parent_token.to_string()));
        while let Some((local_dir, remote_parent)) = queue.pop_front() {
            let folder_name = local_dir
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| AppError::Message(format!("无法解析文件夹名称: {}", local_dir.display())))?;
            let remote_token = self
                .create_drive_folder_entry(tenant, tenant_id, &remote_parent, folder_name)
                .await?;
            let mut entries = async_fs::read_dir(&local_dir).await?;
            while let Some(entry) = entries.next_entry().await? {
                let file_type = entry.file_type().await?;
                if file_type.is_dir() {
                    queue.push_back((entry.path(), remote_token.clone()));
                } else if file_type.is_file() {
                    let name = entry.file_name().to_string_lossy().to_string();
                    self.upload_local_file_path(
                        tenant_id,
                        tenant,
                        &remote_token,
                        &entry.path(),
                        &name,
                        None,
                        app,
                    )
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn download_drive_file(
        &self,
        tenant_id: &str,
        tenant: &TenantConfig,
        token: &str,
        dest_dir: &Path,
        file_name: &str,
        existing_task: Option<TransferTaskRecord>,
        app: Option<&AppHandle>,
        expected_size: Option<u64>,
    ) -> AppResult<PathBuf> {
        let token_value = tenant
            .tenant_access_token
            .as_ref()
            .ok_or_else(|| AppError::Message("token 不存在".into()))?;
        let url = build_url(
            tenant.api_base(),
            &format!("/open-apis/drive/v1/files/{}/download", token),
            None,
        )?;
        let sanitized = normalize_node_name(file_name)?;
        let mut target = dest_dir.to_path_buf();
        target.push(&sanitized);
        if let Some(parent) = target.parent() {
            async_fs::create_dir_all(parent).await?;
        }
        let mut temp = target.clone();
        temp.set_file_name(format!("{}.feisync.part", sanitized));
        let task_record = if let Some(record) = existing_task {
            self.update_transfer_task(
                &record.id,
                |task| {
                    task.status = TransferStatus::Running;
                    task.message = None;
                    if task.size == 0 {
                        task.size = expected_size.unwrap_or(0);
                    }
                },
                app,
            )?;
            record
        } else {
            self
                .create_transfer_task(
                    TransferTaskArgs {
                        id: None,
                        direction: TransferDirection::Download,
                        kind: TransferKind::FileDownload,
                        name: sanitized.clone(),
                        tenant_id: Some(tenant_id.to_string()),
                        parent_token: None,
                        resource_token: Some(token.to_string()),
                        local_path: Some(target.to_string_lossy().to_string()),
                        remote_path: None,
                        size: expected_size.unwrap_or(0),
                        transferred: 0,
                        status: TransferStatus::Running,
                        resume: None,
                        message: None,
                    },
                    app,
                )?
        };
        let task_id = task_record.id.clone();
        let control = Some(self.register_active_control(&task_id));
        let resume_state = match task_record.resume.clone() {
            Some(TransferResumeData::DownloadFile { downloaded, .. }) => downloaded,
            _ => 0,
        };
        let download_result: AppResult<PathBuf> = (|| async {
            let mut downloaded = resume_state;
            if downloaded == 0 && temp.exists() {
                downloaded = async_fs::metadata(&temp).await.map(|meta| meta.len()).unwrap_or(0);
            }
            let mut file = async_fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(&temp)
                .await?;
            file.seek(SeekFrom::Start(downloaded)).await?;
            let mut request = self.client.get(url).bearer_auth(token_value);
            if downloaded > 0 {
                request = request.header("Range", format!("bytes={}-", downloaded));
            }
            Self::wait_for_transfer_control(control.as_ref()).await?;
            let mut resp = request.send().await?;
            let status = resp.status();
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(api_error("download_drive_file", status, &body));
            }
            if task_record.size == 0 {
                if let Some(content_length) = resp.content_length() {
                    let total = downloaded + content_length;
                    let _ = self.update_transfer_task(
                        &task_id,
                        |task| task.size = total,
                        app,
                    );
                }
            }
            if downloaded > 0 {
                let resume_payload = TransferResumeData::DownloadFile {
                    temp_path: temp.to_string_lossy().to_string(),
                    target_path: target.to_string_lossy().to_string(),
                    downloaded,
                    token: token.to_string(),
                    file_name: sanitized.clone(),
                };
                self.record_transfer_progress(&task_id, downloaded, Some(resume_payload), app)?;
            }
            while let Some(chunk) = resp.chunk().await? {
                Self::wait_for_transfer_control(control.as_ref()).await?;
                file.write_all(&chunk).await?;
                downloaded += chunk.len() as u64;
                let resume_payload = TransferResumeData::DownloadFile {
                    temp_path: temp.to_string_lossy().to_string(),
                    target_path: target.to_string_lossy().to_string(),
                    downloaded,
                    token: token.to_string(),
                    file_name: sanitized.clone(),
                };
                self.record_transfer_progress(&task_id, downloaded, Some(resume_payload), app)?;
            }
            file.flush().await?;
            drop(file);
            async_fs::rename(&temp, &target).await?;
            Ok(target)
        })()
        .await;
        match download_result {
            Ok(path) => {
                self.finalize_transfer(&task_id, TransferStatus::Success, None, app)?;
                Ok(path)
            }
            Err(err) => {
                let message = err.to_string();
                let _ =
                    self.finalize_transfer(&task_id, TransferStatus::Failed, Some(message.clone()), app);
                Err(err)
            }
        }
    }

    async fn download_drive_folder(
        &self,
        tenant_id: &str,
        tenant: &TenantConfig,
        folder_token: &str,
        dest_dir: &Path,
        app: Option<&AppHandle>,
    ) -> AppResult<()> {
        let mut queue = VecDeque::new();
        queue.push_back((folder_token.to_string(), dest_dir.to_path_buf()));
        while let Some((remote_token, local_dir)) = queue.pop_front() {
            async_fs::create_dir_all(&local_dir).await?;
            let entries = list_folder(self, tenant, Some(remote_token.clone())).await?;
            for entry in entries {
                let sanitized = normalize_node_name(&entry.name)?;
                if entry.entry_type.eq_ignore_ascii_case("folder") {
                    queue.push_back((entry.token.clone(), local_dir.join(&sanitized)));
                } else {
                    self
                        .download_drive_file(
                            tenant_id,
                            tenant,
                            &entry.token,
                            &local_dir,
                            &sanitized,
                            None,
                            app,
                            entry.size.map(|size| size as u64),
                        )
                        .await?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct TenantPayload {
    name: String,
    app_id: String,
    app_secret: String,
    quota_gb: f64,
    #[serde(default)]
    platform: Option<TenantPlatform>,
}

#[derive(Deserialize)]
struct ProxyRequest {
    tenant_id: Option<String>,
    method: String,
    path: String,
    #[serde(default)]
    query: Vec<(String, String)>,
    body: Option<Value>,
    #[serde(default)]
    resource_token: Option<String>,
    #[serde(default)]
    _external: bool,
}

#[derive(Deserialize)]
struct DeleteFilePayload {
    token: String,
    #[serde(rename = "type")]
    file_type: String,
}

#[derive(Deserialize)]
struct CreateFolderPayload {
    parent_token: String,
    name: String,
}

#[derive(Deserialize)]
struct UploadFilePayload {
    parent_token: String,
    file_path: String,
    #[serde(default)]
    file_name: Option<String>,
}

#[derive(Deserialize)]
struct UploadFolderPayload {
    parent_token: String,
    dir_path: String,
}

#[derive(Serialize)]
struct PathInspectResponse {
    is_dir: bool,
    is_file: bool,
}

#[derive(Deserialize)]
struct DownloadFilePayload {
    token: String,
    dest_dir: String,
    file_name: String,
    #[serde(default)]
    size: Option<u64>,
}

#[derive(Deserialize)]
struct DownloadFolderPayload {
    token: String,
    dest_dir: String,
    folder_name: String,
}

#[derive(Deserialize)]
struct MoveFilePayload {
    token: String,
    #[serde(rename = "type")]
    file_type: String,
    target_parent: String,
}

#[derive(Deserialize)]
struct CopyFilePayload {
    token: String,
    #[serde(rename = "type")]
    file_type: String,
    target_parent: String,
    name: String,
}

#[derive(Deserialize)]
struct RenameFilePayload {
    token: String,
    #[serde(rename = "type")]
    file_type: String,
    name: String,
}

#[derive(Deserialize)]
struct PickFilesPayload {
    #[serde(default)]
    multiple: bool,
}

#[derive(Deserialize)]
struct PickEntriesPayload {
    #[serde(default)]
    multiple: bool,
}

#[derive(Serialize)]
struct PickDialogEntry {
    path: String,
    #[serde(rename = "type")]
    entry_type: PickEntryKind,
}

#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
enum PickEntryKind {
    File,
    Folder,
}

#[derive(Deserialize)]
struct UpdateKeyPayload {
    #[serde(rename = "currentKey")]
    current_key: Option<String>,
    #[serde(rename = "newKey")]
    new_key: String,
}

#[derive(Deserialize)]
struct UpdateTenantPayload {
    tenant_id: String,
    name: Option<String>,
    app_id: Option<String>,
    app_secret: Option<String>,
    quota_gb: Option<f64>,
    active: Option<bool>,
    platform: Option<TenantPlatform>,
    order: Option<i32>,
}

#[derive(Deserialize)]
struct ReorderTenant {
    tenant_id: String,
    order: i32,
}

#[derive(Deserialize)]
struct RemoveTenantPayload {
    tenant_id: String,
}

#[derive(Deserialize)]
struct GroupPayload {
    name: String,
    #[serde(default)]
    remark: Option<String>,
    #[serde(default)]
    tenant_ids: Vec<String>,
}

#[derive(Deserialize)]
struct UpdateGroupPayload {
    group_id: String,
    name: Option<String>,
    remark: Option<String>,
    tenant_ids: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct RemoveGroupPayload {
    group_id: String,
}

#[derive(Deserialize)]
struct TenantTokenResponse {
    code: i32,
    msg: Option<String>,
    tenant_access_token: String,
    expire: i64,
}

#[derive(Deserialize, Serialize)]
struct RootMetaData {
    code: i32,
    msg: String,
    data: RootMeta,
}

#[derive(Deserialize, Serialize)]
struct RootMeta {
    token: String,
}

#[derive(Deserialize, Serialize)]
struct FileListResponse {
    code: i32,
    msg: String,
    data: FileListData,
}

#[derive(Deserialize, Serialize)]
struct FileListData {
    files: Vec<RawFileEntry>,
}

#[derive(Deserialize, Serialize, Clone)]
struct RawFileEntry {
    token: String,
    name: String,
    #[serde(rename = "type")]
    type_field: String,
    #[serde(default)]
    parent_token: Option<String>,
    #[serde(default)]
    size: Option<i64>,
    #[serde(default)]
    update_time: Option<String>,
}

#[derive(Serialize, Clone)]
struct FileEntry {
    token: String,
    name: String,
    #[serde(rename = "type")]
    entry_type: String,
    parent_token: Option<String>,
    size: Option<i64>,
    update_time: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    tenant_name: Option<String>,
}

#[derive(Serialize, Deserialize, Default)]
struct SecurityFile {
    hash: Option<String>,
    plain: Option<String>,
    #[serde(default)]
    group_keys: Vec<GroupKeyRecord>,
}

#[derive(Debug, Deserialize)]
struct MetaBatchResponse {
    code: i32,
    #[serde(default)]
    _msg: String,
    data: Option<MetaBatchData>,
}

#[derive(Debug, Deserialize)]
struct MetaBatchData {
    metas: Vec<DocMeta>,
}

#[derive(Debug, Deserialize, Default)]
struct DocMeta {
    #[serde(rename = "doc_token")]
    doc_token: String,
    #[serde(rename = "doc_type")]
    _doc_type: String,
    #[serde(rename = "latest_modify_time")]
    latest_modify_time: Option<String>,
    #[serde(rename = "create_time")]
    create_time: Option<String>,
    #[serde(rename = "file_size")]
    file_size: Option<i64>,
    #[serde(rename = "size")]
    size: Option<i64>,
}

#[derive(Deserialize)]
struct DriveApiResponse<T> {
    code: i32,
    msg: String,
    data: Option<T>,
}

impl<T> DriveApiResponse<T> {
    fn into_data(self) -> AppResult<T> {
        if self.code != 0 {
            return Err(AppError::Message(self.msg));
        }
        self.data
            .ok_or_else(|| AppError::Message("响应缺少 data 字段".into()))
    }
}

#[derive(Serialize, Deserialize)]
struct CreateFolderResult {
    token: String,
    #[serde(default)]
    url: Option<String>,
}

#[derive(Deserialize)]
struct UploadFileResult {
    #[serde(rename = "file_token")]
    file_token: String,
}

#[derive(Deserialize)]
struct UploadPrepareResult {
    upload_id: String,
    block_size: u64,
    #[allow(dead_code)]
    block_num: u64,
}

#[derive(Deserialize)]
struct CopyFileResult {
    file: DriveFileMeta,
}

#[derive(Serialize, Deserialize, Clone)]
struct DriveFileMeta {
    token: String,
    name: String,
    #[serde(rename = "type")]
    entry_type: String,
    #[serde(default)]
    parent_token: Option<String>,
    #[serde(default)]
    url: Option<String>,
}

#[derive(Deserialize)]
struct MoveFileResult {
    #[serde(default)]
    task_id: Option<String>,
}

#[tauri::command]
async fn list_tenants(state: State<'_, AppState>, api_key: Option<String>) -> Result<Vec<TenantPublic>, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    AppState::ensure_admin(&scope).map_err(|e| e.to_string())?;
    let tenants = state.tenants.read();
    let mut list: Vec<_> = tenants.values().cloned().map(|t| t.to_public()).collect();
    list.sort_by_key(|t| t.order);
    Ok(list)
}

#[tauri::command]
async fn add_tenant(state: State<'_, AppState>, api_key: Option<String>, payload: TenantPayload) -> Result<TenantPublic, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    AppState::ensure_admin(&scope).map_err(|e| e.to_string())?;
    state.add_tenant(payload).await.map_err(|e| e.to_string())
}

#[tauri::command]
async fn refresh_tenant_token(
    state: State<'_, AppState>,
    api_key: Option<String>,
    tenant_id: String,
) -> Result<TenantPublic, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    AppState::ensure_admin(&scope).map_err(|e| e.to_string())?;
    state
        .refresh_token_by_id(&tenant_id)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn list_root_entries(
    state: State<'_, AppState>,
    api_key: Option<String>,
    tenant_id: Option<String>,
    aggregate: Option<bool>,
) -> Result<serde_json::Value, String> {
    let scope = state
        .verify_api_key(api_key.clone())
        .map_err(|e| e.to_string())?;
    let log = |action: &str, extra: &dyn std::fmt::Display| {
        let time = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        eprintln!("{} list_root_entries {} {}", time, action, extra);
    };
    log(
        "接收请求",
        &format!(
            "aggregate={} tenant_id={:?} api_key={}",
            aggregate.unwrap_or(false),
            tenant_id,
            api_key.is_some()
        ),
    );
    if aggregate.unwrap_or(false) && tenant_id.is_none() {
        let tenants_list: Vec<_> = state
            .tenants_for_scope(&scope)
            .map_err(|e| e.to_string())?
            .into_iter()
            .filter(|t| t.active)
            .collect();
        if tenants_list.is_empty() {
            return Err("暂无可用企业实例，请先添加。".into());
        }
        log("聚合请求", &format!("租户数={}", tenants_list.len()));
        let mut result_map = serde_json::Map::new();
        let chunk_size = 5usize;
        let mut index = 0;
        while index < tenants_list.len() {
            let chunk_end = (index + chunk_size).min(tenants_list.len());
            let chunk = tenants_list[index..chunk_end].to_vec();
            let fetch = |meta: TenantConfig| fetch_tenant_entries(state.inner(), meta);
            let results = match chunk.len() {
                5 => {
                    let (r1, r2, r3, r4, r5) = tokio::join!(
                        fetch(chunk[0].clone()),
                        fetch(chunk[1].clone()),
                        fetch(chunk[2].clone()),
                        fetch(chunk[3].clone()),
                        fetch(chunk[4].clone())
                    );
                    vec![r1, r2, r3, r4, r5]
                }
                4 => {
                    let (r1, r2, r3, r4) = tokio::join!(
                        fetch(chunk[0].clone()),
                        fetch(chunk[1].clone()),
                        fetch(chunk[2].clone()),
                        fetch(chunk[3].clone())
                    );
                    vec![r1, r2, r3, r4]
                }
                3 => {
                    let (r1, r2, r3) = tokio::join!(
                        fetch(chunk[0].clone()),
                        fetch(chunk[1].clone()),
                        fetch(chunk[2].clone())
                    );
                    vec![r1, r2, r3]
                }
                2 => {
                    let (r1, r2) = tokio::join!(fetch(chunk[0].clone()), fetch(chunk[1].clone()));
                    vec![r1, r2]
                }
                1 => {
                    let (r1,) = tokio::join!(fetch(chunk[0].clone()));
                    vec![r1]
                }
                _ => Vec::new(),
            };
            for res in results {
                let (id, value) = res.map_err(|e| e.to_string())?;
                result_map.insert(id, value);
            }
            index = chunk_end;
            log("聚合分片完成", &format!("progress={}/{}", index, tenants_list.len()));
        }
        return Ok(serde_json::json!({
            "aggregate": true,
            "entries": result_map
        }));
    }
    let selected_id = match tenant_id {
        Some(id) => {
            log("指定租户", &format!("tenant_id={}", id));
            state
                .assert_scope_for_tenant(&scope, &id)
                .map_err(|e| e.to_string())?;
            id
        }
        None => {
            let selected = state
                .select_active_tenant_for_scope(&scope)
                .map_err(|e| e.to_string())?;
            log("自动选择租户", &format!("tenant_id={}", selected));
            selected
        }
    };
    let tenant = state.ensure_token(&selected_id).await.map_err(|e| e.to_string())?;
    log(
        "加载租户成功",
        &format!("tenant_id={} name={} base={}", tenant.id, tenant.name, tenant.api_base()),
    );
    let root_meta: RootMetaData = state
        .drive_get(&tenant, "/open-apis/drive/explorer/v2/root_folder/meta", None)
        .await
        .map_err(|e| e.to_string())?;
    let root_token = root_meta.data.token.clone();
    state
        .register_resource(&selected_id, root_token.clone())
        .map_err(|e| e.to_string())?;
    let entries = list_folder(&state, &tenant, Some(root_token.clone()))
        .await
        .map_err(|e| e.to_string())?;
    Ok(serde_json::json!({
        "rootToken": root_token,
        "entries": entries
    }))
}

#[tauri::command]
async fn list_folder_entries(
    state: State<'_, AppState>,
    api_key: Option<String>,
    folder_token: String,
) -> Result<Vec<FileEntry>, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    let tenant_id = state
        .assert_scope_for_token(&scope, &folder_token)
        .map_err(|e| e.to_string())?;
    let tenant = state.ensure_token(&tenant_id).await.map_err(|e| e.to_string())?;
    list_folder(&state, &tenant, Some(folder_token))
        .await
        .map_err(|e| e.to_string())
}

async fn fetch_tenant_entries(state: &AppState, tenant_meta: TenantConfig) -> AppResult<(String, serde_json::Value)> {
    let tenant = state.ensure_token(&tenant_meta.id).await?;
    let root_meta: RootMetaData = state
        .drive_get(&tenant, "/open-apis/drive/explorer/v2/root_folder/meta", None)
        .await?;
    let root_token = root_meta.data.token.clone();
    state.register_resource(&tenant_meta.id, root_token.clone())?;
    let entries = list_folder(state, &tenant, Some(root_token)).await?;
    Ok((tenant_meta.id, serde_json::to_value(entries)?))
}
#[tauri::command]
async fn search_entries(
    state: State<'_, AppState>,
    api_key: Option<String>,
    keyword: String,
    tenant_id: Option<String>,
    root_name: Option<String>,
) -> Result<Vec<FileEntry>, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    let term = keyword.trim().to_lowercase();
    if term.is_empty() {
        return Ok(vec![]);
    }
    let selected_id = match tenant_id {
        Some(id) if !id.is_empty() => {
            state
                .assert_scope_for_tenant(&scope, &id)
                .map_err(|e| e.to_string())?;
            id
        }
        _ => state
            .select_active_tenant_for_scope(&scope)
            .map_err(|e| e.to_string())?,
    };
    let tenant = state.ensure_token(&selected_id).await.map_err(|e| e.to_string())?;
    let root_meta: RootMetaData = state
        .drive_get(&tenant, "/open-apis/drive/explorer/v2/root_folder/meta", None)
        .await
        .map_err(|e| e.to_string())?;
    let root_token = root_meta.data.token.clone();
    state
        .register_resource(&selected_id, root_token.clone())
        .map_err(|e| e.to_string())?;
    let root_label = root_name.unwrap_or_else(|| "Root".into());
    search_drive(&state, &tenant, &root_token, &root_label, &term)
        .await
        .map_err(|e| e.to_string())
}

async fn list_folder(state: &AppState, tenant: &TenantConfig, folder_token: Option<String>) -> AppResult<Vec<FileEntry>> {
    let mut query = Vec::new();
    if let Some(token) = folder_token.clone() {
        query.push(("folder_token".to_string(), token.clone()));
        state.register_resource(&tenant.id, token)?;
    }
    let resp: FileListResponse = state.drive_get(tenant, "/open-apis/drive/v1/files", Some(query)).await?;
    let mut entries: Vec<FileEntry> = resp
        .data
        .files
        .into_iter()
        .map(|item| FileEntry {
            token: item.token.clone(),
            name: item.name,
            entry_type: item.type_field,
            parent_token: item.parent_token,
            size: item.size,
            update_time: item.update_time,
            path: None,
            tenant_name: Some(tenant.name.clone()),
        })
        .collect();
    let tokens = entries.iter().map(|item| item.token.clone()).collect::<Vec<_>>();
    state.register_resources(&tenant.id, tokens)?;
    state.enrich_entries_with_meta(tenant, &mut entries).await?;
    Ok(entries)
}

async fn search_drive(
    state: &AppState,
    tenant: &TenantConfig,
    root_token: &str,
    root_name: &str,
    keyword: &str,
) -> AppResult<Vec<FileEntry>> {
    let mut results = Vec::new();
    let mut queue = VecDeque::new();
    let mut visited = HashSet::new();
    queue.push_back((root_token.to_string(), root_name.to_string()));
    visited.insert(root_token.to_string());
    while let Some((folder, current_path)) = queue.pop_front() {
        let entries = list_folder(state, tenant, Some(folder.clone())).await?;
        for entry in entries.iter() {
            if entry.name.to_lowercase().contains(keyword) {
                let mut enriched = entry.clone();
                enriched.path = Some(format!("{} / {}", current_path, entry.name));
                enriched.tenant_name = Some(tenant.name.clone());
                results.push(enriched);
            }
            if entry.entry_type.to_lowercase() == "folder" && visited.insert(entry.token.clone()) {
                let next_path = format!("{} / {}", current_path, entry.name);
                queue.push_back((entry.token.clone(), next_path));
            }
        }
    }
    Ok(results)
}

#[tauri::command]
fn inspect_local_path(path: String) -> Result<PathInspectResponse, String> {
    let meta = std::fs::metadata(&path).map_err(|e| e.to_string())?;
    Ok(PathInspectResponse {
        is_dir: meta.is_dir(),
        is_file: meta.is_file(),
    })
}

#[tauri::command]
fn reveal_local_path(path: String) -> Result<(), String> {
    if path.trim().is_empty() {
        return Err("路径不能为空".into());
    }
    let target_path = PathBuf::from(&path);
    if !target_path.exists() {
        return Err("路径不存在".into());
    }
    #[cfg(target_os = "macos")]
    {
        let mut cmd = Command::new("open");
        if target_path.is_file() {
            cmd.arg("-R").arg(&target_path);
        } else {
            cmd.arg(&target_path);
        }
        cmd.status().map_err(|e| e.to_string())?;
    }
    #[cfg(target_os = "windows")]
    {
        let mut cmd = Command::new("explorer");
        if target_path.is_file() {
            cmd.arg(format!("/select,\"{}\"", target_path.display()));
        } else {
            cmd.arg(target_path.display().to_string());
        }
        cmd.status().map_err(|e| e.to_string())?;
    }
    #[cfg(all(not(target_os = "macos"), not(target_os = "windows")))]
    {
        let dir = if target_path.is_file() {
            target_path
                .parent()
                .ok_or_else(|| "无法定位文件所在目录".to_string())?
                .to_path_buf()
        } else {
            target_path
        };
        Command::new("xdg-open")
            .arg(&dir)
            .status()
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

#[tauri::command]
async fn proxy_official_api(
    state: State<'_, AppState>,
    api_key: Option<String>,
    request: ProxyRequest,
) -> Result<Value, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    let tenant_id = if let Some(id) = request.tenant_id.clone() {
        state
            .assert_scope_for_tenant(&scope, &id)
            .map_err(|e| e.to_string())?;
        id
    } else if let Some(token) = &request.resource_token {
        state
            .assert_scope_for_token(&scope, token)
            .map_err(|e| e.to_string())?
    } else {
        state
            .select_active_tenant_for_scope(&scope)
            .map_err(|e| e.to_string())?
    };
    let tenant = state.ensure_token(&tenant_id).await.map_err(|e| e.to_string())?;
    state
        .forward_request(
            &tenant,
            &request.method,
            &request.path,
            Some(request.query.clone()),
            request.body.clone(),
        )
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn delete_file(
    state: State<'_, AppState>,
    api_key: Option<String>,
    payload: DeleteFilePayload,
) -> Result<Value, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    let tenant_id = state
        .assert_scope_for_token(&scope, &payload.token)
        .map_err(|e| e.to_string())?;
    let tenant = state.ensure_token(&tenant_id).await.map_err(|e| e.to_string())?;
    let path = format!("/open-apis/drive/v1/files/{}", payload.token);
    let resp = state
        .forward_request(
            &tenant,
            "DELETE",
            &path,
            Some(vec![("type".to_string(), payload.file_type.clone())]),
            None,
        )
        .await
        .map_err(|e| e.to_string())?;
    let _ = state.remove_resource(&payload.token);
    Ok(resp)
}

#[tauri::command]
async fn create_folder(
    state: State<'_, AppState>,
    api_key: Option<String>,
    payload: CreateFolderPayload,
) -> Result<CreateFolderResult, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    let folder_name = normalize_node_name(&payload.name).map_err(|e| e.to_string())?;
    let tenant_id = state
        .assert_scope_for_token(&scope, &payload.parent_token)
        .map_err(|e| e.to_string())?;
    let tenant = state.ensure_token(&tenant_id).await.map_err(|e| e.to_string())?;
    let resp = state
        .forward_request(
            &tenant,
            "POST",
            "/open-apis/drive/v1/files/create_folder",
            None,
            Some(serde_json::json!({
                "name": folder_name,
                "folder_token": payload.parent_token
            })),
        )
        .await
        .map_err(|e| e.to_string())?;
    let result = serde_json::from_value::<DriveApiResponse<CreateFolderResult>>(resp)
        .map_err(|e| e.to_string())?
        .into_data()
        .map_err(|e| e.to_string())?;
    state
        .register_resource(&tenant_id, result.token.clone())
        .map_err(|e| e.to_string())?;
    Ok(result)
}

#[tauri::command]
async fn upload_file(
    app: AppHandle,
    state: State<'_, AppState>,
    api_key: Option<String>,
    payload: UploadFilePayload,
) -> Result<String, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    let tenant_id = state
        .assert_scope_for_token(&scope, &payload.parent_token)
        .map_err(|e| e.to_string())?;
    let tenant = state.ensure_token(&tenant_id).await.map_err(|e| e.to_string())?;
    let path = PathBuf::from(&payload.file_path);
    let raw_name = if let Some(name) = payload.file_name.as_deref() {
        name.to_string()
    } else {
        path.file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| "无法解析文件名".to_string())?
            .to_string()
    };
    state
        .upload_local_file_path(
            &tenant_id,
            &tenant,
            &payload.parent_token,
            &path,
            &raw_name,
            None,
            Some(&app),
        )
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn upload_folder(
    app: AppHandle,
    state: State<'_, AppState>,
    api_key: Option<String>,
    payload: UploadFolderPayload,
) -> Result<(), String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    let tenant_id = state
        .assert_scope_for_token(&scope, &payload.parent_token)
        .map_err(|e| e.to_string())?;
    let tenant = state.ensure_token(&tenant_id).await.map_err(|e| e.to_string())?;
    let dir_path = PathBuf::from(&payload.dir_path);
    if !dir_path.is_dir() {
        return Err("选择的路径不是文件夹".into());
    }
    state
        .upload_directory_recursive(&tenant_id, &tenant, &payload.parent_token, &dir_path, Some(&app))
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn download_file(
    app: AppHandle,
    state: State<'_, AppState>,
    api_key: Option<String>,
    payload: DownloadFilePayload,
) -> Result<String, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    let tenant_id = state
        .assert_scope_for_token(&scope, &payload.token)
        .map_err(|e| e.to_string())?;
    let tenant = state.ensure_token(&tenant_id).await.map_err(|e| e.to_string())?;
    let dest_dir = PathBuf::from(&payload.dest_dir);
    state
        .download_drive_file(
            &tenant_id,
            &tenant,
            &payload.token,
            &dest_dir,
            &payload.file_name,
            None,
            Some(&app),
            payload.size,
        )
        .await
        .map(|path| path.to_string_lossy().to_string())
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn download_folder(
    app: AppHandle,
    state: State<'_, AppState>,
    api_key: Option<String>,
    payload: DownloadFolderPayload,
) -> Result<String, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    let tenant_id = state
        .assert_scope_for_token(&scope, &payload.token)
        .map_err(|e| e.to_string())?;
    let tenant = state.ensure_token(&tenant_id).await.map_err(|e| e.to_string())?;
    let sanitized = normalize_node_name(&payload.folder_name).map_err(|e| e.to_string())?;
    let mut target = PathBuf::from(&payload.dest_dir);
    target.push(&sanitized);
    state
        .download_drive_folder(&tenant_id, &tenant, &payload.token, &target, Some(&app))
        .await
        .map_err(|e| e.to_string())?;
    Ok(target.to_string_lossy().to_string())
}

#[tauri::command]
async fn move_file(
    state: State<'_, AppState>,
    api_key: Option<String>,
    payload: MoveFilePayload,
) -> Result<Option<String>, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    let tenant_id = state
        .assert_scope_for_token(&scope, &payload.token)
        .map_err(|e| e.to_string())?;
    let target_tenant = state
        .assert_scope_for_token(&scope, &payload.target_parent)
        .map_err(|e| e.to_string())?;
    if tenant_id != target_tenant {
        return Err("暂不支持跨企业移动文件".into());
    }
    let tenant = state.ensure_token(&tenant_id).await.map_err(|e| e.to_string())?;
    let resp = state
        .forward_request(
            &tenant,
            "POST",
            &format!("/open-apis/drive/v1/files/{}/move", payload.token),
            None,
            Some(serde_json::json!({
                "type": payload.file_type,
                "folder_token": payload.target_parent
            })),
        )
        .await
        .map_err(|e| e.to_string())?;
    let result = serde_json::from_value::<DriveApiResponse<MoveFileResult>>(resp)
        .map_err(|e| e.to_string())?
        .into_data()
        .map_err(|e| e.to_string())?;
    Ok(result.task_id)
}

#[tauri::command]
async fn copy_file(
    state: State<'_, AppState>,
    api_key: Option<String>,
    payload: CopyFilePayload,
) -> Result<DriveFileMeta, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    let tenant_id = state
        .assert_scope_for_token(&scope, &payload.token)
        .map_err(|e| e.to_string())?;
    let target_tenant = state
        .assert_scope_for_token(&scope, &payload.target_parent)
        .map_err(|e| e.to_string())?;
    if tenant_id != target_tenant {
        return Err("暂不支持跨企业复制".into());
    }
    let tenant = state.ensure_token(&tenant_id).await.map_err(|e| e.to_string())?;
    let copy_name = normalize_node_name(&payload.name).map_err(|e| e.to_string())?;
    let resp = state
        .forward_request(
            &tenant,
            "POST",
            &format!("/open-apis/drive/v1/files/{}/copy", payload.token),
            None,
            Some(serde_json::json!({
                "name": copy_name,
                "type": payload.file_type,
                "folder_token": payload.target_parent
            })),
        )
        .await
        .map_err(|e| e.to_string())?;
    let result = serde_json::from_value::<DriveApiResponse<CopyFileResult>>(resp)
        .map_err(|e| e.to_string())?
        .into_data()
        .map_err(|e| e.to_string())?;
    state
        .register_resource(&tenant_id, result.file.token.clone())
        .map_err(|e| e.to_string())?;
    Ok(result.file)
}

#[tauri::command]
async fn rename_file(
    state: State<'_, AppState>,
    api_key: Option<String>,
    payload: RenameFilePayload,
) -> Result<(), String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    let tenant_id = state
        .assert_scope_for_token(&scope, &payload.token)
        .map_err(|e| e.to_string())?;
    let tenant = state.ensure_token(&tenant_id).await.map_err(|e| e.to_string())?;
    let new_name = normalize_node_name(&payload.name).map_err(|e| e.to_string())?;
    rename_drive_entry(&state, &tenant, &payload, &new_name)
        .await
        .map_err(|e| e.to_string())
}

async fn rename_drive_entry(
    state: &AppState,
    tenant: &TenantConfig,
    payload: &RenameFilePayload,
    new_name: &str,
) -> AppResult<()> {
    let path = if payload.file_type.eq_ignore_ascii_case("folder") {
        format!("/open-apis/drive/explorer/v2/folder/{}", payload.token)
    } else {
        format!("/open-apis/drive/explorer/v2/file/{}", payload.token)
    };
    let mut body = serde_json::json!({ "name": new_name });
    if !payload.file_type.eq_ignore_ascii_case("folder") {
        body["type"] = serde_json::Value::String(payload.file_type.clone());
    }
    state
        .forward_request(
            tenant,
            "PATCH",
            &path,
            None,
            Some(body),
        )
        .await?;
    Ok(())
}

#[tauri::command]
async fn pick_files_dialog(payload: PickFilesPayload) -> Result<Vec<String>, String> {
    let multiple = payload.multiple;
    let result = tauri::async_runtime::spawn_blocking(move || {
        if multiple {
            FileDialog::new().pick_files()
        } else {
            FileDialog::new().pick_file().map(|p| vec![p])
        }
    })
    .await
    .map_err(|e| e.to_string())?;
    Ok(result
        .unwrap_or_default()
        .into_iter()
        .map(|p| p.to_string_lossy().to_string())
        .collect())
}

#[tauri::command]
async fn pick_directory_dialog() -> Result<Option<String>, String> {
    let result = tauri::async_runtime::spawn_blocking(|| FileDialog::new().pick_folder())
        .await
        .map_err(|e| e.to_string())?;
    Ok(result.map(|p| p.to_string_lossy().to_string()))
}

#[tauri::command]
async fn pick_entries_dialog(payload: PickEntriesPayload) -> Result<Vec<PickDialogEntry>, String> {
    tauri::async_runtime::spawn_blocking(move || pick_entries_blocking(payload.multiple))
        .await
        .map_err(|e| e.to_string())?
}

#[tauri::command]
async fn update_api_key(state: State<'_, AppState>, payload: UpdateKeyPayload) -> Result<(), String> {
    let scope = state
        .verify_api_key(payload.current_key.clone())
        .map_err(|e| e.to_string())?;
    AppState::ensure_admin(&scope).map_err(|e| e.to_string())?;
    state.set_api_key(payload.new_key).map_err(|e| e.to_string())?;
    Ok(())
}

#[tauri::command]
async fn get_api_key(state: State<'_, AppState>) -> Result<Option<String>, String> {
    Ok(state.api_key_plain.read().clone())
}

#[tauri::command]
async fn get_tenant_detail(
    state: State<'_, AppState>,
    api_key: Option<String>,
    tenant_id: String,
) -> Result<TenantDetail, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    AppState::ensure_admin(&scope).map_err(|e| e.to_string())?;
    state.get_tenant_detail(&tenant_id).map_err(|e| e.to_string())
}

#[tauri::command]
async fn update_tenant_meta(
    state: State<'_, AppState>,
    api_key: Option<String>,
    payload: UpdateTenantPayload,
) -> Result<TenantPublic, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    AppState::ensure_admin(&scope).map_err(|e| e.to_string())?;
    state.update_tenant_meta(payload).await.map_err(|e| e.to_string())
}

#[tauri::command]
async fn remove_tenant(
    state: State<'_, AppState>,
    api_key: Option<String>,
    payload: RemoveTenantPayload,
) -> Result<(), String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    AppState::ensure_admin(&scope).map_err(|e| e.to_string())?;
    state.remove_tenant(&payload.tenant_id).map_err(|e| e.to_string())
}

#[tauri::command]
async fn reorder_tenants(
    state: State<'_, AppState>,
    api_key: Option<String>,
    payload: Vec<ReorderTenant>,
) -> Result<(), String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    AppState::ensure_admin(&scope).map_err(|e| e.to_string())?;
    {
        let mut map = state.tenants.write();
        for item in payload {
            if let Some(tenant) = map.get_mut(&item.tenant_id) {
                tenant.order = item.order;
            }
        }
    }
    state.save().map_err(|e| e.to_string())
}

#[tauri::command]
async fn list_groups(
    state: State<'_, AppState>,
    api_key: Option<String>,
) -> Result<Vec<GroupPublic>, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    AppState::ensure_admin(&scope).map_err(|e| e.to_string())?;
    state.list_groups_snapshot().map_err(|e| e.to_string())
}

#[tauri::command]
async fn add_group(
    state: State<'_, AppState>,
    api_key: Option<String>,
    payload: GroupPayload,
) -> Result<GroupPublic, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    AppState::ensure_admin(&scope).map_err(|e| e.to_string())?;
    state.create_group(payload).map_err(|e| e.to_string())
}

#[tauri::command]
async fn update_group(
    state: State<'_, AppState>,
    api_key: Option<String>,
    payload: UpdateGroupPayload,
) -> Result<GroupPublic, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    AppState::ensure_admin(&scope).map_err(|e| e.to_string())?;
    state.update_group_meta(payload).map_err(|e| e.to_string())
}

#[tauri::command]
async fn delete_group(
    state: State<'_, AppState>,
    api_key: Option<String>,
    payload: RemoveGroupPayload,
) -> Result<(), String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    AppState::ensure_admin(&scope).map_err(|e| e.to_string())?;
    state.remove_group(&payload.group_id).map_err(|e| e.to_string())
}

#[tauri::command]
async fn regenerate_group_key(
    state: State<'_, AppState>,
    api_key: Option<String>,
    group_id: String,
) -> Result<GroupPublic, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    AppState::ensure_admin(&scope).map_err(|e| e.to_string())?;
    state.regenerate_group_key(&group_id).map_err(|e| e.to_string())
}

#[tauri::command]
async fn list_transfer_tasks(
    state: State<'_, AppState>,
    api_key: Option<String>,
) -> Result<Vec<TransferTaskRecord>, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    AppState::ensure_admin(&scope).map_err(|e| e.to_string())?;
    Ok(state.list_transfer_snapshots())
}

#[tauri::command]
async fn clear_transfer_history(
    state: State<'_, AppState>,
    api_key: Option<String>,
    mode: Option<String>,
) -> Result<usize, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    AppState::ensure_admin(&scope).map_err(|e| e.to_string())?;
    let removed = state
        .remove_transfer_tasks_by(|task| match mode.as_deref() {
            Some("success") => matches!(task.status, TransferStatus::Success),
            Some("failed") => matches!(task.status, TransferStatus::Failed),
            Some("finished") => matches!(task.status, TransferStatus::Success | TransferStatus::Failed),
            Some("all") | None => true,
            _ => false,
        })
        .map_err(|e| e.to_string())?;
    Ok(removed)
}

#[tauri::command]
async fn pause_active_transfer(
    app: AppHandle,
    state: State<'_, AppState>,
    api_key: Option<String>,
    task_id: String,
) -> Result<TransferTaskRecord, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    AppState::ensure_admin(&scope).map_err(|e| e.to_string())?;
    let control = state.ensure_transfer_control(&task_id);
    control.pause();
    state
        .update_transfer_task(
            &task_id,
            |task| {
                if matches!(task.status, TransferStatus::Running | TransferStatus::Pending) {
                    task.status = TransferStatus::Paused;
                }
            },
            Some(&app),
        )
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn cancel_transfer_task(
    app: AppHandle,
    state: State<'_, AppState>,
    api_key: Option<String>,
    task_id: String,
) -> Result<TransferTaskRecord, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    AppState::ensure_admin(&scope).map_err(|e| e.to_string())?;
    let control = state.ensure_transfer_control(&task_id);
    control.cancel();
    state
        .update_transfer_task(
            &task_id,
            |task| {
                task.status = TransferStatus::Failed;
                task.message = Some("任务已取消".into());
            },
            Some(&app),
        )
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn delete_transfer_task(
    state: State<'_, AppState>,
    api_key: Option<String>,
    task_id: String,
) -> Result<(), String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    AppState::ensure_admin(&scope).map_err(|e| e.to_string())?;
    state.delete_transfer_entry(&task_id).map_err(|e| e.to_string())
}

#[tauri::command]
async fn resume_transfer_task(
    app: AppHandle,
    state: State<'_, AppState>,
    api_key: Option<String>,
    task_id: String,
) -> Result<(), String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    AppState::ensure_admin(&scope).map_err(|e| e.to_string())?;
    if state.is_task_active(&task_id) {
        let control = state.ensure_transfer_control(&task_id);
        control.resume();
        state
            .update_transfer_task(
                &task_id,
                |task| {
                    task.status = TransferStatus::Running;
                    task.message = None;
                },
                Some(&app),
            )
            .map_err(|e| e.to_string())?;
        return Ok(());
    }
    let task = state.get_transfer_task(&task_id).map_err(|e| e.to_string())?;
    match task.kind {
        TransferKind::FileUpload => {
            let tenant_id = task
                .tenant_id
                .clone()
                .ok_or_else(|| "任务缺少企业实例信息".to_string())?;
            let parent_token = task
                .parent_token
                .clone()
                .ok_or_else(|| "任务缺少目标目录".to_string())?;
            let local_path = task
                .local_path
                .clone()
                .ok_or_else(|| "任务缺少本地路径".to_string())?;
            let tenant = state.ensure_token(&tenant_id).await.map_err(|e| e.to_string())?;
            let path_buf = PathBuf::from(&local_path);
            let file_label = task.name.clone();
            let resume_task = task.clone();
            state
                .upload_local_file_path(
                    &tenant_id,
                    &tenant,
                    &parent_token,
                    &path_buf,
                    &file_label,
                    Some(resume_task),
                    Some(&app),
                )
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        }
        TransferKind::FileDownload => {
            let tenant_id = task
                .tenant_id
                .clone()
                .ok_or_else(|| "任务缺少企业实例信息".to_string())?;
            let local_path = task
                .local_path
                .clone()
                .ok_or_else(|| "任务缺少下载目标路径".to_string())?;
            let dest_dir = PathBuf::from(&local_path)
                .parent()
                .map(|p| p.to_path_buf())
                .ok_or_else(|| "无法解析下载目录".to_string())?;
            let tenant = state.ensure_token(&tenant_id).await.map_err(|e| e.to_string())?;
            let file_name = PathBuf::from(&local_path)
                .file_name()
                .and_then(|n| n.to_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| task.name.clone());
            let file_token = task
                .resource_token
                .clone()
                .ok_or_else(|| "任务缺少文件 token".to_string())?;
            let resume_task = task.clone();
            state
                .download_drive_file(
                    &tenant_id,
                    &tenant,
                    &file_token,
                    &dest_dir,
                    &file_name,
                    Some(resume_task),
                    Some(&app),
                    Some(task.size),
                )
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        }
        _ => Err("暂不支持重新执行该类型任务".into()),
    }
}

#[cfg(target_os = "macos")]
fn pick_entries_blocking(multiple: bool) -> Result<Vec<PickDialogEntry>, String> {
    run_on_main(move || unsafe {
        autoreleasepool(|| {
            let panel: *mut Object = msg_send![class!(NSOpenPanel), openPanel];
            let allow_multi = if multiple { YES } else { NO };
            let _: () = msg_send![panel, setCanChooseFiles: YES];
            let _: () = msg_send![panel, setCanChooseDirectories: YES];
            let _: () = msg_send![panel, setAllowsMultipleSelection: allow_multi];
            let _: () = msg_send![panel, setCanCreateDirectories: YES];
            let response: i64 = msg_send![panel, runModal];
            const NS_MODAL_RESPONSE_OK: i64 = 1;
            if response != NS_MODAL_RESPONSE_OK {
                return Ok(Vec::new());
            }
            let urls: *mut Object = msg_send![panel, URLs];
            let count: usize = msg_send![urls, count];
            let mut entries = Vec::with_capacity(count);
            for index in 0..count {
                let url: *mut Object = msg_send![urls, objectAtIndex: index];
                let ns_path: *mut Object = msg_send![url, path];
                if ns_path.is_null() {
                    continue;
                }
                let c_str: *const c_char = msg_send![ns_path, UTF8String];
                if c_str.is_null() {
                    continue;
                }
                let path = CStr::from_ptr(c_str).to_string_lossy().into_owned();
                if path.is_empty() {
                    continue;
                }
                let kind = if Path::new(&path).is_dir() {
                    PickEntryKind::Folder
                } else {
                    PickEntryKind::File
                };
                entries.push(PickDialogEntry {
                    path,
                    entry_type: kind,
                });
            }
            Ok(entries)
        })
    })
}

#[cfg(target_os = "macos")]
fn run_on_main<R: Send + 'static, F: FnOnce() -> R + Send + 'static>(run: F) -> R {
    unsafe {
        let is_main: bool = msg_send![class!(NSThread), isMainThread];
        if is_main {
            run()
        } else {
            Queue::main().exec_sync(run)
        }
    }
}

#[cfg(not(target_os = "macos"))]
fn pick_entries_blocking(_multiple: bool) -> Result<Vec<PickDialogEntry>, String> {
    Err("当前平台暂不支持统一文件/文件夹选择".into())
}

fn build_url(base: &str, path: &str, query: Option<Vec<(String, String)>>) -> AppResult<Url> {
    let mut url = Url::parse(&format!("{}{}", base, path)).map_err(|e| AppError::Message(e.to_string()))?;
    if let Some(pairs) = query {
        {
            let mut qp = url.query_pairs_mut();
            qp.clear();
            for (k, v) in pairs {
                qp.append_pair(&k, &v);
            }
        }
    }
    Ok(url)
}

fn normalize_node_name(raw: &str) -> AppResult<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AppError::Message("名称不能为空".into()));
    }
    if trimmed.contains('/') || trimmed.contains('\\') {
        return Err(AppError::Message("名称不能包含路径分隔符".into()));
    }
    Ok(trimmed.to_string())
}

fn adler32_checksum(data: &[u8]) -> u32 {
    const MOD_ADLER: u32 = 65521;
    let mut a: u32 = 1;
    let mut b: u32 = 0;
    for chunk in data.chunks(5552) {
        for &byte in chunk {
            a = (a + byte as u32) % MOD_ADLER;
            b = (b + a) % MOD_ADLER;
        }
    }
    (b << 16) | a
}

fn show_main_window(app: &AppHandle) {
    if let Some(window) = app.get_webview_window("main") {
        let _ = window.show();
        let _ = window.set_focus();
    }
}

fn hide_main_window(app: &AppHandle) {
    if let Some(window) = app.get_webview_window("main") {
        let _ = window.hide();
    }
}

fn toggle_main_window(app: &AppHandle) {
    if let Some(window) = app.get_webview_window("main") {
        let is_visible = window.is_visible().unwrap_or(false);
        if is_visible {
            let _ = window.hide();
        } else {
            let _ = window.show();
            let _ = window.set_focus();
        }
    }
}

#[cfg(desktop)]
fn setup_tray(app: &AppHandle) -> tauri::Result<()> {
    let show_item = MenuItemBuilder::with_id(TRAY_MENU_SHOW, "显示窗口").build(app)?;
    let hide_item = MenuItemBuilder::with_id(TRAY_MENU_HIDE, "隐藏窗口").build(app)?;
    let quit_item = MenuItemBuilder::with_id(TRAY_MENU_QUIT, "退出 FeiSync").build(app)?;
    let menu = MenuBuilder::new(app)
        .items(&[&show_item, &hide_item])
        .separator()
        .item(&quit_item)
        .build()?;
    if let Some(tray) = app.tray_by_id("main") {
        tray.set_menu(Some(menu))?;
        tray.set_tooltip(Some("FeiSync"))?;
    }
    Ok(())
}

fn main() {
    tauri::Builder::default()
        .setup(|app| {
            app.manage(AppState::new());
            #[cfg(target_os = "macos")]
            {
                app.set_activation_policy(ActivationPolicy::Accessory);
            }
            #[cfg(desktop)]
            {
                let handle = app.handle();
                setup_tray(&handle)?;
                handle.on_menu_event(|app, event| match event.id().as_ref() {
                    TRAY_MENU_SHOW => show_main_window(app),
                    TRAY_MENU_HIDE => hide_main_window(app),
                    TRAY_MENU_QUIT => app.exit(0),
                    _ => {}
                });
                handle.on_tray_icon_event(|app, event| {
                    if let TrayIconEvent::Click { button, .. } = event {
                        if button == MouseButton::Left {
                            toggle_main_window(app);
                        }
                    }
                });
                show_main_window(&handle);
            }
            Ok(())
        })
        .on_window_event(|window, event| {
            if let WindowEvent::CloseRequested { api, .. } = event {
                api.prevent_close();
                let app_handle = window.app_handle();
                hide_main_window(&app_handle);
            }
        })
        .invoke_handler(tauri::generate_handler![
            list_tenants,
            add_tenant,
            refresh_tenant_token,
            list_root_entries,
            list_folder_entries,
            search_entries,
            delete_file,
            create_folder,
            upload_file,
            upload_folder,
            download_file,
            download_folder,
            move_file,
            copy_file,
            rename_file,
            inspect_local_path,
            reveal_local_path,
            pick_files_dialog,
            pick_directory_dialog,
            pick_entries_dialog,
            get_api_key,
            get_tenant_detail,
            update_api_key,
            update_tenant_meta,
            remove_tenant,
            reorder_tenants,
            list_groups,
            add_group,
            update_group,
            delete_group,
            regenerate_group_key,
            list_transfer_tasks,
            clear_transfer_history,
            pause_active_transfer,
            cancel_transfer_task,
            delete_transfer_task,
            resume_transfer_task,
            proxy_official_api
        ])
        .run(tauri::generate_context!())
        .expect("error running FeiSync");
}
