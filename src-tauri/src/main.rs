#![allow(unexpected_cfgs)]

use axum::{
    extract::{Path as AxumPath, State as AxumState},
    http::{HeaderMap, StatusCode as AxumStatusCode},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Duration, Utc};
#[cfg(target_os = "macos")]
use dispatch::Queue;
#[cfg(target_os = "macos")]
use tauri::ActivationPolicy;
use parking_lot::RwLock;
use reqwest::{multipart, Client, StatusCode as HttpStatus, Url};
use rfd::FileDialog;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    env,
    fs,
    io::SeekFrom,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::SystemTime,
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
    net::TcpListener,
    sync::{oneshot, Notify},
    task::spawn_blocking,
    time::{timeout, Duration as TokioDuration},
};
use tower_http::cors::{Any, CorsLayer};
use uuid::Uuid;
use walkdir::WalkDir;
use wildmatch::WildMatch;
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
const SYNC_TASK_FILE: &str = "feisync.sync_tasks.json";
const SYNC_LOG_FILE: &str = "feisync.sync_logs.json";

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
    #[error(transparent)]
    Walkdir(#[from] walkdir::Error),
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

fn log_api_error(label: &str, status: HttpStatus, body: &str) {
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

fn api_error(label: &str, status: HttpStatus, body: &str) -> AppError {
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

const API_SERVER_FILE: &str = "feisync.api_server.json";
const DEFAULT_API_PORT: u16 = 6688;
const DEFAULT_API_TIMEOUT: u64 = 120;

#[derive(Clone, Serialize, Deserialize)]
struct ApiServerConfig {
    listen_host: String,
    port: u16,
    timeout_secs: u64,
}

impl Default for ApiServerConfig {
    fn default() -> Self {
        ApiServerConfig {
            listen_host: "0.0.0.0".into(),
            port: DEFAULT_API_PORT,
            timeout_secs: DEFAULT_API_TIMEOUT,
        }
    }
}

struct ApiServerRuntime {
    addr: SocketAddr,
    shutdown: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

#[derive(Clone)]
struct ApiRouterState {
    app: AppHandle,
    timeout: TokioDuration,
}

#[derive(Serialize, Deserialize, Clone)]
struct ApiServerStatus {
    running: bool,
    address: Option<String>,
    config: ApiServerConfig,
}

#[derive(Deserialize)]
struct ApiCommandBody {
    #[serde(default)]
    payload: Option<Value>,
    #[serde(default)]
    api_key: Option<String>,
}

#[derive(Serialize)]
struct ApiDocEntry {
    command: String,
    method: String,
    path: String,
    description: String,
    payload: String,
    response: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    notes: Option<String>,
    payload_fields: Vec<ApiFieldDoc>,
    response_fields: Vec<ApiFieldDoc>,
}

#[derive(Clone, Serialize)]
struct ApiFieldDoc {
    name: &'static str,
    typ: &'static str,
    required: bool,
    description: &'static str,
}

struct ApiDocStatic {
    command: &'static str,
    description: &'static str,
    payload: &'static str,
    response: &'static str,
    notes: Option<&'static str>,
    payload_fields: &'static [ApiFieldDoc],
    response_fields: &'static [ApiFieldDoc],
}

const fn field(
    name: &'static str,
    typ: &'static str,
    required: bool,
    description: &'static str,
) -> ApiFieldDoc {
    ApiFieldDoc {
        name,
        typ,
        required,
        description,
    }
}

const NO_BODY_FIELDS: &[ApiFieldDoc] = &[field("-", "-", false, "无需请求体")];
const GENERIC_RESULT_FIELDS: &[ApiFieldDoc] = &[field("data", "object", false, "返回数据结构，参考示例")];

const API_DOCS: &[ApiDocStatic] = &[
    ApiDocStatic {
        command: "list_tenants",
        description: "列出全部企业实例。",
        payload: "{}",
        response: r#"{"data":[{"id":"tenant_id","name":"企业名称","quota_gb":100,"used_gb":23.2,"active":true,"platform":"feishu"}]}"#,
        notes: Some("需要管理员级 API Key。"),
        payload_fields: NO_BODY_FIELDS,
        response_fields: &[
            field("data[].id", "string", false, "企业实例 ID"),
            field("data[].name", "string", false, "企业名称"),
            field("data[].quota_gb", "number", false, "配额 (GB)"),
            field("data[].used_gb", "number", false, "已用容量 (GB)"),
            field("data[].platform", "string", false, "实例接入的云平台"),
        ],
    },
    ApiDocStatic {
        command: "add_tenant",
        description: "新增企业实例。",
        payload: r#"{"payload":{"name":"企业名称","app_id":"cli_xxx","app_secret":"xxx","quota_gb":100,"platform":"feishu"}}"#,
        response: r#"{"data":{"id":"tenant_id","name":"企业名称",...}}"#,
        notes: Some("app_secret 可选，若缺失需要后续补充。"),
        payload_fields: &[
            field("payload.name", "string", true, "企业显示名称"),
            field("payload.app_id", "string", true, "飞书/企业互联应用 app_id"),
            field("payload.app_secret", "string", false, "飞书 app_secret"),
            field("payload.quota_gb", "number", true, "空间配额 (GB)"),
            field("payload.platform", "string", false, "接入平台，feishu 或 lark"),
        ],
        response_fields: &[
            field("data.id", "string", true, "创建后的企业实例 ID"),
            field("data.name", "string", true, "企业名称"),
        ],
    },
    ApiDocStatic {
        command: "refresh_tenant_token",
        description: "强制刷新租户访问令牌。",
        payload: r#"{"payload":{"tenant_id":"tenant_id"}}"#,
        response: r#"{"data":{"tenant_access_token":"****","expire":7200}}"#,
        notes: Some("若应用权限或凭证变动需要刷新。"),
        payload_fields: &[
            field("payload.tenant_id", "string", true, "目标企业实例 ID"),
        ],
        response_fields: &[
            field("data.tenant_access_token", "string", true, "新的访问令牌"),
            field("data.expire", "number", true, "令牌有效期（秒）"),
        ],
    },
    ApiDocStatic {
        command: "list_root_entries",
        description: "列出租户根目录或聚合的根目录列表。",
        payload: r#"{"payload":{"tenant_id":"tenant_id","aggregate":false}}"#,
        response: r#"{"data":{"rootToken":"fld_xxx","entries":[{"token":"fld_xxx","name":"文件夹","type":"folder","path":null,"tenant_name":"企业A"}]}}"#,
        notes: Some("aggregate=true 时返回 {\"aggregate\":true,\"entries\":{\"tenantId\":[...]}}。"),
        payload_fields: &[
            field("payload.tenant_id", "string", false, "指定租户 ID，缺省时自动选择"),
            field("payload.aggregate", "bool", false, "是否聚合全部租户根目录"),
        ],
        response_fields: &[
            field("data.rootToken", "string", false, "当前根目录 token"),
            field("data.entries[]", "array", false, "根目录下的文件/文件夹列表"),
        ],
    },
    ApiDocStatic {
        command: "list_folder_entries",
        description: "列出指定文件夹下的节点。",
        payload: r#"{"payload":{"folder_token":"fld_xxx"}}"#,
        response: r#"{"data":[{"token":"doc_xxx","name":"文档","type":"doc","parent_token":"fld_xxx","update_time":"2024-01-01T10:00:00Z"}]}"#,
        notes: None,
        payload_fields: &[
            field("payload.folder_token", "string", true, "目标文件夹 token"),
        ],
        response_fields: &[
            field("data[].token", "string", true, "条目 token"),
            field("data[].type", "string", true, "条目类型（file/doc/folder 等）"),
            field("data[].update_time", "string", false, "更新时间 (ISO8601)"),
        ],
    },
    ApiDocStatic {
        command: "search_entries",
        description: "从指定租户根目录向下模糊搜索文件。",
        payload: r#"{"payload":{"keyword":"合同","tenant_id":"tenant_id","root_name":"Root"}}"#,
        response: r#"{"data":[{"token":"doc_xxx","name":"合同.docx","path":"Root / 合同.docx"}]}"#,
        notes: Some("keyword 为必填，tenant_id 为空时自动选择当前租户。"),
        payload_fields: &[
            field("payload.keyword", "string", true, "搜索关键字"),
            field("payload.tenant_id", "string", false, "指定租户"),
            field("payload.root_name", "string", false, "根目录显示名"),
        ],
        response_fields: &[
            field("data[].path", "string", false, "命中文件的完整路径"),
            field("data[].tenant_name", "string", false, "所属租户"),
        ],
    },
    ApiDocStatic {
        command: "delete_file",
        description: "删除云端文件或文件夹。",
        payload: r#"{"payload":{"token":"doc_xxx","type":"file"}}"#,
        response: r#"{"data":{"code":0}}"#,
        notes: Some("type 取值 file/folder。"),
        payload_fields: &[
            field("payload.token", "string", true, "文件/文件夹 token"),
            field("payload.type", "string", true, "类型（file/folder）"),
        ],
        response_fields: GENERIC_RESULT_FIELDS,
    },
    ApiDocStatic {
        command: "create_folder",
        description: "在指定目录下创建新文件夹。",
        payload: r#"{"payload":{"parent_token":"fld_parent","name":"子文件夹"}}"#,
        response: r#"{"data":{"token":"fld_new","url":null}}"#,
        notes: None,
        payload_fields: &[
            field("payload.parent_token", "string", true, "目标父目录 token"),
            field("payload.name", "string", true, "新建的文件夹名称"),
        ],
        response_fields: &[
            field("data.token", "string", true, "新建文件夹 token"),
            field("data.url", "string", false, "可选的网页版链接"),
        ],
    },
    ApiDocStatic {
        command: "upload_file",
        description: "上传本地文件到云端目录。",
        payload: r#"{"payload":{"parent_token":"fld_parent","file_path":"/path/to/file.docx","file_name":"可选新名称"}}"#,
        response: r#"{"data":"file_token"}"#,
        notes: Some("file_path 必须是本地可访问的文件路径。"),
        payload_fields: &[
            field("payload.parent_token", "string", true, "上传目标目录 token"),
            field("payload.file_path", "string", true, "本地文件绝对路径"),
            field("payload.file_name", "string", false, "云端保存名称"),
        ],
        response_fields: &[
            field("data", "string", true, "上传成功后的文件 token"),
        ],
    },
    ApiDocStatic {
        command: "upload_folder",
        description: "递归上传本地文件夹到云端目录。",
        payload: r#"{"payload":{"parent_token":"fld_parent","dir_path":"/path/to/folder"}}"#,
        response: r#"{"data":null}"#,
        notes: Some("文件夹内所有子文件都会排队上传。"),
        payload_fields: &[
            field("payload.parent_token", "string", true, "上传目标目录 token"),
            field("payload.dir_path", "string", true, "本地文件夹路径"),
        ],
        response_fields: GENERIC_RESULT_FIELDS,
    },
    ApiDocStatic {
        command: "download_file",
        description: "下载云端文件到本地目录。",
        payload: r#"{"payload":{"token":"doc_xxx","dest_dir":"/tmp/downloads","file_name":"保存名","size":12345}}"#,
        response: r#"{"data":"/tmp/downloads/保存名"}"#,
        notes: Some("dest_dir 需存在写权限。"),
        payload_fields: &[
            field("payload.token", "string", true, "云端文件 token"),
            field("payload.dest_dir", "string", true, "本地保存目录"),
            field("payload.file_name", "string", true, "保存时的文件名"),
            field("payload.size", "number", false, "可选的文件大小"),
        ],
        response_fields: &[
            field("data", "string", true, "实际保存路径"),
        ],
    },
    ApiDocStatic {
        command: "download_folder",
        description: "递归下载云端文件夹到本地。",
        payload: r#"{"payload":{"token":"fld_xxx","dest_dir":"/tmp","folder_name":"拷贝目录名"}}"#,
        response: r#"{"data":"/tmp/拷贝目录名"}"#,
        notes: None,
        payload_fields: &[
            field("payload.token", "string", true, "云端文件夹 token"),
            field("payload.dest_dir", "string", true, "本地目的目录"),
            field("payload.folder_name", "string", true, "保存的文件夹名称"),
        ],
        response_fields: &[
            field("data", "string", true, "最终生成的本地目录"),
        ],
    },
    ApiDocStatic {
        command: "move_file",
        description: "移动云端文件或文件夹到新父目录。",
        payload: r#"{"payload":{"token":"doc_xxx","type":"file","target_parent":"fld_target"}}"#,
        response: r#"{"data":{"task_id":null}}"#,
        notes: Some("仅支持同一租户内移动。"),
        payload_fields: &[
            field("payload.token", "string", true, "文件或文件夹 token"),
            field("payload.type", "string", true, "类型（file/folder/doc 等）"),
            field("payload.target_parent", "string", true, "目标父目录 token"),
        ],
        response_fields: &[
            field("data.task_id", "string", false, "异步任务 ID，部分情况下返回 null"),
        ],
    },
    ApiDocStatic {
        command: "copy_file",
        description: "复制云端文件/文件夹。",
        payload: r#"{"payload":{"token":"doc_xxx","type":"file","target_parent":"fld_target","name":"副本名称"}}"#,
        response: r#"{"data":{"token":"doc_copy","name":"副本名称"}} "#,
        notes: None,
        payload_fields: &[
            field("payload.token", "string", true, "源文件 token"),
            field("payload.type", "string", true, "源类型"),
            field("payload.target_parent", "string", true, "目标父目录 token"),
            field("payload.name", "string", true, "复制后的文件名"),
        ],
        response_fields: &[
            field("data.token", "string", true, "新文件 token"),
            field("data.name", "string", true, "新文件名称"),
        ],
    },
    ApiDocStatic {
        command: "rename_file",
        description: "重命名云端文件或文件夹。",
        payload: r#"{"payload":{"token":"doc_xxx","type":"file","name":"新名称"}}"#,
        response: r#"{"data":null}"#,
        notes: None,
        payload_fields: &[
            field("payload.token", "string", true, "文件/文件夹 token"),
            field("payload.type", "string", true, "类型"),
            field("payload.name", "string", true, "新的显示名称"),
        ],
        response_fields: GENERIC_RESULT_FIELDS,
    },
    ApiDocStatic {
        command: "list_sync_tasks",
        description: "列出同步任务。",
        payload: "{}",
        response: r#"{"data":[{"id":"task_id","name":"任务","direction":"bidirectional","group_id":"grp_x","local_path":"/data",...}]}"#,
        notes: None,
        payload_fields: NO_BODY_FIELDS,
        response_fields: &[
            field("data[].id", "string", true, "任务 ID"),
            field("data[].direction", "string", true, "同步方向"),
            field("data[].local_path", "string", true, "本地目录"),
        ],
    },
    ApiDocStatic {
        command: "create_sync_task",
        description: "创建同步任务。",
        payload: r#"{"payload":{"name":"任务","direction":"local_to_cloud","group_id":"grp_x","tenant_id":"tenant_x","remote_folder_token":"fld_x","remote_label":"企业A / 资料","local_path":"/Users/demo","schedule":"0 * * * *","enabled":true,"detection":"checksum","conflict":"newest","propagate_delete":true,"include_patterns":["**/*"],"exclude_patterns":[]}}"#,
        response: r#"{"data":{"id":"task_id",...}}"#,
        notes: Some("include/exclude 使用 glob 语法。"),
        payload_fields: &[
            field("payload.name", "string", true, "任务名称"),
            field("payload.direction", "string", true, "同步方向 (local_to_cloud/cloud_to_local/bidirectional)"),
            field("payload.group_id", "string", true, "企业分组 ID"),
            field("payload.tenant_id", "string", true, "云端租户 ID"),
            field("payload.remote_folder_token", "string", true, "云端根目录 token"),
            field("payload.local_path", "string", true, "本地目录"),
            field("payload.schedule", "string", true, "Cron 表达式"),
            field("payload.propagate_delete", "bool", true, "是否同步删除"),
        ],
        response_fields: &[
            field("data.id", "string", true, "任务 ID"),
            field("data.last_status", "string", false, "最近运行状态"),
        ],
    },
    ApiDocStatic {
        command: "update_sync_task",
        description: "更新任务配置。",
        payload: r#"{"payload":{"task_id":"task_id","local_path":"/new/path","enabled":false}}"#,
        response: r#"{"data":{"id":"task_id",...}}"#,
        notes: Some("修改目录会重置快照。"),
        payload_fields: &[
            field("payload.task_id", "string", true, "目标任务 ID"),
            field("payload.local_path", "string", false, "新的本地路径"),
            field("payload.enabled", "bool", false, "是否启用"),
            field("payload.remote_folder_token", "string", false, "新的云端目录 token"),
        ],
        response_fields: &[
            field("data.id", "string", true, "任务 ID"),
            field("data.updated_at", "string", false, "更新时间"),
        ],
    },
    ApiDocStatic {
        command: "delete_sync_task",
        description: "删除任务。",
        payload: r#"{"payload":{"task_id":"task_id"}}"#,
        response: r#"{"data":null}"#,
        notes: None,
        payload_fields: &[
            field("payload.task_id", "string", true, "任务 ID"),
        ],
        response_fields: GENERIC_RESULT_FIELDS,
    },
    ApiDocStatic {
        command: "trigger_sync_task",
        description: "立即执行同步任务。",
        payload: r#"{"payload":{"task_id":"task_id"}}"#,
        response: r#"{"data":{"id":"task_id","last_status":"success",...}}"#,
        notes: Some("任务执行完成后返回最新任务快照。"),
        payload_fields: &[
            field("payload.task_id", "string", true, "任务 ID"),
        ],
        response_fields: &[
            field("data.last_status", "string", true, "执行结果"),
            field("data.last_message", "string", false, "结果描述"),
        ],
    },
    ApiDocStatic {
        command: "list_sync_logs",
        description: "查询任务日志。",
        payload: r#"{"payload":{"task_id":"task_id","limit":100}}"#,
        response: r#"{"data":[{"timestamp":"2024-01-01T10:00:00Z","level":"info","message":"扫描本地目录"}]}"#,
        notes: None,
        payload_fields: &[
            field("payload.task_id", "string", true, "任务 ID"),
            field("payload.limit", "number", false, "返回记录条数 (默认 100)"),
        ],
        response_fields: &[
            field("data[].timestamp", "string", true, "日志时间"),
            field("data[].level", "string", true, "日志级别 info/warn/error"),
            field("data[].message", "string", true, "日志内容"),
        ],
    },
    ApiDocStatic {
        command: "inspect_local_path",
        description: "检测本地路径属性。",
        payload: r#"{"payload":{"path":"/Users/demo"}} "#,
        response: r#"{"data":{"is_dir":true,"is_file":false}}"#,
        notes: Some("仅在本机可用。"),
        payload_fields: &[
            field("payload.path", "string", true, "本地路径"),
        ],
        response_fields: &[
            field("data.is_dir", "bool", true, "是否为目录"),
            field("data.is_file", "bool", true, "是否为文件"),
        ],
    },
    ApiDocStatic {
        command: "reveal_local_path",
        description: "在系统中打开指定路径。",
        payload: r#"{"payload":{"path":"/Users/demo/report.docx"}}"#,
        response: r#"{"data":null}"#,
        notes: Some("macOS 使用 open，Windows 使用 explorer。"),
        payload_fields: &[
            field("payload.path", "string", true, "需要打开的路径"),
        ],
        response_fields: GENERIC_RESULT_FIELDS,
    },
    ApiDocStatic {
        command: "get_api_key",
        description: "读取管理 API Key（仅限本机 UI 调用）。",
        payload: "{}",
        response: r#"{"data":"current_key" }"#,
        notes: Some("HTTP API 调用该命令需在本机环境。"),
        payload_fields: NO_BODY_FIELDS,
        response_fields: &[
            field("data", "string", true, "当前管理密钥，可能为 null"),
        ],
    },
    ApiDocStatic {
        command: "update_api_key",
        description: "更新管理 API Key。",
        payload: r#"{"payload":{"currentKey":"旧 key 或 null","newKey":"新 key"}}"#,
        response: r#"{"data":null}"#,
        notes: Some("设置后需重新附带新的 X-API-Key。"),
        payload_fields: &[
            field("payload.currentKey", "string", false, "原有密钥，没有填 null"),
            field("payload.newKey", "string", true, "新密钥"),
        ],
        response_fields: GENERIC_RESULT_FIELDS,
    },
    ApiDocStatic {
        command: "get_tenant_detail",
        description: "获取企业实例详细信息。",
        payload: r#"{"payload":{"tenant_id":"tenant_id"}}"#,
        response: r#"{"data":{"id":"tenant_id","app_id":"cli_xxx","quota_gb":100,...}}"#,
        notes: None,
        payload_fields: &[
            field("payload.tenant_id", "string", true, "企业实例 ID"),
        ],
        response_fields: &[
            field("data.app_id", "string", true, "飞书应用 app_id"),
            field("data.quota_gb", "number", true, "当前配额"),
            field("data.active", "bool", true, "是否启用"),
        ],
    },
    ApiDocStatic {
        command: "update_tenant_meta",
        description: "更新企业实例信息。",
        payload: r#"{"payload":{"tenant_id":"tenant_id","name":"新名称","quota_gb":200,"active":true}}"#,
        response: r#"{"data":{"id":"tenant_id","name":"新名称",...}}"#,
        notes: Some("修改 app_id/app_secret 会触发 token 刷新。"),
        payload_fields: &[
            field("payload.tenant_id", "string", true, "企业实例 ID"),
            field("payload.name", "string", false, "企业名称"),
            field("payload.quota_gb", "number", false, "配额"),
            field("payload.active", "bool", false, "是否启用"),
            field("payload.app_id", "string", false, "新 app_id"),
            field("payload.app_secret", "string", false, "新 app_secret"),
        ],
        response_fields: &[
            field("data.id", "string", true, "企业实例 ID"),
            field("data.name", "string", true, "企业名称"),
        ],
    },
    ApiDocStatic {
        command: "remove_tenant",
        description: "删除企业实例。",
        payload: r#"{"payload":{"tenant_id":"tenant_id"}}"#,
        response: r#"{"data":null}"#,
        notes: Some("同时会从所属分组移除。"),
        payload_fields: &[
            field("payload.tenant_id", "string", true, "企业实例 ID"),
        ],
        response_fields: GENERIC_RESULT_FIELDS,
    },
    ApiDocStatic {
        command: "reorder_tenants",
        description: "批量更新企业实例排序。",
        payload: r#"{"payload":[{"tenant_id":"tenant_a","order":1},{"tenant_id":"tenant_b","order":2}]}"#,
        response: r#"{"data":null}"#,
        notes: None,
        payload_fields: &[
            field("payload[].tenant_id", "string", true, "企业实例 ID"),
            field("payload[].order", "number", true, "排序值，越小越靠前"),
        ],
        response_fields: GENERIC_RESULT_FIELDS,
    },
    ApiDocStatic {
        command: "list_groups",
        description: "列出企业分组与分组 API Key。",
        payload: "{}",
        response: r#"{"data":[{"id":"grp_x","name":"研发组","tenant_ids":["tenant_a"],"api_key":"grp_key"}]}"#,
        notes: None,
        payload_fields: NO_BODY_FIELDS,
        response_fields: &[
            field("data[].id", "string", true, "分组 ID"),
            field("data[].name", "string", true, "分组名称"),
            field("data[].tenant_ids[]", "string", false, "所属企业实例"),
            field("data[].api_key", "string", true, "分组 API Key"),
        ],
    },
    ApiDocStatic {
        command: "add_group",
        description: "新增企业分组并生成 API Key。",
        payload: r#"{"payload":{"name":"新分组","remark":"说明","tenant_ids":["tenant_a","tenant_b"]}}"#,
        response: r#"{"data":{"id":"grp_new","api_key":"****"}} "#,
        notes: None,
        payload_fields: &[
            field("payload.name", "string", true, "分组名称"),
            field("payload.remark", "string", false, "备注"),
            field("payload.tenant_ids[]", "string", false, "包含的企业实例"),
        ],
        response_fields: &[
            field("data.id", "string", true, "分组 ID"),
            field("data.api_key", "string", true, "新生成的分组密钥"),
        ],
    },
    ApiDocStatic {
        command: "update_group",
        description: "更新分组信息。",
        payload: r#"{"payload":{"group_id":"grp_x","name":"新名称","tenant_ids":["tenant_a"]}}"#,
        response: r#"{"data":{"id":"grp_x","name":"新名称","tenant_ids":["tenant_a"],"api_key":"****"}}"#,
        notes: None,
        payload_fields: &[
            field("payload.group_id", "string", true, "分组 ID"),
            field("payload.name", "string", false, "分组名称"),
            field("payload.remark", "string", false, "备注"),
            field("payload.tenant_ids[]", "string", false, "企业实例列表"),
        ],
        response_fields: &[
            field("data.id", "string", true, "分组 ID"),
            field("data.tenant_ids[]", "string", false, "最新的企业列表"),
        ],
    },
    ApiDocStatic {
        command: "delete_group",
        description: "删除分组。",
        payload: r#"{"payload":{"group_id":"grp_x"}}"#,
        response: r#"{"data":null}"#,
        notes: Some("删除后该分组 API Key 失效。"),
        payload_fields: &[
            field("payload.group_id", "string", true, "分组 ID"),
        ],
        response_fields: GENERIC_RESULT_FIELDS,
    },
    ApiDocStatic {
        command: "regenerate_group_key",
        description: "重置分组 API Key。",
        payload: r#"{"payload":{"group_id":"grp_x"}}"#,
        response: r#"{"data":{"id":"grp_x","api_key":"new_key"}} "#,
        notes: Some("客户端需更新携带的分组 Key。"),
        payload_fields: &[
            field("payload.group_id", "string", true, "分组 ID"),
        ],
        response_fields: &[
            field("data.api_key", "string", true, "新的分组密钥"),
        ],
    },
    ApiDocStatic {
        command: "list_transfer_tasks",
        description: "列出传输任务列表。",
        payload: "{}",
        response: r#"{"data":[{"id":"task","direction":"upload","status":"running","local_path":"/tmp/a"}]}"#,
        notes: None,
        payload_fields: NO_BODY_FIELDS,
        response_fields: &[
            field("data[].id", "string", true, "传输任务 ID"),
            field("data[].direction", "string", true, "传输方向"),
            field("data[].status", "string", true, "任务状态"),
            field("data[].local_path", "string", false, "对应的本地路径"),
        ],
    },
    ApiDocStatic {
        command: "clear_transfer_history",
        description: "清理传输记录。",
        payload: r#"{"payload":{"mode":"success|failed|finished|all"}}"#,
        response: r#"{"data":10}"#,
        notes: Some("返回被删除的条目数量。"),
        payload_fields: &[
            field("payload.mode", "string", false, "过滤模式（success/failed/finished/all）"),
        ],
        response_fields: &[
            field("data", "number", true, "被删除的任务数量"),
        ],
    },
    ApiDocStatic {
        command: "pause_active_transfer",
        description: "暂停正在运行的传输任务。",
        payload: r#"{"payload":{"task_id":"transfer_id"}}"#,
        response: r#"{"data":{"id":"transfer_id","status":"paused",...}}"#,
        notes: None,
        payload_fields: &[
            field("payload.task_id", "string", true, "传输任务 ID"),
        ],
        response_fields: &[
            field("data.status", "string", true, "最新状态"),
            field("data.message", "string", false, "状态描述"),
        ],
    },
    ApiDocStatic {
        command: "cancel_transfer_task",
        description: "取消传输任务。",
        payload: r#"{"payload":{"task_id":"transfer_id"}}"#,
        response: r#"{"data":{"id":"transfer_id","status":"failed","message":"任务已取消"}} "#,
        notes: None,
        payload_fields: &[
            field("payload.task_id", "string", true, "传输任务 ID"),
        ],
        response_fields: &[
            field("data.status", "string", true, "最新状态（failed）"),
            field("data.message", "string", false, "提示信息"),
        ],
    },
    ApiDocStatic {
        command: "delete_transfer_task",
        description: "删除传输任务记录。",
        payload: r#"{"payload":{"task_id":"transfer_id"}}"#,
        response: r#"{"data":null}"#,
        notes: None,
        payload_fields: &[
            field("payload.task_id", "string", true, "传输任务 ID"),
        ],
        response_fields: GENERIC_RESULT_FIELDS,
    },
    ApiDocStatic {
        command: "resume_transfer_task",
        description: "恢复被暂停/失败的传输任务。",
        payload: r#"{"payload":{"task_id":"transfer_id"}}"#,
        response: r#"{"data":null}"#,
        notes: Some("仅支持文件上传/下载任务。"),
        payload_fields: &[
            field("payload.task_id", "string", true, "传输任务 ID"),
        ],
        response_fields: GENERIC_RESULT_FIELDS,
    },
    ApiDocStatic {
        command: "proxy_official_api",
        description: "转发飞书官方 API 请求。",
        payload: r#"{"payload":{"tenant_id":"tenant_id","method":"GET","path":"/open-apis/drive/v1/files","query":[["page_size","20"]],"body":null}}"#,
        response: r#"{"data":{"code":0,"data":{...}}}"#,
        notes: Some("method 支持 GET/POST/PUT/PATCH/DELETE。"),
        payload_fields: &[
            field("payload.tenant_id", "string", false, "指定代替调用的租户 ID"),
            field("payload.method", "string", true, "HTTP 方法"),
            field("payload.path", "string", true, "官方 API 路径"),
            field("payload.query", "array", false, "查询参数数组 [key,value]"),
            field("payload.body", "object", false, "请求体 JSON"),
        ],
        response_fields: &[
            field("data", "object", true, "官方 API 原始响应"),
        ],
    },
    ApiDocStatic {
        command: "pick_files_dialog",
        description: "弹出系统文件选择对话框。",
        payload: r#"{"payload":{"multiple":true}}"#,
        response: r#"{"data":["/Users/demo/a.txt","/Users/demo/b.txt"]}"#,
        notes: Some("仅限本地 UI 环境。"),
        payload_fields: &[
            field("payload.multiple", "bool", false, "是否允许多选"),
        ],
        response_fields: &[
            field("data[]", "string", false, "所选文件绝对路径"),
        ],
    },
    ApiDocStatic {
        command: "pick_directory_dialog",
        description: "弹出选择文件夹对话框。",
        payload: "{}",
        response: r#"{"data":"/Users/demo/Documents"}"#,
        notes: Some("仅限本地 UI 环境。"),
        payload_fields: NO_BODY_FIELDS,
        response_fields: &[
            field("data", "string", false, "所选目录路径，若取消则为 null"),
        ],
    },
    ApiDocStatic {
        command: "pick_entries_dialog",
        description: "同时支持选择文件或文件夹的对话框。",
        payload: r#"{"payload":{"multiple":false}}"#,
        response: r#"{"data":[{"path":"/Users/demo/file.txt","type":"file"}]}"#,
        notes: Some("仅限本地 UI 环境。"),
        payload_fields: &[
            field("payload.multiple", "bool", false, "是否允许多选"),
        ],
        response_fields: &[
            field("data[].path", "string", true, "选择的路径"),
            field("data[].type", "string", true, "类型 file/folder"),
        ],
    },
];

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum SyncTaskDirection {
    CloudToLocal,
    LocalToCloud,
    Bidirectional,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
enum SyncDetectionMode {
    Metadata,
    Size,
    Checksum,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
enum SyncConflictStrategy {
    PreferRemote,
    PreferLocal,
    Newest,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
enum SyncTaskStatus {
    Idle,
    Scheduled,
    Running,
    Success,
    Failed,
}

impl Default for SyncTaskStatus {
    fn default() -> Self {
        SyncTaskStatus::Idle
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
struct SyncSnapshotEntry {
    path: String,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    modified_at: Option<DateTime<Utc>>,
    #[serde(default)]
    checksum: Option<String>,
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    entry_type: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct SyncTaskRecord {
    id: String,
    name: String,
    direction: SyncTaskDirection,
    group_id: String,
    #[serde(default)]
    group_name: Option<String>,
    tenant_id: String,
    #[serde(default)]
    tenant_name: Option<String>,
    remote_folder_token: String,
    remote_label: String,
    local_path: String,
    schedule: String,
    enabled: bool,
    detection: SyncDetectionMode,
    conflict: SyncConflictStrategy,
    #[serde(default = "default_true")]
    propagate_delete: bool,
    include_patterns: Vec<String>,
    exclude_patterns: Vec<String>,
    #[serde(default)]
    notes: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    #[serde(default)]
    next_run_at: Option<DateTime<Utc>>,
    #[serde(default)]
    last_run_at: Option<DateTime<Utc>>,
    #[serde(default)]
    last_status: SyncTaskStatus,
    #[serde(default)]
    last_message: Option<String>,
    #[serde(default)]
    consecutive_failures: i32,
    #[serde(default)]
    linked_transfer_ids: Vec<String>,
    #[serde(default)]
    local_snapshot: Option<Vec<SyncSnapshotEntry>>,
    #[serde(default)]
    remote_snapshot: Option<Vec<SyncSnapshotEntry>>,
}

#[derive(Serialize, Deserialize, Default)]
struct SyncTaskStoreFile {
    version: u32,
    tasks: Vec<SyncTaskRecord>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct SyncLogEntry {
    task_id: String,
    timestamp: DateTime<Utc>,
    level: String,
    message: String,
}

#[derive(Serialize, Deserialize, Default)]
struct SyncLogStoreFile {
    version: u32,
    logs: Vec<SyncLogEntry>,
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
    sync_task_path: PathBuf,
    sync_log_path: PathBuf,
    api_server_path: PathBuf,
    tenants: RwLock<HashMap<String, TenantConfig>>,
    groups: RwLock<HashMap<String, GroupConfig>>,
    group_keys: RwLock<HashMap<String, GroupKeyRecord>>,
    resource_index: RwLock<HashMap<String, String>>,
    api_key_hash: RwLock<Option<String>>,
    api_key_plain: RwLock<Option<String>>,
    transfers: RwLock<HashMap<String, TransferTaskRecord>>,
    transfer_controls: RwLock<HashMap<String, Arc<TransferControl>>>,
    active_tasks: RwLock<HashSet<String>>,
    sync_tasks: RwLock<HashMap<String, SyncTaskRecord>>,
    sync_logs: RwLock<Vec<SyncLogEntry>>,
    api_server_config: RwLock<ApiServerConfig>,
    api_server_runtime: RwLock<Option<ApiServerRuntime>>,
}

impl AppState {
    fn new(base_dir: PathBuf) -> Self {
        let store_path = base_dir.join(TENANT_STORE_FILE);
        let resource_path = base_dir.join(RESOURCE_INDEX_FILE);
        let security_path = base_dir.join(SECURITY_FILE);
        let transfer_state_path = base_dir.join(TRANSFER_STATE_FILE);
        let sync_task_path = base_dir.join(SYNC_TASK_FILE);
        let sync_log_path = base_dir.join(SYNC_LOG_FILE);
        let api_server_path = base_dir.join(API_SERVER_FILE);
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
        let sync_store = if sync_task_path.exists() {
            let raw = fs::read_to_string(&sync_task_path)
                .expect("无法读取 feisync.sync_tasks.json，请检查权限");
            serde_json::from_str::<SyncTaskStoreFile>(&raw).unwrap_or_default()
        } else {
            SyncTaskStoreFile::default()
        };
        if !sync_task_path.exists() {
            let _ = fs::write(
                &sync_task_path,
                serde_json::to_string_pretty(&sync_store).unwrap(),
            );
        }
        let sync_tasks_map: HashMap<String, SyncTaskRecord> = sync_store
            .tasks
            .into_iter()
            .map(|task| (task.id.clone(), task))
            .collect();
        let sync_log_store = if sync_log_path.exists() {
            let raw =
                fs::read_to_string(&sync_log_path).expect("无法读取 feisync.sync_logs.json，请检查权限");
            serde_json::from_str::<SyncLogStoreFile>(&raw).unwrap_or_default()
        } else {
            SyncLogStoreFile::default()
        };
        if !sync_log_path.exists() {
            let _ = fs::write(
                &sync_log_path,
                serde_json::to_string_pretty(&sync_log_store).unwrap(),
            );
        }
        let api_server_config = if api_server_path.exists() {
            fs::read_to_string(&api_server_path)
                .ok()
                .and_then(|content| serde_json::from_str::<ApiServerConfig>(&content).ok())
                .unwrap_or_default()
        } else {
            ApiServerConfig::default()
        };
        if !api_server_path.exists() {
            let _ = fs::write(
                &api_server_path,
                serde_json::to_string_pretty(&api_server_config).unwrap(),
            );
        }
        AppState {
            client: Client::new(),
            store_path,
            resource_path,
            security_path,
            transfer_state_path,
            sync_task_path,
            sync_log_path,
             api_server_path,
            tenants: RwLock::new(tenants_map),
            groups: RwLock::new(groups_map),
            group_keys: RwLock::new(group_keys_map),
            resource_index: RwLock::new(resource_index),
            api_key_hash: RwLock::new(api_key_hash),
            api_key_plain: RwLock::new(api_key_plain),
            transfers: RwLock::new(transfers_map),
            transfer_controls: RwLock::new(HashMap::new()),
            active_tasks: RwLock::new(HashSet::new()),
            sync_tasks: RwLock::new(sync_tasks_map),
            sync_logs: RwLock::new(sync_log_store.logs),
            api_server_config: RwLock::new(api_server_config),
            api_server_runtime: RwLock::new(None),
        }
    }

    fn persist_sync_tasks(&self) -> AppResult<()> {
        let tasks = self.sync_tasks.read();
        let payload = SyncTaskStoreFile {
            version: 1,
            tasks: tasks.values().cloned().collect(),
        };
        fs::write(
            &self.sync_task_path,
            serde_json::to_string_pretty(&payload)?,
        )?;
        Ok(())
    }

    fn persist_sync_logs(&self) -> AppResult<()> {
        let logs = self.sync_logs.read();
        let payload = SyncLogStoreFile {
            version: 1,
            logs: logs.clone(),
        };
        fs::write(
            &self.sync_log_path,
            serde_json::to_string_pretty(&payload)?,
        )?;
        Ok(())
    }

    fn persist_api_server_config(&self) -> AppResult<()> {
        let config = self.api_server_config.read().clone();
        fs::write(
            &self.api_server_path,
            serde_json::to_string_pretty(&config)?,
        )?;
        Ok(())
    }

    fn api_server_status_snapshot(&self) -> ApiServerStatus {
        let config = self.api_server_config.read().clone();
        let runtime = self.api_server_runtime.read();
        let address = runtime
            .as_ref()
            .map(|rt| rt.addr.to_string());
        ApiServerStatus {
            running: runtime.is_some(),
            address,
            config,
        }
    }

    fn update_api_server_config(&self, patch: UpdateApiServerConfigPayload) -> AppResult<ApiServerConfig> {
        {
            let mut cfg = self.api_server_config.write();
            if let Some(host) = patch.listen_host {
                cfg.listen_host = host;
            }
            if let Some(port) = patch.port {
                cfg.port = port;
            }
            if let Some(timeout) = patch.timeout_secs {
                cfg.timeout_secs = timeout.clamp(30, 600);
            }
        }
        self.persist_api_server_config()?;
        Ok(self.api_server_config.read().clone())
    }

    async fn start_api_service(&self, app: &AppHandle) -> AppResult<ApiServerStatus> {
        if self.api_server_runtime.read().is_some() {
            return Ok(self.api_server_status_snapshot());
        }
        let config = self.api_server_config.read().clone();
        let addr: SocketAddr = format!("{}:{}", config.listen_host, config.port)
            .parse()
            .map_err(|err| AppError::Message(format!("监听地址无效: {}", err)))?;
        let (tx, rx) = oneshot::channel();
        let timeout = TokioDuration::from_secs(config.timeout_secs.clamp(30, 600));
        let app_handle = app.clone();
        let task = tokio::spawn(async move {
            run_api_http_server(app_handle, addr, timeout, rx).await;
        });
        {
            let mut runtime = self.api_server_runtime.write();
            *runtime = Some(ApiServerRuntime { addr, shutdown: tx, task });
        }
        Ok(self.api_server_status_snapshot())
    }

    async fn stop_api_service(&self) -> AppResult<ApiServerStatus> {
        let runtime_opt = {
            let mut guard = self.api_server_runtime.write();
            guard.take()
        };
        if let Some(runtime) = runtime_opt {
            let _ = runtime.shutdown.send(());
            let _ = runtime.task.await;
        }
        Ok(self.api_server_status_snapshot())
    }

    fn list_sync_tasks_internal(&self) -> Vec<SyncTaskRecord> {
        let tasks = self.sync_tasks.read();
        let mut list: Vec<SyncTaskRecord> = tasks.values().cloned().collect();
        list.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        list
    }

    fn create_sync_task_record(&self, payload: CreateSyncTaskPayload) -> AppResult<SyncTaskRecord> {
        let mut map = self.sync_tasks.write();
        let id = Uuid::new_v4().to_string();
        let now = Utc::now();
        let record = SyncTaskRecord {
            id: id.clone(),
            name: payload.name,
            direction: payload.direction,
            group_id: payload.group_id,
            group_name: payload.group_name,
            tenant_id: payload.tenant_id,
            tenant_name: payload.tenant_name,
            remote_folder_token: payload.remote_folder_token,
            remote_label: payload.remote_label,
            local_path: payload.local_path,
            schedule: payload.schedule,
            enabled: payload.enabled,
            detection: payload.detection,
            conflict: payload.conflict,
            propagate_delete: payload.propagate_delete,
            include_patterns: payload.include_patterns,
            exclude_patterns: payload.exclude_patterns,
            notes: payload.notes,
            created_at: now,
            updated_at: now,
            next_run_at: None,
            last_run_at: None,
            last_status: SyncTaskStatus::Idle,
            last_message: None,
            consecutive_failures: 0,
            linked_transfer_ids: Vec::new(),
            local_snapshot: None,
            remote_snapshot: None,
        };
        map.insert(id.clone(), record.clone());
        drop(map);
        self.persist_sync_tasks()?;
        Ok(record)
    }

    fn update_sync_task_record<F>(&self, task_id: &str, updater: F) -> AppResult<SyncTaskRecord>
    where
        F: FnOnce(&mut SyncTaskRecord),
    {
        let mut map = self.sync_tasks.write();
        let task = map
            .get_mut(task_id)
            .ok_or_else(|| AppError::Message("任务不存在".into()))?;
        updater(task);
        task.updated_at = Utc::now();
        let snapshot = task.clone();
        drop(map);
        self.persist_sync_tasks()?;
        Ok(snapshot)
    }

    fn remove_sync_task_record(&self, task_id: &str) -> AppResult<()> {
        let mut map = self.sync_tasks.write();
        map.remove(task_id)
            .ok_or_else(|| AppError::Message("任务不存在".into()))?;
        drop(map);
        self.persist_sync_tasks()?;
        Ok(())
    }

    fn append_sync_log(&self, entry: SyncLogEntry) -> AppResult<()> {
        let mut logs = self.sync_logs.write();
        logs.push(entry);
        if logs.len() > 2000 {
            let overflow = logs.len() - 2000;
            logs.drain(0..overflow);
        }
        drop(logs);
        self.persist_sync_logs()
    }

    fn list_sync_logs_by_task(&self, task_id: &str, limit: usize) -> Vec<SyncLogEntry> {
        let logs = self.sync_logs.read();
        let mut filtered: Vec<SyncLogEntry> = logs
            .iter()
            .filter(|log| log.task_id == task_id)
            .cloned()
            .collect();
        filtered.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        filtered.truncate(limit);
        filtered
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

    async fn delete_drive_entry(
        &self,
        tenant: &TenantConfig,
        token: &str,
        entry_type: &str,
    ) -> AppResult<()> {
        let path = format!("/open-apis/drive/v1/files/{}", token);
        let _ = self
            .forward_request(
                tenant,
                "DELETE",
                &path,
                Some(vec![("type".to_string(), entry_type.to_string())]),
                None,
            )
            .await?;
        let _ = self.remove_resource(token);
        Ok(())
    }
}

// Sync helpers
impl AppState {
    async fn run_local_to_cloud_sync(&self, task_id: &str, app: &AppHandle) -> AppResult<()> {
        let task_record = {
            let tasks = self.sync_tasks.read();
            tasks
                .get(task_id)
                .cloned()
                .ok_or_else(|| AppError::Message("任务不存在".into()))?
        };
        if !matches!(
            task_record.direction,
            SyncTaskDirection::LocalToCloud
        ) {
            let _ = self.append_sync_log(SyncLogEntry {
                task_id: task_id.to_string(),
                timestamp: Utc::now(),
                level: "warn".into(),
                message: "当前任务方向不是本地 → 云端，执行已跳过".into(),
            });
            return Ok(());
        }
        let tenant = self.ensure_token(&task_record.tenant_id).await?;
        let local_root = PathBuf::from(&task_record.local_path);
        let include_patterns = task_record.include_patterns.clone();
        let exclude_patterns = task_record.exclude_patterns.clone();
        if !task_record.propagate_delete {
            let _ = self.append_sync_log(SyncLogEntry {
                task_id: task_record.id.clone(),
                timestamp: Utc::now(),
                level: "info".into(),
                message: "当前任务未启用“同步删除”，仅会上传新增/更新文件。".into(),
            });
        }

        self.append_sync_log(SyncLogEntry {
            task_id: task_record.id.clone(),
            timestamp: Utc::now(),
            level: "info".into(),
            message: format!("扫描本地目录 {}", local_root.display()),
        })?;
        let local_entries =
            scan_local_entries(local_root.clone(), include_patterns.clone(), exclude_patterns.clone())
                .await?;
        self.append_sync_log(SyncLogEntry {
            task_id: task_record.id.clone(),
            timestamp: Utc::now(),
            level: "info".into(),
            message: format!("本地文件数 {}", local_entries.len()),
        })?;

        let (remote_entries, mut remote_dirs) = self
            .scan_remote_entries(
                &tenant,
                &task_record.remote_folder_token,
                include_patterns,
                exclude_patterns,
            )
            .await?;
        let uploads = diff_local_to_remote(&local_entries, &remote_entries);
        let can_delete_remote = task_record.propagate_delete
            && task_record.local_snapshot.is_some()
            && task_record.remote_snapshot.is_some();
        if task_record.propagate_delete && !can_delete_remote {
            let _ = self.append_sync_log(SyncLogEntry {
                task_id: task_record.id.clone(),
                timestamp: Utc::now(),
                level: "info".into(),
                message: "首次运行尚未建立同步快照，暂不执行云端删除。".into(),
            });
        }
        let remote_removals = if can_delete_remote {
            find_remote_only(&remote_entries, &local_entries)
        } else {
            Vec::new()
        };
        if uploads.is_empty() && remote_removals.is_empty() {
            self.append_sync_log(SyncLogEntry {
                task_id: task_record.id.clone(),
                timestamp: Utc::now(),
                level: "info".into(),
                message: "云端已是最新，无需上传".into(),
            })?;
            self.update_sync_task_record(task_id, |task| {
                task.local_snapshot = Some(local_entries.clone());
                task.remote_snapshot = Some(remote_entries.clone());
                task.last_status = SyncTaskStatus::Success;
                task.last_message = Some("云端已是最新".into());
                task.last_run_at = Some(Utc::now());
            })?;
            return Ok(());
        }

        remote_dirs.insert(String::new(), task_record.remote_folder_token.clone());
        if !uploads.is_empty() || !remote_removals.is_empty() {
            self.append_sync_log(SyncLogEntry {
                task_id: task_record.id.clone(),
                timestamp: Utc::now(),
                level: "info".into(),
                message: format!(
                    "需上传 {} 个文件{}",
                    uploads.len(),
                    if task_record.propagate_delete {
                        format!(", 需删除云端 {} 个", remote_removals.len())
                    } else {
                        String::new()
                    }
                ),
            })?;
        }

        let mut uploaded = 0usize;
        for entry in uploads {
            let parent_token = self
                .ensure_remote_parent_for_path(
                    &tenant,
                    &task_record.tenant_id,
                    &task_record.remote_folder_token,
                    &mut remote_dirs,
                    &entry.path,
                )
                .await?;
            let local_file = local_root.join(&entry.path);
            let file_name = Path::new(&entry.path)
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| AppError::Message(format!("无法解析文件名 {}", entry.path)))?;
            self.append_sync_log(SyncLogEntry {
                task_id: task_record.id.clone(),
                timestamp: Utc::now(),
                level: "info".into(),
                message: format!("上传 {}", entry.path),
            })?;
            self
                .upload_local_file_path(
                    &task_record.tenant_id,
                    &tenant,
                    &parent_token,
                    &local_file,
                    file_name,
                    None,
                    Some(app),
                )
                .await?;
            uploaded += 1;
        }

        let mut deleted_remote = 0usize;
        for entry in &remote_removals {
            if let Some(token) = entry.token.as_deref() {
                let entry_type = entry.entry_type.as_deref().unwrap_or("file").to_string();
                self.append_sync_log(SyncLogEntry {
                    task_id: task_record.id.clone(),
                    timestamp: Utc::now(),
                    level: "info".into(),
                    message: format!("删除云端 {}", entry.path),
                })?;
                self
                    .delete_drive_entry(&tenant, token, &entry_type)
                    .await?;
                deleted_remote += 1;
            }
        }

        let summary = if task_record.propagate_delete {
            format!("上传 {} 个，删除云端 {} 个", uploaded, deleted_remote)
        } else {
            format!("上传完成，共 {} 个文件", uploaded)
        };
        self.append_sync_log(SyncLogEntry {
            task_id: task_record.id.clone(),
            timestamp: Utc::now(),
            level: "info".into(),
            message: summary.clone(),
        })?;
        let (remote_after, _) = self
            .scan_remote_entries(
                &tenant,
                &task_record.remote_folder_token,
                task_record.include_patterns.clone(),
                task_record.exclude_patterns.clone(),
            )
            .await?;
        self.update_sync_task_record(task_id, |task| {
            task.local_snapshot = Some(local_entries.clone());
            task.remote_snapshot = Some(remote_after.clone());
            task.last_status = SyncTaskStatus::Success;
            task.last_message = Some(summary.clone());
            task.last_run_at = Some(Utc::now());
        })?;
        Ok(())
    }

    async fn run_cloud_to_local_sync(&self, task_id: &str, app: &AppHandle) -> AppResult<()> {
        let task_record = {
            let tasks = self.sync_tasks.read();
            tasks
                .get(task_id)
                .cloned()
                .ok_or_else(|| AppError::Message("任务不存在".into()))?
        };
        if !matches!(
            task_record.direction,
            SyncTaskDirection::CloudToLocal
        ) {
            let _ = self.append_sync_log(SyncLogEntry {
                task_id: task_id.to_string(),
                timestamp: Utc::now(),
                level: "warn".into(),
                message: "当前任务方向不是云端 → 本地，执行已跳过".into(),
            });
            return Ok(());
        }
        let tenant = self.ensure_token(&task_record.tenant_id).await?;
        let local_root = PathBuf::from(&task_record.local_path);
        let include_patterns = task_record.include_patterns.clone();
        let exclude_patterns = task_record.exclude_patterns.clone();
        if !task_record.propagate_delete {
            let _ = self.append_sync_log(SyncLogEntry {
                task_id: task_record.id.clone(),
                timestamp: Utc::now(),
                level: "info".into(),
                message: "当前任务未启用“同步删除”，仅会下载新增/更新文件。".into(),
            });
        }

        self.append_sync_log(SyncLogEntry {
            task_id: task_record.id.clone(),
            timestamp: Utc::now(),
            level: "info".into(),
            message: "扫描云端文件".into(),
        })?;
        let (remote_entries, remote_dirs) = self
            .scan_remote_entries(
                &tenant,
                &task_record.remote_folder_token,
                include_patterns.clone(),
                exclude_patterns.clone(),
            )
            .await?;
        self.append_sync_log(SyncLogEntry {
            task_id: task_record.id.clone(),
            timestamp: Utc::now(),
            level: "info".into(),
            message: format!("云端文件数 {}", remote_entries.len()),
        })?;

        self.append_sync_log(SyncLogEntry {
            task_id: task_record.id.clone(),
            timestamp: Utc::now(),
            level: "info".into(),
            message: "扫描本地文件".into(),
        })?;
        if !local_root.exists() {
            async_fs::create_dir_all(&local_root).await?;
            let _ = self.append_sync_log(SyncLogEntry {
                task_id: task_record.id.clone(),
                timestamp: Utc::now(),
                level: "info".into(),
                message: format!("本地目录不存在，已创建 {}", local_root.display()),
            });
        }
        let local_entries =
            scan_local_entries(local_root.clone(), include_patterns.clone(), exclude_patterns.clone())
                .await?;
        let to_download = diff_remote_to_local(&remote_entries, &local_entries);
        let can_delete_local = task_record.propagate_delete
            && task_record.local_snapshot.is_some()
            && task_record.remote_snapshot.is_some();
        if task_record.propagate_delete && !can_delete_local {
            let _ = self.append_sync_log(SyncLogEntry {
                task_id: task_record.id.clone(),
                timestamp: Utc::now(),
                level: "info".into(),
                message: "首次运行尚未建立同步快照，暂不执行本地删除。".into(),
            });
        }
        let to_delete = if can_delete_local {
            find_local_only(&local_entries, &remote_entries)
        } else {
            Vec::new()
        };
        self.append_sync_log(SyncLogEntry {
            task_id: task_record.id.clone(),
            timestamp: Utc::now(),
            level: "info".into(),
            message: format!(
                "需下载 {} 个文件{}",
                to_download.len(),
                if task_record.propagate_delete {
                    format!(", 待删除本地 {} 个", to_delete.len())
                } else {
                    String::new()
                }
            ),
        })?;
        if to_download.is_empty() && to_delete.is_empty() {
            self.append_sync_log(SyncLogEntry {
                task_id: task_record.id.clone(),
                timestamp: Utc::now(),
                level: "info".into(),
                message: "本地目录已是最新，无需下载".into(),
            })?;
            self.update_sync_task_record(task_id, |task| {
                task.local_snapshot = Some(local_entries.clone());
                task.remote_snapshot = Some(remote_entries.clone());
                task.last_status = SyncTaskStatus::Success;
                task.last_message = Some("本地目录已是最新".into());
                task.last_run_at = Some(Utc::now());
            })?;
            return Ok(());
        }

        for (relative, _) in remote_dirs.iter() {
            if relative.is_empty() {
                continue;
            }
            let target_dir = local_root.join(relative);
            async_fs::create_dir_all(&target_dir).await?;
        }

        let mut downloaded = 0usize;
        for entry in &to_download {
            let token = entry
                .token
                .as_deref()
                .ok_or_else(|| AppError::Message(format!("{} 缺少远端 token", entry.path)))?;
            let file_name = Path::new(&entry.path)
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| AppError::Message(format!("无法解析文件名 {}", entry.path)))?;
            let local_path = local_root.join(&entry.path);
            if let Some(parent) = local_path.parent() {
                async_fs::create_dir_all(parent).await?;
            }
            self.append_sync_log(SyncLogEntry {
                task_id: task_record.id.clone(),
                timestamp: Utc::now(),
                level: "info".into(),
                message: format!("下载 {}", entry.path),
            })?;
            self
                .download_drive_file(
                    &task_record.tenant_id,
                    &tenant,
                    token,
                    local_path
                        .parent()
                        .unwrap_or(&local_root),
                    file_name,
                    None,
                    Some(app),
                    entry.size,
                )
                .await?;
            downloaded += 1;
        }

        let mut deleted = 0usize;
        for entry in &to_delete {
            let target = local_root.join(&entry.path);
            match async_fs::metadata(&target).await {
                Ok(meta) => {
                    self.append_sync_log(SyncLogEntry {
                        task_id: task_record.id.clone(),
                        timestamp: Utc::now(),
                        level: "info".into(),
                        message: format!("删除本地 {}", entry.path),
                    })?;
                    if meta.is_dir() {
                        async_fs::remove_dir_all(&target).await?;
                    } else {
                        async_fs::remove_file(&target).await?;
                    }
                    deleted += 1;
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err.into()),
            }
        }

        self.append_sync_log(SyncLogEntry {
            task_id: task_record.id.clone(),
            timestamp: Utc::now(),
            level: "info".into(),
            message: format!("下载 {} 个文件，删除 {} 个文件", downloaded, deleted),
        })?;
        let refreshed_local =
            scan_local_entries(local_root.clone(), include_patterns.clone(), exclude_patterns.clone())
                .await?;
        self.update_sync_task_record(task_id, |task| {
            task.local_snapshot = Some(refreshed_local.clone());
            task.remote_snapshot = Some(remote_entries.clone());
            task.last_status = SyncTaskStatus::Success;
            task.last_message = Some(format!("下载 {} 个，删除 {} 个", downloaded, deleted));
            task.last_run_at = Some(Utc::now());
        })?;
        Ok(())
    }

    async fn run_bidirectional_sync(&self, task_id: &str, app: &AppHandle) -> AppResult<()> {
        let task_record = {
            let tasks = self.sync_tasks.read();
            tasks
                .get(task_id)
                .cloned()
                .ok_or_else(|| AppError::Message("任务不存在".into()))?
        };
        if !matches!(task_record.direction, SyncTaskDirection::Bidirectional) {
            let _ = self.append_sync_log(SyncLogEntry {
                task_id: task_id.to_string(),
                timestamp: Utc::now(),
                level: "warn".into(),
                message: "当前任务不是双向同步，执行已跳过".into(),
            });
            return Ok(());
        }
        let tenant = self.ensure_token(&task_record.tenant_id).await?;
        let local_root = PathBuf::from(&task_record.local_path);
        let include_patterns = task_record.include_patterns.clone();
        let exclude_patterns = task_record.exclude_patterns.clone();
        if !task_record.propagate_delete {
            let _ = self.append_sync_log(SyncLogEntry {
                task_id: task_record.id.clone(),
                timestamp: Utc::now(),
                level: "info".into(),
                message: "未启用“同步删除”，双向同步仅比对新增/修改文件。".into(),
            });
        }

        self.append_sync_log(SyncLogEntry {
            task_id: task_record.id.clone(),
            timestamp: Utc::now(),
            level: "info".into(),
            message: "双向同步：扫描本地与云端".into(),
        })?;
        let local_entries =
            scan_local_entries(local_root.clone(), include_patterns.clone(), exclude_patterns.clone())
                .await?;
        let (remote_entries, mut remote_dirs) = self
            .scan_remote_entries(
                &tenant,
                &task_record.remote_folder_token,
                include_patterns.clone(),
                exclude_patterns.clone(),
            )
            .await?;
        remote_dirs.insert(String::new(), task_record.remote_folder_token.clone());
        let plan = plan_bidirectional_actions(
            &local_entries,
            &remote_entries,
            task_record.local_snapshot.as_deref(),
            task_record.remote_snapshot.as_deref(),
            task_record.propagate_delete,
            task_record.conflict.clone(),
        );
        for message in &plan.conflicts {
            let _ = self.append_sync_log(SyncLogEntry {
                task_id: task_record.id.clone(),
                timestamp: Utc::now(),
                level: "warn".into(),
                message: message.clone(),
            });
        }
        if plan.uploads.is_empty()
            && plan.downloads.is_empty()
            && plan.delete_local.is_empty()
            && plan.delete_remote.is_empty()
        {
            let note = if plan.conflicts.is_empty() {
                "未检测到差异".to_string()
            } else {
                format!("存在 {} 个冲突，未执行变更", plan.conflicts.len())
            };
            self.append_sync_log(SyncLogEntry {
                task_id: task_record.id.clone(),
                timestamp: Utc::now(),
                level: "info".into(),
                message: note.clone(),
            })?;
            self.update_sync_task_record(task_id, |task| {
                task.local_snapshot = Some(local_entries.clone());
                task.remote_snapshot = Some(remote_entries.clone());
                task.last_status = SyncTaskStatus::Success;
                task.last_message = Some(note);
                task.last_run_at = Some(Utc::now());
            })?;
            return Ok(());
        }

        let mut uploaded = 0usize;
        for entry in &plan.uploads {
            let parent_token = self
                .ensure_remote_parent_for_path(
                    &tenant,
                    &task_record.tenant_id,
                    &task_record.remote_folder_token,
                    &mut remote_dirs,
                    &entry.path,
                )
                .await?;
            let local_file = local_root.join(&entry.path);
            let file_name = Path::new(&entry.path)
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| AppError::Message(format!("无法解析文件名 {}", entry.path)))?;
            self.append_sync_log(SyncLogEntry {
                task_id: task_record.id.clone(),
                timestamp: Utc::now(),
                level: "info".into(),
                message: format!("上传 {}", entry.path),
            })?;
            self
                .upload_local_file_path(
                    &task_record.tenant_id,
                    &tenant,
                    &parent_token,
                    &local_file,
                    file_name,
                    None,
                    Some(app),
                )
                .await?;
            uploaded += 1;
        }

        let mut downloaded = 0usize;
        for entry in &plan.downloads {
            let token = entry
                .token
                .as_deref()
                .ok_or_else(|| AppError::Message(format!("{} 缺少远端 token", entry.path)))?;
            let file_name = Path::new(&entry.path)
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| AppError::Message(format!("无法解析文件名 {}", entry.path)))?;
            let local_path = local_root.join(&entry.path);
            if let Some(parent) = local_path.parent() {
                async_fs::create_dir_all(parent).await?;
            }
            self.append_sync_log(SyncLogEntry {
                task_id: task_record.id.clone(),
                timestamp: Utc::now(),
                level: "info".into(),
                message: format!("下载 {}", entry.path),
            })?;
            self
                .download_drive_file(
                    &task_record.tenant_id,
                    &tenant,
                    token,
                    local_path
                        .parent()
                        .unwrap_or(&local_root),
                    file_name,
                    None,
                    Some(app),
                    entry.size,
                )
                .await?;
            downloaded += 1;
        }

        let mut deleted_remote = 0usize;
        for entry in &plan.delete_remote {
            if let Some(token) = &entry.token {
                let entry_type = entry
                    .entry_type
                    .as_deref()
                    .unwrap_or("file")
                    .to_string();
                self.append_sync_log(SyncLogEntry {
                    task_id: task_record.id.clone(),
                    timestamp: Utc::now(),
                    level: "info".into(),
                    message: format!("删除云端 {}", entry.path),
                })?;
                self
                    .delete_drive_entry(&tenant, token, &entry_type)
                    .await?;
                deleted_remote += 1;
            }
        }

        let mut deleted_local = 0usize;
        for entry in &plan.delete_local {
            let target = local_root.join(&entry.path);
            match async_fs::metadata(&target).await {
                Ok(meta) => {
                    self.append_sync_log(SyncLogEntry {
                        task_id: task_record.id.clone(),
                        timestamp: Utc::now(),
                        level: "info".into(),
                        message: format!("删除本地 {}", entry.path),
                    })?;
                    if meta.is_dir() {
                        async_fs::remove_dir_all(&target).await?;
                    } else {
                        async_fs::remove_file(&target).await?;
                    }
                    deleted_local += 1;
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err.into()),
            }
        }

        let refreshed_local =
            scan_local_entries(local_root.clone(), include_patterns.clone(), exclude_patterns.clone())
                .await?;
        let (refreshed_remote, _) = self
            .scan_remote_entries(
                &tenant,
                &task_record.remote_folder_token,
                include_patterns.clone(),
                exclude_patterns.clone(),
            )
            .await?;
        let summary = format!(
            "上传 {}、下载 {}、删除本地 {}、删除云端 {}",
            uploaded, downloaded, deleted_local, deleted_remote
        );
        self.append_sync_log(SyncLogEntry {
            task_id: task_record.id.clone(),
            timestamp: Utc::now(),
            level: "info".into(),
            message: summary.clone(),
        })?;
        self.update_sync_task_record(task_id, |task| {
            task.local_snapshot = Some(refreshed_local.clone());
            task.remote_snapshot = Some(refreshed_remote.clone());
            task.last_status = SyncTaskStatus::Success;
            task.last_message = Some(summary.clone());
            task.last_run_at = Some(Utc::now());
        })?;
        Ok(())
    }

    async fn scan_remote_entries(
        &self,
        tenant: &TenantConfig,
        root_token: &str,
        includes: Vec<String>,
        excludes: Vec<String>,
    ) -> AppResult<(Vec<SyncSnapshotEntry>, HashMap<String, String>)> {
        let include_patterns: Vec<WildMatch> = includes.iter().map(|p| WildMatch::new(p)).collect();
        let exclude_patterns: Vec<WildMatch> = excludes.iter().map(|p| WildMatch::new(p)).collect();
        let mut files = Vec::new();
        let mut directories = HashMap::new();
        directories.insert(String::new(), root_token.to_string());
        let mut queue = VecDeque::new();
        queue.push_back((root_token.to_string(), PathBuf::new()));
        while let Some((token, prefix)) = queue.pop_front() {
            let entries = list_folder(self, tenant, Some(token.clone())).await?;
            for entry in entries {
                let mut child_path = prefix.clone();
                child_path.push(&entry.name);
                let rel = normalize_relative_path(&child_path);
                if entry.entry_type.eq_ignore_ascii_case("folder") {
                    directories.insert(rel.clone(), entry.token.clone());
                    queue.push_back((entry.token.clone(), child_path));
                    continue;
                }
                if !matches_filters(&rel, &include_patterns, &exclude_patterns) {
                    continue;
                }
                let modified_at = entry
                    .update_time
                    .as_deref()
                    .and_then(parse_remote_timestamp);
                files.push(SyncSnapshotEntry {
                    path: rel,
                    size: entry.size.map(|s| s as u64),
                    modified_at,
                    entry_type: Some(entry.entry_type),
                    token: Some(entry.token),
                    ..Default::default()
                });
            }
        }
        Ok((files, directories))
    }

    async fn ensure_remote_parent_for_path(
        &self,
        tenant: &TenantConfig,
        tenant_id: &str,
        root_token: &str,
        cache: &mut HashMap<String, String>,
        relative_path: &str,
    ) -> AppResult<String> {
        let parent = Path::new(relative_path).parent();
        let mut current_token = root_token.to_string();
        if let Some(parent_path) = parent {
            let mut current_key = String::new();
            for component in parent_path.components() {
                if let std::path::Component::Normal(seg) = component {
                    let part = seg.to_string_lossy().to_string();
                    if !current_key.is_empty() {
                        current_key.push('/');
                    }
                    current_key.push_str(&part);
                    if let Some(token) = cache.get(&current_key) {
                        current_token = token.clone();
                        continue;
                    }
                    let token = self
                        .create_drive_folder_entry(tenant, tenant_id, &current_token, &part)
                        .await?;
                    cache.insert(current_key.clone(), token.clone());
                    current_token = token;
                }
            }
        }
        Ok(current_token)
    }
}

async fn scan_local_entries(
    base_path: PathBuf,
    includes: Vec<String>,
    excludes: Vec<String>,
) -> AppResult<Vec<SyncSnapshotEntry>> {
    spawn_blocking(move || -> AppResult<Vec<SyncSnapshotEntry>> {
        if !base_path.exists() {
            return Err(AppError::Message(format!("本地目录不存在: {}", base_path.display())));
        }
        let include_patterns: Vec<WildMatch> = includes.iter().map(|p| WildMatch::new(p)).collect();
        let exclude_patterns: Vec<WildMatch> = excludes.iter().map(|p| WildMatch::new(p)).collect();
        let mut result = Vec::new();
        for entry in WalkDir::new(&base_path).into_iter().filter_map(|e| e.ok()) {
            if !entry.file_type().is_file() {
                continue;
            }
            let rel = entry
                .path()
                .strip_prefix(&base_path)
                .map_err(|_| AppError::Message("计算相对路径失败".into()))?;
            let rel_str = normalize_relative_path(rel);
            if !matches_filters(&rel_str, &include_patterns, &exclude_patterns) {
                continue;
            }
            let metadata = entry.metadata()?;
            let modified_at = metadata.modified().ok().and_then(system_time_to_utc);
            result.push(SyncSnapshotEntry {
                path: rel_str,
                size: Some(metadata.len()),
                modified_at,
                entry_type: Some("file".into()),
                ..Default::default()
            });
        }
        Ok(result)
    })
    .await
    .map_err(|err| AppError::Message(format!("扫描本地目录失败: {}", err)))?
}

fn diff_local_to_remote(
    local: &[SyncSnapshotEntry],
    remote: &[SyncSnapshotEntry],
) -> Vec<SyncSnapshotEntry> {
    let remote_map =
        remote
            .iter()
            .map(|entry| (entry.path.as_str(), entry))
            .collect::<HashMap<_, _>>();
    local
        .iter()
        .filter(|entry| {
            if let Some(remote_entry) = remote_map.get(entry.path.as_str()) {
                !snapshots_equal(entry, remote_entry)
            } else {
                true
            }
        })
        .cloned()
        .collect()
}

fn diff_remote_to_local(
    remote: &[SyncSnapshotEntry],
    local: &[SyncSnapshotEntry],
) -> Vec<SyncSnapshotEntry> {
    let local_map =
        local
            .iter()
            .map(|entry| (entry.path.as_str(), entry))
            .collect::<HashMap<_, _>>();
    remote
        .iter()
        .filter(|entry| {
            if let Some(local_entry) = local_map.get(entry.path.as_str()) {
                !snapshots_equal(entry, local_entry)
            } else {
                true
            }
        })
        .cloned()
        .collect()
}

fn entries_only_in_first<'a>(
    first: &'a [SyncSnapshotEntry],
    second: &'a [SyncSnapshotEntry],
) -> Vec<SyncSnapshotEntry> {
    let map =
        second
            .iter()
            .map(|entry| (entry.path.as_str(), entry))
            .collect::<HashMap<_, _>>();
    first
        .iter()
        .filter(|entry| !map.contains_key(entry.path.as_str()))
        .cloned()
        .collect()
}

fn find_local_only(
    local: &[SyncSnapshotEntry],
    remote: &[SyncSnapshotEntry],
) -> Vec<SyncSnapshotEntry> {
    entries_only_in_first(local, remote)
}

fn find_remote_only(
    remote: &[SyncSnapshotEntry],
    local: &[SyncSnapshotEntry],
) -> Vec<SyncSnapshotEntry> {
    entries_only_in_first(remote, local)
}

fn snapshots_equal(a: &SyncSnapshotEntry, b: &SyncSnapshotEntry) -> bool {
    if a.size.is_some() && b.size.is_some() && a.size != b.size {
        return false;
    }
    match (&a.modified_at, &b.modified_at) {
        (Some(lhs), Some(rhs)) => lhs.signed_duration_since(*rhs).num_seconds().abs() <= 2,
        _ => true,
    }
}

fn matches_filters(path: &str, includes: &[WildMatch], excludes: &[WildMatch]) -> bool {
    if !includes.is_empty() && !includes.iter().any(|pat| pat.matches(path)) {
        return false;
    }
    if excludes.iter().any(|pat| pat.matches(path)) {
        return false;
    }
    true
}

fn normalize_relative_path(path: &Path) -> String {
    let mut value = path.to_string_lossy().replace('\\', "/");
    if value.starts_with("./") {
        value = value.trim_start_matches("./").to_string();
    }
    value
}

fn system_time_to_utc(time: SystemTime) -> Option<DateTime<Utc>> {
    Some(chrono::DateTime::<Utc>::from(time))
}

fn parse_remote_timestamp(text: &str) -> Option<DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(text)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
}

fn default_true() -> bool {
    true
}

fn reset_task_snapshots(task: &mut SyncTaskRecord, note: &str) {
    task.local_snapshot = None;
    task.remote_snapshot = None;
    task.linked_transfer_ids.clear();
    task.last_status = SyncTaskStatus::Idle;
    task.last_message = Some(note.to_string());
    task.last_run_at = None;
    task.consecutive_failures = 0;
}

fn build_api_docs() -> Vec<ApiDocEntry> {
    API_DOCS
        .iter()
        .map(|entry| ApiDocEntry {
            command: entry.command.to_string(),
            method: "POST".into(),
            path: format!("/command/{}", entry.command),
            description: entry.description.to_string(),
            payload: entry.payload.to_string(),
            response: entry.response.to_string(),
            notes: entry.notes.map(|note| note.to_string()),
            payload_fields: entry.payload_fields.to_vec(),
            response_fields: entry.response_fields.to_vec(),
        })
        .collect()
}

fn to_json_value<T: Serialize>(value: T) -> Result<Value, String> {
    serde_json::to_value(value).map_err(|e| e.to_string())
}

#[derive(Default)]
struct BidirectionalPlan {
    uploads: Vec<SyncSnapshotEntry>,
    downloads: Vec<SyncSnapshotEntry>,
    delete_local: Vec<SyncSnapshotEntry>,
    delete_remote: Vec<SyncSnapshotEntry>,
    conflicts: Vec<String>,
}

fn entries_to_map(entries: &[SyncSnapshotEntry]) -> HashMap<String, SyncSnapshotEntry> {
    entries
        .iter()
        .cloned()
        .map(|entry| (entry.path.clone(), entry))
        .collect()
}

fn has_snapshot_changed(
    current: Option<&SyncSnapshotEntry>,
    previous: Option<&SyncSnapshotEntry>,
) -> bool {
    match (previous, current) {
        (None, None) => false,
        (None, Some(_)) | (Some(_), None) => true,
        (Some(old), Some(newer)) => !snapshots_equal(old, newer),
    }
}

fn is_local_newer(
    local: Option<&SyncSnapshotEntry>,
    remote: Option<&SyncSnapshotEntry>,
) -> bool {
    let local_time = local.and_then(|entry| entry.modified_at);
    let remote_time = remote.and_then(|entry| entry.modified_at);
    match (local_time, remote_time) {
        (Some(lhs), Some(rhs)) => lhs > rhs,
        (Some(_), None) => true,
        (None, Some(_)) => false,
        (None, None) => {
            let local_size = local.and_then(|entry| entry.size).unwrap_or(0);
            let remote_size = remote.and_then(|entry| entry.size).unwrap_or(0);
            local_size >= remote_size
        }
    }
}

#[derive(Clone, Copy)]
enum ConflictOutcome {
    Upload,
    Download,
    DeleteLocal,
    DeleteRemote,
    Skip,
}

fn describe_conflict_action(action: ConflictOutcome) -> &'static str {
    match action {
        ConflictOutcome::Upload => "以本地版本覆盖云端",
        ConflictOutcome::Download => "以云端版本覆盖本地",
        ConflictOutcome::DeleteLocal => "按云端删除同步删除本地",
        ConflictOutcome::DeleteRemote => "按本地删除同步删除云端",
        ConflictOutcome::Skip => "冲突暂不处理",
    }
}

fn resolve_conflict(
    local_current: Option<&SyncSnapshotEntry>,
    remote_current: Option<&SyncSnapshotEntry>,
    local_previous: Option<&SyncSnapshotEntry>,
    remote_previous: Option<&SyncSnapshotEntry>,
    propagate_delete: bool,
    strategy: SyncConflictStrategy,
) -> ConflictOutcome {
    match (local_current, remote_current) {
        (Some(_), Some(_)) => match strategy {
            SyncConflictStrategy::PreferLocal => ConflictOutcome::Upload,
            SyncConflictStrategy::PreferRemote => ConflictOutcome::Download,
            SyncConflictStrategy::Newest => {
                if is_local_newer(local_current, remote_current) {
                    ConflictOutcome::Upload
                } else {
                    ConflictOutcome::Download
                }
            }
        },
        (Some(_), None) => match strategy {
            SyncConflictStrategy::PreferLocal => ConflictOutcome::Upload,
            SyncConflictStrategy::PreferRemote => {
                if propagate_delete {
                    ConflictOutcome::DeleteLocal
                } else {
                    ConflictOutcome::Skip
                }
            }
            SyncConflictStrategy::Newest => {
                let remote_ref = remote_current.or(remote_previous);
                if is_local_newer(local_current, remote_ref) {
                    ConflictOutcome::Upload
                } else if propagate_delete {
                    ConflictOutcome::DeleteLocal
                } else {
                    ConflictOutcome::Skip
                }
            }
        },
        (None, Some(_)) => match strategy {
            SyncConflictStrategy::PreferLocal => {
                if propagate_delete {
                    ConflictOutcome::DeleteRemote
                } else {
                    ConflictOutcome::Skip
                }
            }
            SyncConflictStrategy::PreferRemote => ConflictOutcome::Download,
            SyncConflictStrategy::Newest => {
                let local_ref = local_current.or(local_previous);
                if is_local_newer(local_ref, remote_current) {
                    if propagate_delete {
                        ConflictOutcome::DeleteRemote
                    } else {
                        ConflictOutcome::Skip
                    }
                } else {
                    ConflictOutcome::Download
                }
            }
        },
        (None, None) => ConflictOutcome::Skip,
    }
}

fn plan_bidirectional_actions(
    local_current: &[SyncSnapshotEntry],
    remote_current: &[SyncSnapshotEntry],
    local_previous: Option<&[SyncSnapshotEntry]>,
    remote_previous: Option<&[SyncSnapshotEntry]>,
    propagate_delete: bool,
    strategy: SyncConflictStrategy,
) -> BidirectionalPlan {
    let local_map = entries_to_map(local_current);
    let remote_map = entries_to_map(remote_current);
    let prev_local_map = entries_to_map(local_previous.unwrap_or(&[]));
    let prev_remote_map = entries_to_map(remote_previous.unwrap_or(&[]));
    let mut paths: HashSet<String> = HashSet::new();
    paths.extend(local_map.keys().cloned());
    paths.extend(remote_map.keys().cloned());
    paths.extend(prev_local_map.keys().cloned());
    paths.extend(prev_remote_map.keys().cloned());
    let mut plan = BidirectionalPlan::default();
    for path in paths {
        let local_current_entry = local_map.get(&path);
        let remote_current_entry = remote_map.get(&path);
        let local_previous_entry = prev_local_map.get(&path);
        let remote_previous_entry = prev_remote_map.get(&path);
        if let (Some(local_now), Some(remote_now)) = (&local_current_entry, &remote_current_entry) {
            if local_previous_entry.is_none()
                && remote_previous_entry.is_none()
                && snapshots_equal(local_now, remote_now)
            {
                continue;
            }
        }
        if let (Some(local_now), Some(remote_now)) = (&local_current_entry, &remote_current_entry) {
            if snapshots_equal(local_now, remote_now)
                && snapshots_equal(
                    local_previous_entry.unwrap_or(local_now),
                    remote_previous_entry.unwrap_or(remote_now),
                )
            {
                continue;
            }
        }
        let local_changed = has_snapshot_changed(local_current_entry, local_previous_entry);
        let remote_changed = has_snapshot_changed(remote_current_entry, remote_previous_entry);
        if !local_changed && !remote_changed {
            continue;
        }
        if local_changed && !remote_changed {
            if let Some(entry) = local_current_entry {
                plan.uploads.push(entry.clone());
            } else if propagate_delete {
                if let Some(remote_entry) = remote_current_entry {
                    plan.delete_remote.push(remote_entry.clone());
                }
            }
            continue;
        }
        if !local_changed && remote_changed {
            if let Some(entry) = remote_current_entry {
                plan.downloads.push(entry.clone());
            } else if propagate_delete {
                if let Some(entry) = local_current_entry {
                    plan.delete_local.push(entry.clone());
                } else if let Some(entry) = local_previous_entry {
                    plan.delete_local.push(entry.clone());
                }
            }
            continue;
        }
        let outcome = resolve_conflict(
            local_current_entry,
            remote_current_entry,
            local_previous_entry,
            remote_previous_entry,
            propagate_delete,
            strategy.clone(),
        );
        let message = format!("{} -> {}", path, describe_conflict_action(outcome));
        plan.conflicts.push(message);
        match outcome {
            ConflictOutcome::Upload => {
                if let Some(entry) = local_current_entry {
                    plan.uploads.push(entry.clone());
                }
            }
            ConflictOutcome::Download => {
                if let Some(entry) = remote_current_entry {
                    plan.downloads.push(entry.clone());
                }
            }
            ConflictOutcome::DeleteLocal => {
                if let Some(entry) = local_current_entry {
                    plan.delete_local.push(entry.clone());
                } else if let Some(entry) = local_previous_entry {
                    plan.delete_local.push(entry.clone());
                }
            }
            ConflictOutcome::DeleteRemote => {
                if let Some(entry) = remote_current_entry {
                    plan.delete_remote.push(entry.clone());
                }
            }
            ConflictOutcome::Skip => {}
        }
    }
    plan
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
struct CreateSyncTaskPayload {
    name: String,
    direction: SyncTaskDirection,
    group_id: String,
    #[serde(default)]
    group_name: Option<String>,
    tenant_id: String,
    #[serde(default)]
    tenant_name: Option<String>,
    remote_folder_token: String,
    remote_label: String,
    local_path: String,
    schedule: String,
    enabled: bool,
    detection: SyncDetectionMode,
    conflict: SyncConflictStrategy,
    #[serde(default = "default_true")]
    propagate_delete: bool,
    #[serde(default)]
    include_patterns: Vec<String>,
    #[serde(default)]
    exclude_patterns: Vec<String>,
    #[serde(default)]
    notes: Option<String>,
}

#[derive(Deserialize)]
struct UpdateSyncTaskPayload {
    task_id: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    direction: Option<SyncTaskDirection>,
    #[serde(default)]
    group_id: Option<String>,
    #[serde(default)]
    group_name: Option<String>,
    #[serde(default)]
    tenant_id: Option<String>,
    #[serde(default)]
    tenant_name: Option<String>,
    #[serde(default)]
    remote_folder_token: Option<String>,
    #[serde(default)]
    remote_label: Option<String>,
    #[serde(default)]
    local_path: Option<String>,
    #[serde(default)]
    schedule: Option<String>,
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    detection: Option<SyncDetectionMode>,
    #[serde(default)]
    conflict: Option<SyncConflictStrategy>,
    #[serde(default)]
    propagate_delete: Option<bool>,
    #[serde(default)]
    include_patterns: Option<Vec<String>>,
    #[serde(default)]
    exclude_patterns: Option<Vec<String>>,
    #[serde(default)]
    notes: Option<String>,
}

#[derive(Deserialize)]
struct DeleteSyncTaskPayload {
    task_id: String,
}

#[derive(Deserialize)]
struct TriggerSyncTaskPayload {
    task_id: String,
}

#[derive(Deserialize)]
struct SyncLogQueryPayload {
    task_id: String,
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct UpdateApiServerConfigPayload {
    listen_host: Option<String>,
    port: Option<u16>,
    timeout_secs: Option<u64>,
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
async fn get_api_service_config(state: State<'_, AppState>) -> Result<ApiServerStatus, String> {
    Ok(state.inner().api_server_status_snapshot())
}

#[tauri::command]
async fn update_api_service_config(
    state: State<'_, AppState>,
    payload: UpdateApiServerConfigPayload,
) -> Result<ApiServerStatus, String> {
    state
        .inner()
        .update_api_server_config(payload)
        .map_err(|e| e.to_string())?;
    Ok(state.inner().api_server_status_snapshot())
}

#[tauri::command]
async fn start_api_service(
    state: State<'_, AppState>,
    app: AppHandle,
) -> Result<ApiServerStatus, String> {
    state
        .inner()
        .start_api_service(&app)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn stop_api_service(state: State<'_, AppState>) -> Result<ApiServerStatus, String> {
    state
        .inner()
        .stop_api_service()
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn list_api_routes() -> Result<Vec<ApiDocEntry>, String> {
    Ok(build_api_docs())
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

#[tauri::command]
async fn list_sync_tasks(state: State<'_, AppState>, api_key: Option<String>) -> Result<Vec<SyncTaskRecord>, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    AppState::ensure_admin(&scope).map_err(|e| e.to_string())?;
    Ok(state.inner().list_sync_tasks_internal())
}

#[tauri::command]
async fn create_sync_task(
    state: State<'_, AppState>,
    api_key: Option<String>,
    payload: CreateSyncTaskPayload,
) -> Result<SyncTaskRecord, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    AppState::ensure_admin(&scope).map_err(|e| e.to_string())?;
    state
        .inner()
        .create_sync_task_record(payload)
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn update_sync_task(
    state: State<'_, AppState>,
    api_key: Option<String>,
    payload: UpdateSyncTaskPayload,
) -> Result<SyncTaskRecord, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    AppState::ensure_admin(&scope).map_err(|e| e.to_string())?;
    let task_id = payload.task_id.clone();
    state
        .inner()
        .update_sync_task_record(&task_id, |task| {
            let mut reset_reason: Option<String> = None;
            if let Some(name) = payload.name.clone() {
                task.name = name;
            }
            if let Some(direction) = payload.direction.clone() {
                if task.direction != direction {
                    reset_reason
                        .get_or_insert_with(|| "同步方向已更新，等待重新同步。".into());
                }
                task.direction = direction;
            }
            if let Some(group_id) = payload.group_id.clone() {
                task.group_id = group_id;
            }
            if payload.group_name.is_some() {
                task.group_name = payload.group_name.clone();
            }
            if let Some(tenant_id) = payload.tenant_id.clone() {
                task.tenant_id = tenant_id;
            }
            if payload.tenant_name.is_some() {
                task.tenant_name = payload.tenant_name.clone();
            }
            if let Some(remote_token) = payload.remote_folder_token.clone() {
                if task.remote_folder_token != remote_token {
                    reset_reason
                        .get_or_insert_with(|| "云端目录已更新，等待重新同步。".into());
                }
                task.remote_folder_token = remote_token;
            }
            if let Some(remote_label) = payload.remote_label.clone() {
                task.remote_label = remote_label;
            }
            if let Some(local_path) = payload.local_path.clone() {
                if task.local_path != local_path {
                    reset_reason
                        .get_or_insert_with(|| "本地目录已更新，等待重新同步。".into());
                }
                task.local_path = local_path;
            }
            if let Some(schedule) = payload.schedule.clone() {
                task.schedule = schedule;
            }
            if let Some(enabled) = payload.enabled {
                task.enabled = enabled;
            }
            if let Some(detection) = payload.detection.clone() {
                task.detection = detection;
            }
            if let Some(conflict) = payload.conflict.clone() {
                task.conflict = conflict;
            }
            if let Some(propagate) = payload.propagate_delete {
                task.propagate_delete = propagate;
            }
            if let Some(include) = payload.include_patterns.clone() {
                task.include_patterns = include;
            }
            if let Some(exclude) = payload.exclude_patterns.clone() {
                task.exclude_patterns = exclude;
            }
            if payload.notes.is_some() {
                task.notes = payload.notes.clone();
            }
            if let Some(reason) = reset_reason {
                reset_task_snapshots(task, &reason);
            }
        })
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn delete_sync_task(
    state: State<'_, AppState>,
    api_key: Option<String>,
    payload: DeleteSyncTaskPayload,
) -> Result<(), String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    AppState::ensure_admin(&scope).map_err(|e| e.to_string())?;
    state
        .inner()
        .remove_sync_task_record(&payload.task_id)
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn trigger_sync_task(
    state: State<'_, AppState>,
    app: AppHandle,
    api_key: Option<String>,
    payload: TriggerSyncTaskPayload,
) -> Result<SyncTaskRecord, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    AppState::ensure_admin(&scope).map_err(|e| e.to_string())?;
    let direction = {
        let tasks = state.inner().sync_tasks.read();
        tasks
            .get(&payload.task_id)
            .map(|task| task.direction.clone())
            .ok_or_else(|| AppError::Message("任务不存在".into()))
    }
    .map_err(|e| e.to_string())?;
    state
        .inner()
        .update_sync_task_record(&payload.task_id, |task| {
            task.last_status = SyncTaskStatus::Running;
            task.last_run_at = Some(Utc::now());
            task.last_message = Some("同步任务准备执行".into());
        })
        .map_err(|e| e.to_string())?;
    let run_result = match direction {
        SyncTaskDirection::LocalToCloud => {
            state.inner().run_local_to_cloud_sync(&payload.task_id, &app).await
        }
        SyncTaskDirection::CloudToLocal => {
            state.inner().run_cloud_to_local_sync(&payload.task_id, &app).await
        }
        SyncTaskDirection::Bidirectional => {
            state.inner().run_bidirectional_sync(&payload.task_id, &app).await
        }
    };
    match run_result {
        Ok(_) => {
            let finished = {
                let tasks = state.inner().sync_tasks.read();
                tasks
                    .get(&payload.task_id)
                    .cloned()
                    .ok_or_else(|| "任务不存在".to_string())?
            };
            let _ = state.inner().append_sync_log(SyncLogEntry {
                task_id: payload.task_id.clone(),
                timestamp: Utc::now(),
                level: "info".into(),
                message: "同步任务完成".into(),
            });
            Ok(finished)
        }
        Err(err) => {
            let message = err.to_string();
            let _ = state.inner().append_sync_log(SyncLogEntry {
                task_id: payload.task_id.clone(),
                timestamp: Utc::now(),
                level: "error".into(),
                message: message.clone(),
            });
            let _ = state.inner().update_sync_task_record(&payload.task_id, |task| {
                task.last_status = SyncTaskStatus::Failed;
                task.last_message = Some(message.clone());
                task.last_run_at = Some(Utc::now());
            });
            Err(message)
        }
    }
}

#[tauri::command]
async fn list_sync_logs(
    state: State<'_, AppState>,
    api_key: Option<String>,
    payload: SyncLogQueryPayload,
) -> Result<Vec<SyncLogEntry>, String> {
    let scope = state.verify_api_key(api_key).map_err(|e| e.to_string())?;
    AppState::ensure_admin(&scope).map_err(|e| e.to_string())?;
    let limit = payload.limit.unwrap_or(100).min(500);
    Ok(state.inner().list_sync_logs_by_task(&payload.task_id, limit))
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

async fn run_api_http_server(app: AppHandle, addr: SocketAddr, timeout: TokioDuration, shutdown: oneshot::Receiver<()>) {
    let router_state = ApiRouterState { app, timeout };
    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(err) => {
            eprintln!("API server bind error: {}", err);
            return;
        }
    };
    let cors = CorsLayer::new()
        .allow_methods(Any)
        .allow_headers(Any)
        .allow_origin(Any);
    let router = Router::new()
        .route("/health", get(api_health_handler))
        .route("/docs", get(api_docs_handler))
        .route("/command/:name", post(api_dispatch_handler))
        .with_state(router_state)
        .layer(cors);
    let server = axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            let _ = shutdown.await;
        });
    if let Err(err) = server.await {
        eprintln!("API server error: {}", err);
    }
}

async fn api_health_handler() -> Json<Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn api_docs_handler() -> Json<Value> {
    Json(serde_json::json!({ "commands": build_api_docs() }))
}

async fn api_dispatch_handler(
    AxumPath(name): AxumPath<String>,
    AxumState(state): AxumState<ApiRouterState>,
    headers: HeaderMap,
    Json(body): Json<ApiCommandBody>,
) -> (AxumStatusCode, Json<Value>) {
    let api_key = headers
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .map(|s| s.to_string())
        .or(body.api_key.clone());
    let key = match api_key {
        Some(value) => value,
        None => {
            return (
                AxumStatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "缺少 API Key" })),
            );
        }
    };
    let fut = dispatch_api_command(&state.app, &name, key, body.payload);
    match timeout(state.timeout, fut).await {
        Ok(Ok(value)) => (
            AxumStatusCode::OK,
            Json(serde_json::json!({ "data": value })),
        ),
        Ok(Err(message)) => (
            AxumStatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": message })),
        ),
        Err(_) => (
            AxumStatusCode::REQUEST_TIMEOUT,
            Json(serde_json::json!({ "error": "请求超时" })),
        ),
    }
}

async fn dispatch_api_command(
    app: &AppHandle,
    command: &str,
    api_key: String,
    payload: Option<Value>,
) -> Result<Value, String> {
    match command {
        "list_tenants" => {
            let state = app.state::<AppState>();
            let result = list_tenants(state, Some(api_key.clone())).await?;
            to_json_value(result)
        }
        "add_tenant" => {
            let data: TenantPayload = parse_payload(&payload)?;
            let state = app.state::<AppState>();
            let result = add_tenant(state, Some(api_key.clone()), data).await?;
            to_json_value(result)
        }
        "refresh_tenant_token" => {
            let tenant_id = parse_string_field(&payload, "tenant_id")?;
            let state = app.state::<AppState>();
            let result = refresh_tenant_token(state, Some(api_key.clone()), tenant_id).await?;
            to_json_value(result)
        }
        "list_root_entries" => {
            #[derive(Deserialize, Default)]
            struct ListRootPayload {
                tenant_id: Option<String>,
                aggregate: Option<bool>,
            }
            let data: ListRootPayload = deserialize_or_default(&payload)?;
            let state = app.state::<AppState>();
            let result = list_root_entries(
                state,
                Some(api_key.clone()),
                data.tenant_id,
                data.aggregate,
            )
            .await?;
            Ok(result)
        }
        "list_folder_entries" => {
            let token = parse_string_field(&payload, "folder_token")?;
            let state = app.state::<AppState>();
            let result = list_folder_entries(state, Some(api_key.clone()), token).await?;
            to_json_value(result)
        }
        "search_entries" => {
            #[derive(Deserialize)]
            struct SearchPayload {
                keyword: String,
                tenant_id: Option<String>,
                root_name: Option<String>,
            }
            let data: SearchPayload = parse_payload(&payload)?;
            let state = app.state::<AppState>();
            let result = search_entries(
                state,
                Some(api_key.clone()),
                data.keyword,
                data.tenant_id,
                data.root_name,
            )
            .await?;
            to_json_value(result)
        }
        "delete_file" => {
            let data: DeleteFilePayload = parse_payload(&payload)?;
            let state = app.state::<AppState>();
            delete_file(state, Some(api_key.clone()), data).await?;
            Ok(Value::Null)
        }
        "create_folder" => {
            let data: CreateFolderPayload = parse_payload(&payload)?;
            let state = app.state::<AppState>();
            let result = create_folder(state, Some(api_key.clone()), data).await?;
            to_json_value(result)
        }
        "upload_file" => {
            let data: UploadFilePayload = parse_payload(&payload)?;
            let state = app.state::<AppState>();
            let result = upload_file(app.clone(), state, Some(api_key.clone()), data).await?;
            Ok(Value::String(result))
        }
        "upload_folder" => {
            let data: UploadFolderPayload = parse_payload(&payload)?;
            let state = app.state::<AppState>();
            upload_folder(app.clone(), state, Some(api_key.clone()), data).await?;
            Ok(Value::Null)
        }
        "download_file" => {
            let data: DownloadFilePayload = parse_payload(&payload)?;
            let state = app.state::<AppState>();
            let result = download_file(app.clone(), state, Some(api_key.clone()), data).await?;
            Ok(Value::String(result))
        }
        "download_folder" => {
            let data: DownloadFolderPayload = parse_payload(&payload)?;
            let state = app.state::<AppState>();
            let result = download_folder(app.clone(), state, Some(api_key.clone()), data).await?;
            Ok(Value::String(result))
        }
        "move_file" => {
            let data: MoveFilePayload = parse_payload(&payload)?;
            let state = app.state::<AppState>();
            let result = move_file(state, Some(api_key.clone()), data).await?;
            to_json_value(result)
        }
        "copy_file" => {
            let data: CopyFilePayload = parse_payload(&payload)?;
            let state = app.state::<AppState>();
            let result = copy_file(state, Some(api_key.clone()), data).await?;
            to_json_value(result)
        }
        "rename_file" => {
            let data: RenameFilePayload = parse_payload(&payload)?;
            let state = app.state::<AppState>();
            rename_file(state, Some(api_key.clone()), data).await?;
            Ok(Value::Null)
        }
        "list_sync_tasks" => {
            let state = app.state::<AppState>();
            let result = list_sync_tasks(state, Some(api_key.clone())).await?;
            to_json_value(result)
        }
        "create_sync_task" => {
            let data: CreateSyncTaskPayload = parse_payload(&payload)?;
            let state = app.state::<AppState>();
            let result = create_sync_task(state, Some(api_key.clone()), data).await?;
            to_json_value(result)
        }
        "update_sync_task" => {
            let data: UpdateSyncTaskPayload = parse_payload(&payload)?;
            let state = app.state::<AppState>();
            let result = update_sync_task(state, Some(api_key.clone()), data).await?;
            to_json_value(result)
        }
        "delete_sync_task" => {
            let data: DeleteSyncTaskPayload = parse_payload(&payload)?;
            let state = app.state::<AppState>();
            delete_sync_task(state, Some(api_key.clone()), data).await?;
            Ok(Value::Null)
        }
        "trigger_sync_task" => {
            let data: TriggerSyncTaskPayload = parse_payload(&payload)?;
            let state = app.state::<AppState>();
            let result =
                trigger_sync_task(state, app.clone(), Some(api_key.clone()), data).await?;
            to_json_value(result)
        }
        "list_sync_logs" => {
            let data: SyncLogQueryPayload = parse_payload(&payload)?;
            let state = app.state::<AppState>();
            let result = list_sync_logs(state, Some(api_key.clone()), data).await?;
            to_json_value(result)
        }
        "inspect_local_path" => {
            let path = parse_string_field(&payload, "path")?;
            let result = inspect_local_path(path)?;
            to_json_value(result)
        }
        "reveal_local_path" => {
            let path = parse_string_field(&payload, "path")?;
            reveal_local_path(path)?;
            Ok(Value::Null)
        }
        "get_api_key" => {
            let state = app.state::<AppState>();
            let result = get_api_key(state).await?;
            to_json_value(result)
        }
        "update_api_key" => {
            let data: UpdateKeyPayload = parse_payload(&payload)?;
            let state = app.state::<AppState>();
            update_api_key(state, data).await?;
            Ok(Value::Null)
        }
        "get_tenant_detail" => {
            let tenant_id = parse_string_field(&payload, "tenant_id")?;
            let state = app.state::<AppState>();
            let result = get_tenant_detail(state, Some(api_key.clone()), tenant_id).await?;
            to_json_value(result)
        }
        "update_tenant_meta" => {
            let data: UpdateTenantPayload = parse_payload(&payload)?;
            let state = app.state::<AppState>();
            let result = update_tenant_meta(state, Some(api_key.clone()), data).await?;
            to_json_value(result)
        }
        "remove_tenant" => {
            let data: RemoveTenantPayload = parse_payload(&payload)?;
            let state = app.state::<AppState>();
            remove_tenant(state, Some(api_key.clone()), data).await?;
            Ok(Value::Null)
        }
        "reorder_tenants" => {
            let data: Vec<ReorderTenant> = parse_payload(&payload)?;
            let state = app.state::<AppState>();
            reorder_tenants(state, Some(api_key.clone()), data).await?;
            Ok(Value::Null)
        }
        "list_groups" => {
            let state = app.state::<AppState>();
            let result = list_groups(state, Some(api_key.clone())).await?;
            to_json_value(result)
        }
        "add_group" => {
            let data: GroupPayload = parse_payload(&payload)?;
            let state = app.state::<AppState>();
            let result = add_group(state, Some(api_key.clone()), data).await?;
            to_json_value(result)
        }
        "update_group" => {
            let data: UpdateGroupPayload = parse_payload(&payload)?;
            let state = app.state::<AppState>();
            let result = update_group(state, Some(api_key.clone()), data).await?;
            to_json_value(result)
        }
        "delete_group" => {
            let data: RemoveGroupPayload = parse_payload(&payload)?;
            let state = app.state::<AppState>();
            delete_group(state, Some(api_key.clone()), data).await?;
            Ok(Value::Null)
        }
        "regenerate_group_key" => {
            let group_id = parse_string_field(&payload, "group_id")?;
            let state = app.state::<AppState>();
            let result = regenerate_group_key(state, Some(api_key.clone()), group_id).await?;
            to_json_value(result)
        }
        "list_transfer_tasks" => {
            let state = app.state::<AppState>();
            let result = list_transfer_tasks(state, Some(api_key.clone())).await?;
            to_json_value(result)
        }
        "clear_transfer_history" => {
            #[derive(Deserialize, Default)]
            struct ClearPayload {
                mode: Option<String>,
            }
            let data: ClearPayload = deserialize_or_default(&payload)?;
            let state = app.state::<AppState>();
            let result =
                clear_transfer_history(state, Some(api_key.clone()), data.mode).await?;
            to_json_value(result)
        }
        "pause_active_transfer" => {
            let task_id = parse_string_field(&payload, "task_id")?;
            let state = app.state::<AppState>();
            let result =
                pause_active_transfer(app.clone(), state, Some(api_key.clone()), task_id).await?;
            to_json_value(result)
        }
        "cancel_transfer_task" => {
            let task_id = parse_string_field(&payload, "task_id")?;
            let state = app.state::<AppState>();
            let result =
                cancel_transfer_task(app.clone(), state, Some(api_key.clone()), task_id).await?;
            to_json_value(result)
        }
        "delete_transfer_task" => {
            let task_id = parse_string_field(&payload, "task_id")?;
            let state = app.state::<AppState>();
            delete_transfer_task(state, Some(api_key.clone()), task_id).await?;
            Ok(Value::Null)
        }
        "resume_transfer_task" => {
            let task_id = parse_string_field(&payload, "task_id")?;
            let state = app.state::<AppState>();
            resume_transfer_task(app.clone(), state, Some(api_key.clone()), task_id).await?;
            Ok(Value::Null)
        }
        "proxy_official_api" => {
            let data: ProxyRequest = parse_payload(&payload)?;
            let state = app.state::<AppState>();
            let result = proxy_official_api(state, Some(api_key.clone()), data).await?;
            Ok(result)
        }
        "pick_files_dialog" => {
            let data: PickFilesPayload = parse_payload(&payload)?;
            let result = pick_files_dialog(data).await?;
            to_json_value(result)
        }
        "pick_directory_dialog" => {
            let result = pick_directory_dialog().await?;
            to_json_value(result)
        }
        "pick_entries_dialog" => {
            let data: PickEntriesPayload = parse_payload(&payload)?;
            let result = pick_entries_dialog(data).await?;
            to_json_value(result)
        }
        _ => Err("未知的 API 命令".into()),
    }
}

fn parse_payload<T: DeserializeOwned>(payload: &Option<Value>) -> Result<T, String> {
    payload
        .clone()
        .ok_or_else(|| "缺少 payload".to_string())
        .and_then(|value| serde_json::from_value(value).map_err(|e| e.to_string()))
}

fn deserialize_or_default<T: DeserializeOwned + Default>(payload: &Option<Value>) -> Result<T, String> {
    match payload.clone() {
        Some(value) => serde_json::from_value(value).map_err(|e| e.to_string()),
        None => Ok(T::default()),
    }
}

fn parse_string_field(payload: &Option<Value>, field: &str) -> Result<String, String> {
    let value = payload
        .as_ref()
        .and_then(|data| data.get(field))
        .cloned()
        .ok_or_else(|| format!("缺少字段 {}", field))?;
    match value {
        Value::String(text) => Ok(text),
        _ => serde_json::from_value(value).map_err(|e| e.to_string()),
    }
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
            let base_dir = if cfg!(debug_assertions) {
                env::current_dir().unwrap_or_else(|_| env::temp_dir().join("feisync-dev"))
            } else {
                env::var("HOME")
                    .ok()
                    .map(|home| {
                        let mut path = PathBuf::from(home);
                        #[cfg(target_os = "macos")]
                        {
                            path.push("Library");
                            path.push("Application Support");
                        }
                        #[cfg(target_os = "windows")]
                        {
                            path.push("AppData");
                            path.push("Roaming");
                        }
                        #[cfg(all(not(target_os = "macos"), not(target_os = "windows")))]
                        {
                            path.push(".config");
                        }
                        path.push("FeiSync");
                        path
                    })
                    .unwrap_or_else(|| env::temp_dir().join("feisync"))
            };
            fs::create_dir_all(&base_dir).map_err(tauri::Error::Io)?;
            app.manage(AppState::new(base_dir));
            {
                let cloned = app.handle().clone();
                tauri::async_runtime::spawn(async move {
                    let state = cloned.state::<AppState>();
                    if let Err(err) = state.inner().start_api_service(&cloned).await {
                        eprintln!("API server auto-start failed: {}", err);
                    }
                });
            }
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
            get_api_service_config,
            update_api_service_config,
            start_api_service,
            stop_api_service,
            list_api_routes,
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
            list_sync_tasks,
            create_sync_task,
            update_sync_task,
            delete_sync_task,
            trigger_sync_task,
            list_sync_logs,
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
