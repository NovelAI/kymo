use std::path::Path;

use anyhow::{Context, Result};

pub const CLICKHOUSE_USER: &str = "mkdb2";

pub fn postgresql_configuration() -> Vec<(String, String)> {
    [
        ("listen_addresses", ""),
        ("unix_socket_permissions", "0700"),
        ("max_connections", "32"),
        ("shared_buffers", "128MB"),
        ("effective_cache_size", "512MB"),
        ("work_mem", "8MB"),
        ("maintenance_work_mem", "64MB"),
        ("wal_buffers", "16MB"),
        ("checkpoint_timeout", "15min"),
        ("max_wal_size", "1GB"),
        ("fsync", "on"),
        ("synchronous_commit", "on"),
        ("full_page_writes", "on"),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), value.to_owned()))
    .collect()
}

pub fn clickhouse_users_xml(password: &str) -> String {
    let password = xml_text(password);
    format!(
        r#"<clickhouse>
  <profiles>
    <default>
      <max_memory_usage>1073741824</max_memory_usage>
      <compile_expressions>0</compile_expressions>
      <compile_aggregate_expressions>0</compile_aggregate_expressions>
    </default>
  </profiles>
  <users>
    <default>
      <password_sha256_hex>0000000000000000000000000000000000000000000000000000000000000000</password_sha256_hex>
      <networks><ip>127.0.0.2</ip></networks>
      <profile>default</profile><quota>default</quota>
    </default>
    <{CLICKHOUSE_USER}>
      <password>{password}</password>
      <networks><ip>127.0.0.1</ip><ip>::1</ip></networks>
      <profile>default</profile><quota>default</quota>
    </{CLICKHOUSE_USER}>
  </users>
  <quotas><default><interval><duration>3600</duration><queries>0</queries><errors>0</errors><result_rows>0</result_rows><read_rows>0</read_rows><execution_time>0</execution_time></interval></default></quotas>
</clickhouse>
"#
    )
}

pub fn clickhouse_config_xml(
    root: &Path,
    port: u16,
    cert: &Path,
    key: &Path,
    users: &Path,
) -> Result<String> {
    // ClickHouse binds the numeric loopback interface, but kymo-server deliberately connects as `localhost` so webpki matches the generated DNS:localhost SAN. Browser listeners use 127.0.0.1 instead because it is part of their exact Origin contract.
    let root = xml_path(root)?;
    let cert = xml_path(cert)?;
    let key = xml_path(key)?;
    let users = xml_path(users)?;
    Ok(format!(
        r#"<clickhouse>
  <logger><level>information</level><console>1</console></logger>
  <path>{root}/data/</path>
  <tmp_path>{root}/tmp/</tmp_path>
  <user_files_path>{root}/user-files/</user_files_path>
  <format_schema_path>{root}/format-schemas/</format_schema_path>
  <pid_file>{root}/clickhouse.pid</pid_file>
  <users_config>{users}</users_config>
  <listen_host>127.0.0.1</listen_host>
  <https_port>{port}</https_port>
  <max_server_memory_usage_to_ram_ratio>0.4</max_server_memory_usage_to_ram_ratio>
  <background_pool_size>16</background_pool_size>
  <background_schedule_pool_size>4</background_schedule_pool_size>
  <background_message_broker_schedule_pool_size>1</background_message_broker_schedule_pool_size>
  <background_distributed_schedule_pool_size>1</background_distributed_schedule_pool_size>
  <openSSL>
    <server>
      <certificateFile>{cert}</certificateFile>
      <privateKeyFile>{key}</privateKeyFile>
      <verificationMode>none</verificationMode>
      <cacheSessions>true</cacheSessions>
      <disableProtocols>sslv2,sslv3,tlsv1,tlsv1_1</disableProtocols>
      <preferServerCiphers>true</preferServerCiphers>
    </server>
  </openSSL>
</clickhouse>
"#
    ))
}

fn xml_path(path: &Path) -> Result<String> {
    let value = path
        .to_str()
        .with_context(|| format!("local-runtime path is not UTF-8: {}", path.display()))?;
    Ok(xml_text(value))
}

fn xml_text(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profiles_keep_durability_and_listener_constraints() {
        let postgres = postgresql_configuration();
        assert!(postgres.contains(&("listen_addresses".to_owned(), String::new())));
        assert!(postgres.contains(&("fsync".to_owned(), "on".to_owned())));
        assert!(postgres.contains(&("max_connections".to_owned(), "32".to_owned())));
        let clickhouse = clickhouse_config_xml(
            Path::new("/private/root&data"),
            18123,
            Path::new("/private/cert"),
            Path::new("/private/key"),
            Path::new("/private/users.xml"),
        )
        .unwrap();
        assert!(clickhouse.contains("<listen_host>127.0.0.1</listen_host>"));
        assert!(clickhouse.contains("<https_port>18123</https_port>"));
        assert!(clickhouse.contains("<pid_file>/private/root&amp;data/clickhouse.pid</pid_file>"));
        assert!(clickhouse.contains("/private/root&amp;data"));
        assert!(!clickhouse.contains("<http_port>"));
        let users = clickhouse_users_xml("a&<password");
        assert!(users.contains("a&amp;&lt;password"));
        assert!(!users.contains("disabled-default-user"));
        // kymo-server never manages users, roles, or grants.
        assert!(!users.contains("access_management"));
        assert!(users.contains("<password_sha256_hex>0000000000000000000000000000000000000000000000000000000000000000</password_sha256_hex>"));
    }
}
