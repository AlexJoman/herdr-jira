//! Minimal Jira REST client (blocking, ureq).
//!
//! Works against both Jira Cloud and Server/Data Center:
//! - search first tries the new `/rest/api/2/search/jql` endpoint (Cloud replaced
//!   the classic `/search` with it in 2025), then falls back to the classic
//!   `/rest/api/2/search` (Server/DC and older instances);
//! - the v2 API returns descriptions as plain text on Server/DC, but some Cloud
//!   responses carry Atlassian Document Format (ADF) objects — both are handled.

use base64::Engine;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct Issue {
    pub key: String,
    pub summary: String,
    pub status: String,
    pub status_category: String, // "new" | "indeterminate" | "done"
    pub issue_type: String,
    pub priority: String,
    pub assignee: String,
    pub reporter: String,
    pub updated: String,
    pub labels: Vec<String>,
    pub description: String,
    pub url: String,
}

#[derive(Debug, Clone)]
pub struct Transition {
    pub id: String,
    pub name: String,
    pub to_status: String,
}

/// A user an issue can be assigned to. Cloud identifies users by
/// `accountId`; Server/DC by username (`name`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JiraUser {
    pub id: String,
    pub display_name: String,
    pub email: String,
    pub cloud: bool,
}

#[derive(Clone)]
pub struct JiraClient {
    base: String,
    auth_header: String,
    max_results: u32,
    agent: ureq::Agent,
}

const FIELDS: &str = "summary,status,issuetype,priority,assignee,reporter,updated,labels,description";

impl JiraClient {
    pub fn new(cfg: &crate::config::Config) -> Result<Self, String> {
        let token = cfg.resolve_token()?;
        let auth_header = match cfg.jira.auth.as_str() {
            "bearer" => format!("Bearer {token}"),
            "basic" => {
                if cfg.jira.email.trim().is_empty() {
                    return Err("auth = \"basic\" requires [jira].email".into());
                }
                let creds = format!("{}:{}", cfg.jira.email.trim(), token);
                format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD.encode(creds)
                )
            }
            other => return Err(format!("unknown [jira].auth \"{other}\" (use basic|bearer)")),
        };
        Ok(Self {
            base: cfg.jira.base_url.clone(),
            auth_header,
            max_results: cfg.jira.max_results.max(1),
            agent: ureq::AgentBuilder::new()
                .timeout(std::time::Duration::from_secs(20))
                .build(),
        })
    }

    fn get(&self, path: &str, query: &[(&str, &str)]) -> Result<Value, String> {
        let mut req = self
            .agent
            .get(&format!("{}{}", self.base, path))
            .set("Authorization", &self.auth_header)
            .set("Accept", "application/json");
        for (k, v) in query {
            req = req.query(k, v);
        }
        Self::finish(req.call())
    }

    fn post(&self, path: &str, body: Value) -> Result<Value, String> {
        let req = self
            .agent
            .post(&format!("{}{}", self.base, path))
            .set("Authorization", &self.auth_header)
            .set("Accept", "application/json");
        Self::finish(req.send_json(body))
    }

    fn put(&self, path: &str, body: Value) -> Result<Value, String> {
        let req = self
            .agent
            .put(&format!("{}{}", self.base, path))
            .set("Authorization", &self.auth_header)
            .set("Accept", "application/json");
        Self::finish(req.send_json(body))
    }

    fn finish(res: Result<ureq::Response, ureq::Error>) -> Result<Value, String> {
        match res {
            Ok(resp) => {
                let text = resp.into_string().map_err(|e| format!("read body: {e}"))?;
                if text.trim().is_empty() {
                    return Ok(Value::Null);
                }
                serde_json::from_str(&text).map_err(|e| format!("bad JSON from Jira: {e}"))
            }
            Err(ureq::Error::Status(code, resp)) => {
                let body = resp.into_string().unwrap_or_default();
                Err(format!("HTTP {code}: {}", extract_error(&body)))
            }
            Err(e) => Err(format!("request failed: {e}")),
        }
    }

    pub fn search(&self, jql: &str) -> Result<Vec<Issue>, String> {
        let max = self.max_results.to_string();
        let query: &[(&str, &str)] = &[
            ("jql", jql),
            ("maxResults", &max),
            ("fields", FIELDS),
        ];
        // New endpoint first (Jira Cloud), classic /search as fallback (Server/DC).
        let result = match self.get("/rest/api/2/search/jql", query) {
            Ok(v) => Ok(v),
            Err(e) if e.starts_with("HTTP 404") || e.starts_with("HTTP 405") || e.starts_with("HTTP 410") => {
                self.get("/rest/api/2/search", query)
            }
            Err(e) => Err(e),
        }?;
        let issues = result["issues"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|v| self.parse_issue(v))
            .collect();
        Ok(issues)
    }

    fn parse_issue(&self, v: &Value) -> Issue {
        let f = &v["fields"];
        let key = v["key"].as_str().unwrap_or("?").to_string();
        let person = |p: &Value| -> String {
            p["displayName"]
                .as_str()
                .or_else(|| p["name"].as_str())
                .unwrap_or("—")
                .to_string()
        };
        Issue {
            url: format!("{}/browse/{}", self.base, key),
            key,
            summary: f["summary"].as_str().unwrap_or("").to_string(),
            status: f["status"]["name"].as_str().unwrap_or("?").to_string(),
            status_category: f["status"]["statusCategory"]["key"]
                .as_str()
                .unwrap_or("")
                .to_string(),
            issue_type: f["issuetype"]["name"].as_str().unwrap_or("").to_string(),
            priority: f["priority"]["name"].as_str().unwrap_or("—").to_string(),
            assignee: person(&f["assignee"]),
            reporter: person(&f["reporter"]),
            updated: f["updated"]
                .as_str()
                .map(|s| s.chars().take(16).collect::<String>().replace('T', " "))
                .unwrap_or_default(),
            labels: f["labels"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|l| l.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            description: description_text(&f["description"]),
        }
    }

    pub fn transitions(&self, key: &str) -> Result<Vec<Transition>, String> {
        let v = self.get(&format!("/rest/api/2/issue/{key}/transitions"), &[])?;
        Ok(v["transitions"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|t| Transition {
                id: t["id"].as_str().unwrap_or("").to_string(),
                name: t["name"].as_str().unwrap_or("?").to_string(),
                to_status: t["to"]["name"].as_str().unwrap_or("?").to_string(),
            })
            .collect())
    }

    pub fn apply_transition(&self, key: &str, transition_id: &str) -> Result<(), String> {
        self.post(
            &format!("/rest/api/2/issue/{key}/transitions"),
            serde_json::json!({ "transition": { "id": transition_id } }),
        )?;
        Ok(())
    }

    /// Priority names in the instance's configured order, highest first.
    pub fn priorities(&self) -> Result<Vec<String>, String> {
        Ok(parse_priorities(&self.get("/rest/api/2/priority", &[])?))
    }

    /// The authenticated user — the target of "assign to me".
    pub fn myself(&self) -> Result<JiraUser, String> {
        let v = self.get("/rest/api/2/myself", &[])?;
        parse_user(&v).ok_or_else(|| "could not identify the current user".into())
    }

    /// Users assignable to `key` whose name/email matches `query` (empty =
    /// first page of all assignable users).
    pub fn assignable_users(
        &self,
        key: &str,
        query: &str,
        cloud: bool,
    ) -> Result<Vec<JiraUser>, String> {
        let v = self.get(
            "/rest/api/2/user/assignable/search",
            &assignable_query(key, query, cloud),
        )?;
        Ok(parse_users(&v))
    }

    /// Assign `key` to `user`, or unassign it with `None`.
    pub fn assign(&self, key: &str, user: Option<&JiraUser>, cloud: bool) -> Result<(), String> {
        self.put(
            &format!("/rest/api/2/issue/{key}/assignee"),
            assignee_body(user, cloud),
        )?;
        Ok(())
    }
}

/// Query string for the assignable-user search. Cloud takes the search text
/// as `query`; Server/DC as `username`.
fn assignable_query<'a>(key: &'a str, query: &'a str, cloud: bool) -> Vec<(&'a str, &'a str)> {
    let mut q = vec![("issueKey", key), ("maxResults", "50")];
    if !query.is_empty() {
        q.push((if cloud { "query" } else { "username" }, query));
    }
    q
}

/// Request body for `PUT /issue/{key}/assignee`; a null id unassigns.
fn assignee_body(user: Option<&JiraUser>, cloud: bool) -> Value {
    let field = if cloud { "accountId" } else { "name" };
    let mut body = serde_json::Map::new();
    body.insert(
        field.into(),
        user.map_or(Value::Null, |u| Value::String(u.id.clone())),
    );
    Value::Object(body)
}

fn parse_user(v: &Value) -> Option<JiraUser> {
    let (id, cloud) = match (v["accountId"].as_str(), v["name"].as_str()) {
        (Some(id), _) if !id.is_empty() => (id, true),
        (_, Some(name)) if !name.is_empty() => (name, false),
        _ => return None,
    };
    Some(JiraUser {
        id: id.to_string(),
        display_name: v["displayName"].as_str().unwrap_or(id).to_string(),
        email: v["emailAddress"].as_str().unwrap_or("").to_string(),
        cloud,
    })
}

/// Assignable-user search results; inactive accounts are dropped.
fn parse_users(v: &Value) -> Vec<JiraUser> {
    v.as_array()
        .map(|a| {
            a.iter()
                .filter(|u| u["active"].as_bool() != Some(false))
                .filter_map(parse_user)
                .collect()
        })
        .unwrap_or_default()
}

/// `GET /priority` lists priorities in the order Jira ranks them.
fn parse_priorities(v: &Value) -> Vec<String> {
    v.as_array()
        .map(|a| {
            a.iter()
                .filter_map(|p| p["name"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// Jira error payloads look like {"errorMessages":[...],"errors":{...}}.
fn extract_error(body: &str) -> String {
    if let Ok(v) = serde_json::from_str::<Value>(body) {
        let mut parts: Vec<String> = Vec::new();
        if let Some(msgs) = v["errorMessages"].as_array() {
            parts.extend(msgs.iter().filter_map(|m| m.as_str().map(String::from)));
        }
        if let Some(errs) = v["errors"].as_object() {
            parts.extend(errs.iter().map(|(k, val)| {
                format!("{k}: {}", val.as_str().unwrap_or_default())
            }));
        }
        if !parts.is_empty() {
            return parts.join("; ");
        }
    }
    let trimmed = body.trim();
    if trimmed.is_empty() {
        "no error body".into()
    } else {
        trimmed.chars().take(300).collect()
    }
}

/// Description is a plain string on API v2 (Server/DC), but may arrive as an
/// ADF document object from Cloud — flatten either to displayable text.
fn description_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Object(_) => {
            let mut out = String::new();
            adf_walk(v, &mut out);
            out.trim().to_string()
        }
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn description_plain_string_passes_through() {
        assert_eq!(description_text(&Value::String("hi\nthere".into())), "hi\nthere");
        assert_eq!(description_text(&Value::Null), "");
    }

    #[test]
    fn description_adf_flattens_to_text() {
        let adf: Value = serde_json::json!({
            "type": "doc", "version": 1,
            "content": [
                {"type": "paragraph", "content": [
                    {"type": "text", "text": "first"},
                    {"type": "hardBreak"},
                    {"type": "text", "text": "second"}
                ]},
                {"type": "bulletList", "content": [
                    {"type": "listItem", "content": [
                        {"type": "paragraph", "content": [{"type": "text", "text": "item"}]}
                    ]}
                ]}
            ]
        });
        assert_eq!(description_text(&adf), "first\nsecond\n- item");
    }

    #[test]
    fn jira_error_payload_is_extracted() {
        let body = r#"{"errorMessages":["Issue does not exist"],"errors":{"status":"bad"}}"#;
        assert_eq!(extract_error(body), "Issue does not exist; status: bad");
        assert_eq!(extract_error("plain"), "plain");
    }

    fn user(id: &str, cloud: bool) -> JiraUser {
        JiraUser {
            id: id.into(),
            display_name: "Ann".into(),
            email: String::new(),
            cloud,
        }
    }

    #[test]
    fn assignee_body_uses_account_id_on_cloud_and_name_on_server() {
        assert_eq!(
            assignee_body(Some(&user("5b10a", true)), true),
            serde_json::json!({ "accountId": "5b10a" })
        );
        assert_eq!(
            assignee_body(Some(&user("ann", false)), false),
            serde_json::json!({ "name": "ann" })
        );
    }

    #[test]
    fn assignee_body_null_unassigns() {
        assert_eq!(
            assignee_body(None, true),
            serde_json::json!({ "accountId": null })
        );
        assert_eq!(
            assignee_body(None, false),
            serde_json::json!({ "name": null })
        );
    }

    #[test]
    fn assignable_query_names_search_param_per_deployment() {
        assert_eq!(
            assignable_query("PROJ-7", "an", true),
            vec![
                ("issueKey", "PROJ-7"),
                ("maxResults", "50"),
                ("query", "an")
            ]
        );
        assert_eq!(
            assignable_query("PROJ-7", "an", false),
            vec![
                ("issueKey", "PROJ-7"),
                ("maxResults", "50"),
                ("username", "an")
            ]
        );
        assert_eq!(
            assignable_query("PROJ-7", "", true),
            vec![("issueKey", "PROJ-7"), ("maxResults", "50")]
        );
    }

    #[test]
    fn priorities_keep_jira_order() {
        let v = serde_json::json!([
            {"id": "1", "name": "Highest"},
            {"id": "10000", "name": "Blocker"},
            {"id": "3", "name": "Medium"},
            {"id": "x"}
        ]);
        assert_eq!(parse_priorities(&v), vec!["Highest", "Blocker", "Medium"]);
        assert!(parse_priorities(&Value::Null).is_empty());
    }

    #[test]
    fn users_parse_cloud_and_server_and_skip_inactive() {
        let v = serde_json::json!([
            {"accountId": "5b10a", "displayName": "Ann Cloud", "emailAddress": "ann@x.io", "active": true},
            {"name": "bob", "key": "JIRAUSER1", "displayName": "Bob Server", "active": true},
            {"accountId": "gone", "displayName": "Former", "active": false},
            {"displayName": "No id"}
        ]);
        let users = parse_users(&v);
        assert_eq!(
            users,
            vec![
                JiraUser {
                    id: "5b10a".into(),
                    display_name: "Ann Cloud".into(),
                    email: "ann@x.io".into(),
                    cloud: true,
                },
                JiraUser {
                    id: "bob".into(),
                    display_name: "Bob Server".into(),
                    email: String::new(),
                    cloud: false,
                },
            ]
        );
        assert!(parse_users(&Value::Null).is_empty());
    }
}

fn adf_walk(v: &Value, out: &mut String) {
    match v["type"].as_str() {
        Some("text") => {
            out.push_str(v["text"].as_str().unwrap_or(""));
            return;
        }
        Some("hardBreak") => {
            out.push('\n');
            return;
        }
        Some("listItem") => out.push_str("- "),
        _ => {}
    }
    if let Some(children) = v["content"].as_array() {
        for c in children {
            adf_walk(c, out);
        }
    }
    // Block-level nodes end with a blank line.
    if matches!(
        v["type"].as_str(),
        Some("paragraph" | "heading" | "codeBlock" | "blockquote" | "listItem")
    ) && !out.ends_with('\n')
    {
        out.push('\n');
    }
}
