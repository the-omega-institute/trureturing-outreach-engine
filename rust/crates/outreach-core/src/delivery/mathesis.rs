//! Mailbox policy shared with a future Graph adapter. Only the author's own
//! text counts, and correspondence in another conversation counts as well.
use super::*;
fn own_text(text: &str) -> &str {
    let quoted=crate::regex(r"(?m)^(?:From: |-----Original Message|_{20,}|On .{1,240} wrote:|Le .{1,240} a écrit|În .{1,240} a scris:|Am .{1,240} schrieb|El .{1,240} escribió)").expect("constant regex");
    quoted.find(text).map_or(text, |m| &text[..m.start()])
}
pub fn introduced(messages: &Value, account: &str) -> Value {
    let mut found = vec![];
    for message in array(messages) {
        if truth(&message["isDraft"]) || address(&message["from"]) != account {
            continue;
        }
        let mut content = s(&message["body"]["content"]).to_owned();
        if casefold(s(&message["body"]["contentType"])) == "html" {
            content = crate::regex("<[^>]+>")
                .expect("constant regex")
                .replace_all(&content, " ")
                .into_owned();
        }
        if casefold(own_text(&content)).contains("mathesis-omega-research") {
            found.push(object([
                (
                    "sent_at",
                    message["sentDateTime"]
                        .as_str()
                        .map(string)
                        .unwrap_or_else(|| string("")),
                ),
                (
                    "message_id",
                    message["internetMessageId"]
                        .as_str()
                        .map(string)
                        .unwrap_or_else(|| string("")),
                ),
            ]));
        }
    }
    found.sort_by(|a, b| s(&a["sent_at"]).cmp(s(&b["sent_at"])));
    found.into_iter().next().unwrap_or(Value::Null)
}
pub fn require_once(
    box_: &mut impl Mailbox,
    p: &Value,
    rows: &Value,
    account: &str,
    team: &Value,
) -> StageResult<()> {
    let team: BTreeSet<_> = array(team).iter().map(|a| casefold(s(a))).collect();
    let mut prior = introduced(rows, account);
    let recipients: BTreeSet<_> = array(&p["to"])
        .iter()
        .map(|a| casefold(s(a)))
        .filter(|a| !team.contains(a) && a != account)
        .collect();
    for recipient in recipients {
        if truth(&prior) {
            break;
        }
        let history = box_.observe(MailboxRead::RecipientHistory(recipient))?;
        for row in array(&history) {
            if row["folder"] != "sentitems"
                || truth(&row["isDraft"])
                || row["conversationId"] == p["thread_id"]
                || !truth(&row["id"])
            {
                continue;
            }
            prior = introduced(
                &Value::Array(vec![
                    box_.observe(MailboxRead::Message(s(&row["id"]).into()))?,
                ]),
                account,
            );
            if truth(&prior) {
                break;
            }
        }
    }
    check(
        !truth(&prior),
        &format!(
            "Mathesis was already introduced to this correspondent in {} at {}; revise the letter without the Mathesis paragraph",
            s(&prior["message_id"]),
            s(&prior["sent_at"])
        ),
    )
}
