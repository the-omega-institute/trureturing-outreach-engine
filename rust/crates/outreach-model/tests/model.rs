use outreach_canon::{canonical, dumps};
use outreach_model::*;
#[test]
fn unknown_data_is_lossless_and_states_fail_closed() {
    let raw = r#"{"z":true,"result":{"future":10000000000000000000000000000001,"outcome":"hold"},"a":[1.0,{"q":"π"}],"z":false}"#;
    let data = TaskData::parse(raw).unwrap();
    let parsed = outreach_canon::parse(raw).unwrap();
    assert_eq!(data.canonical().unwrap(), canonical(&parsed).unwrap());
    assert_eq!(
        dumps(data.value(), Default::default()).unwrap(),
        dumps(&parsed, Default::default()).unwrap()
    );
    assert_eq!(
        TaskData::parse(&data.canonical().unwrap())
            .unwrap()
            .canonical()
            .unwrap(),
        data.canonical().unwrap()
    );
    assert_eq!(
        TaskResult::parse(r#"{"outcome":"hold","future":1}"#)
            .unwrap()
            .outcome,
        Outcome::Hold
    );
    assert!(TaskResult::parse(r#"{"outcome":"future"}"#).is_err());
    let review = Review::parse(r#"{"verdict":"PASS","binding":{"task_id":"synthetic","task_revision":true,"result_hash":"r","workflow_hash":"w"},"future":10000000000000000000000000000001}"#).unwrap();
    assert_eq!(review.verdict, Verdict::Pass);
    assert_eq!(
        PipelineKey::from("recheck:mail:synthetic"),
        PipelineKey::Recheck {
            lane: "mail".into(),
            task_id: TaskId("synthetic".into())
        }
    );
    assert!("future-state".parse::<TaskState>().is_err());
    assert!("future-kind".parse::<TaskKind>().is_err());
    assert_eq!(TaskState::ALL.len(), 26);
    assert_eq!(TaskKind::ALL.len(), 7);
    for state in TaskState::ALL {
        assert_eq!(state.as_str().parse::<TaskState>().unwrap(), *state);
    }
    let binding = |revision: &str| {
        Binding::parse(&format!(r#"{{"task_id":"synthetic","task_revision":{revision},"result_hash":"r","workflow_hash":"w"}}"#)).unwrap()
    };
    assert!(binding("true").python_eq(&binding("1.0")));
    assert!(!binding("9007199254740993").python_eq(&binding("9007199254740992.0")));
}
